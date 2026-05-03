package webhook

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Subscription represents a registered webhook endpoint.
type Subscription struct {
	ID        string            `json:"id"`
	URL       string            `json:"url"`
	Topics    []string          `json:"topics"`
	Events    []string          `json:"events"`
	Secret    string            `json:"-"`
	Active    bool              `json:"active"`
	CreatedAt time.Time         `json:"created_at"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// WebhookEvent represents an event to be delivered to matching subscriptions.
type WebhookEvent struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Topic     string    `json:"topic"`
	Cluster   string    `json:"cluster"`
	Payload   any       `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

// DeliveryAttempt records the outcome of a single webhook delivery.
type DeliveryAttempt struct {
	SubscriptionID string    `json:"subscription_id"`
	EventID        string    `json:"event_id"`
	URL            string    `json:"url"`
	StatusCode     int       `json:"status_code"`
	Success        bool      `json:"success"`
	Error          string    `json:"error,omitempty"`
	AttemptedAt    time.Time `json:"attempted_at"`
	LatencyMs      int64     `json:"latency_ms"`
}

const (
	maxRetries          = 3
	maxHistoryPerSub    = 100
	deliveryTimeout     = 10 * time.Second
	initialBackoff      = 1 * time.Second
	signatureHeader     = "X-Webhook-Signature"
	eventTypeHeader     = "X-Webhook-Event"
)

// Manager manages webhook subscriptions and event delivery.
type Manager struct {
	mu            sync.RWMutex
	subscriptions map[string]*Subscription
	history       map[string][]DeliveryAttempt // keyed by subscription ID
	client        *http.Client
	logger        *slog.Logger
}

// NewManager creates a new webhook Manager.
func NewManager(logger *slog.Logger) *Manager {
	return &Manager{
		subscriptions: make(map[string]*Subscription),
		history:       make(map[string][]DeliveryAttempt),
		client: &http.Client{
			Timeout: deliveryTimeout,
		},
		logger: logger,
	}
}

// Subscribe registers a new webhook subscription.
func (m *Manager) Subscribe(sub Subscription) (*Subscription, error) {
	if sub.ID == "" {
		return nil, fmt.Errorf("subscription ID is required")
	}
	if sub.URL == "" {
		return nil, fmt.Errorf("subscription URL is required")
	}
	if !strings.HasPrefix(sub.URL, "http://") && !strings.HasPrefix(sub.URL, "https://") {
		return nil, fmt.Errorf("subscription URL must use http or https scheme")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.subscriptions[sub.ID]; exists {
		return nil, fmt.Errorf("subscription %q already exists", sub.ID)
	}

	sub.Active = true
	sub.CreatedAt = time.Now()
	m.subscriptions[sub.ID] = &sub
	m.logger.Info("webhook subscription registered", "id", sub.ID, "url", sub.URL)
	return &sub, nil
}

// Unsubscribe removes a webhook subscription by ID.
func (m *Manager) Unsubscribe(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.subscriptions[id]; !exists {
		return fmt.Errorf("subscription %q not found", id)
	}

	delete(m.subscriptions, id)
	delete(m.history, id)
	m.logger.Info("webhook subscription removed", "id", id)
	return nil
}

// ListSubscriptions returns all active subscriptions.
func (m *Manager) ListSubscriptions() []*Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()

	subs := make([]*Subscription, 0, len(m.subscriptions))
	for _, sub := range m.subscriptions {
		subs = append(subs, sub)
	}
	return subs
}

// GetSubscription returns a subscription by ID.
func (m *Manager) GetSubscription(id string) (*Subscription, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sub, exists := m.subscriptions[id]
	if !exists {
		return nil, fmt.Errorf("subscription %q not found", id)
	}
	return sub, nil
}

// Deliver dispatches an event to all matching subscriptions asynchronously.
func (m *Manager) Deliver(event WebhookEvent) int {
	m.mu.RLock()
	var matched []*Subscription
	for _, sub := range m.subscriptions {
		if sub.Active && m.matches(sub, event) {
			matched = append(matched, sub)
		}
	}
	m.mu.RUnlock()

	for _, sub := range matched {
		go m.deliverWithRetry(sub, event)
	}

	m.logger.Info("webhook event dispatched", "event_id", event.ID, "type", event.Type, "matched", len(matched))
	return len(matched)
}

// DeliveryHistory returns recent delivery attempts for a subscription.
func (m *Manager) DeliveryHistory(subscriptionID string, limit int) []DeliveryAttempt {
	m.mu.RLock()
	defer m.mu.RUnlock()

	h := m.history[subscriptionID]
	if limit <= 0 || limit > len(h) {
		limit = len(h)
	}
	// Return the most recent entries.
	start := len(h) - limit
	result := make([]DeliveryAttempt, limit)
	copy(result, h[start:])
	return result
}

// matches checks whether a subscription should receive the given event.
func (m *Manager) matches(sub *Subscription, event WebhookEvent) bool {
	if !matchesFilter(sub.Events, event.Type) {
		return false
	}
	if len(sub.Topics) > 0 && !matchesFilter(sub.Topics, event.Topic) {
		return false
	}
	return true
}

// matchesFilter returns true if value matches any pattern in the filter list.
// An empty filter matches everything. Patterns support trailing wildcard (*).
func matchesFilter(patterns []string, value string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		if p == "*" || p == value {
			return true
		}
		if strings.HasSuffix(p, "*") && strings.HasPrefix(value, strings.TrimSuffix(p, "*")) {
			return true
		}
	}
	return false
}

// deliverWithRetry attempts delivery with exponential backoff.
func (m *Manager) deliverWithRetry(sub *Subscription, event WebhookEvent) {
	body, err := json.Marshal(event)
	if err != nil {
		m.recordAttempt(sub.ID, event.ID, sub.URL, 0, false, fmt.Sprintf("marshal error: %v", err), 0)
		return
	}

	backoff := initialBackoff
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff *= 2
		}

		start := time.Now()
		statusCode, deliveryErr := m.doPost(sub, event.Type, body)
		latency := time.Since(start).Milliseconds()

		success := deliveryErr == nil && statusCode >= 200 && statusCode < 300
		errStr := ""
		if deliveryErr != nil {
			errStr = deliveryErr.Error()
		} else if !success {
			errStr = fmt.Sprintf("HTTP %d", statusCode)
		}

		m.recordAttempt(sub.ID, event.ID, sub.URL, statusCode, success, errStr, latency)

		if success {
			return
		}

		m.logger.Warn("webhook delivery failed",
			"subscription_id", sub.ID,
			"event_id", event.ID,
			"attempt", attempt+1,
			"status", statusCode,
			"error", errStr,
		)
	}
}

// doPost sends a single HTTP POST to the subscription URL.
func (m *Manager) doPost(sub *Subscription, eventType string, body []byte) (int, error) {
	req, err := http.NewRequest(http.MethodPost, sub.URL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(eventTypeHeader, eventType)

	if sub.Secret != "" {
		mac := hmac.New(sha256.New, []byte(sub.Secret))
		mac.Write(body)
		sig := hex.EncodeToString(mac.Sum(nil))
		req.Header.Set(signatureHeader, "sha256="+sig)
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

// recordAttempt stores a delivery attempt, keeping the last maxHistoryPerSub entries.
func (m *Manager) recordAttempt(subID, eventID, url string, statusCode int, success bool, errStr string, latencyMs int64) {
	attempt := DeliveryAttempt{
		SubscriptionID: subID,
		EventID:        eventID,
		URL:            url,
		StatusCode:     statusCode,
		Success:        success,
		Error:          errStr,
		AttemptedAt:    time.Now(),
		LatencyMs:      latencyMs,
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	h := m.history[subID]
	h = append(h, attempt)
	if len(h) > maxHistoryPerSub {
		h = h[len(h)-maxHistoryPerSub:]
	}
	m.history[subID] = h
}
