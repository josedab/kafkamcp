package watch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/match"
	"github.com/kafkamcp/kafkamcp/internal/urlutil"
)

// Subscription represents a registered watch on topic patterns.
type Subscription struct {
	ID           string            `json:"id"`
	AgentID      string            `json:"agent_id"`
	TopicPattern string            `json:"topic_pattern"`
	KeyFilter    string            `json:"key_filter,omitempty"`
	ValueFilter  string            `json:"value_filter,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	WebhookURL   string            `json:"webhook_url,omitempty"`
	MaxRetries   int               `json:"max_retries"`
	CreatedAt    time.Time         `json:"created_at"`
	Active       bool              `json:"active"`
	Filters      []FilterExpression `json:"filters,omitempty"`
	Paused       bool              `json:"paused"`
	ExpiresAt    *time.Time        `json:"expires_at,omitempty"`
	DeliveryCount int64            `json:"delivery_count"`
	LastMatchAt  *time.Time        `json:"last_match_at,omitempty"`
}

// Notification is a message pushed to an agent when a subscription matches.
type Notification struct {
	SubscriptionID string         `json:"subscription_id"`
	AgentID        string         `json:"agent_id"`
	Topic          string         `json:"topic"`
	Partition      int32          `json:"partition"`
	Offset         int64          `json:"offset"`
	Timestamp      string         `json:"timestamp"`
	Key            any            `json:"key"`
	Value          any            `json:"value"`
	Headers        map[string]string `json:"headers,omitempty"`
	DeliveredAt    string         `json:"delivered_at"`
}

// DeliveryResult tracks the outcome of a notification delivery attempt.
type DeliveryResult struct {
	SubscriptionID string `json:"subscription_id"`
	Status         string `json:"status"` // "delivered", "failed", "retrying"
	Attempts       int    `json:"attempts"`
	LastError      string `json:"last_error,omitempty"`
}

// MessageEvent is an internal event from the consumer pool to the matching engine.
type MessageEvent struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Key       any
	Value     any
	Headers   map[string]string
}

// Manager coordinates subscriptions, background consumers, and notification delivery.
type Manager struct {
	mu            sync.RWMutex
	subscriptions map[string]*Subscription
	counter       int
	logger        *slog.Logger
	httpClient    *http.Client
	maxRetries    int

	// Notification channel for MCP sampling integration
	notifyCh chan Notification
}

// NewManager creates a new watch manager.
func NewManager(logger *slog.Logger) *Manager {
	return &Manager{
		subscriptions: make(map[string]*Subscription),
		logger:        logger,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13},
			},
		},
		maxRetries: 5,
		notifyCh:   make(chan Notification, 1000),
	}
}

// Subscribe registers a new watch subscription and returns the subscription ID.
func (m *Manager) Subscribe(agentID string, topicPattern, keyFilter, valueFilter, webhookURL string, headers map[string]string) (*Subscription, error) {
	if topicPattern == "" {
		return nil, fmt.Errorf("topic_pattern is required")
	}

	if webhookURL != "" {
		if err := urlutil.ValidateWebhookURL(webhookURL); err != nil {
			return nil, err
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.counter++
	id := fmt.Sprintf("watch-%s-%d", agentID, m.counter)

	maxRetries := m.maxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}

	sub := &Subscription{
		ID:           id,
		AgentID:      agentID,
		TopicPattern: topicPattern,
		KeyFilter:    keyFilter,
		ValueFilter:  valueFilter,
		Headers:      headers,
		WebhookURL:   webhookURL,
		MaxRetries:   maxRetries,
		CreatedAt:    time.Now(),
		Active:       true,
	}

	m.subscriptions[id] = sub
	m.logger.Info("watch subscription created",
		"id", id, "agent", agentID, "pattern", topicPattern, "webhook", webhookURL != "")
	return sub, nil
}

// Unsubscribe removes a watch subscription.
func (m *Manager) Unsubscribe(subscriptionID, agentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sub, ok := m.subscriptions[subscriptionID]
	if !ok {
		return fmt.Errorf("subscription %q not found", subscriptionID)
	}
	if sub.AgentID != agentID {
		return fmt.Errorf("subscription %q belongs to a different agent", subscriptionID)
	}

	sub.Active = false
	delete(m.subscriptions, subscriptionID)
	m.logger.Info("watch subscription removed", "id", subscriptionID, "agent", agentID)
	return nil
}

// ListSubscriptions returns all active subscriptions for an agent.
func (m *Manager) ListSubscriptions(agentID string) []*Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Subscription
	for _, sub := range m.subscriptions {
		if sub.Active && (agentID == "" || sub.AgentID == agentID) {
			result = append(result, sub)
		}
	}
	return result
}

// ProcessMessage evaluates a message against all active subscriptions
// and delivers notifications for matches.
func (m *Manager) ProcessMessage(ctx context.Context, event MessageEvent) []DeliveryResult {
	m.mu.RLock()
	subs := make([]*Subscription, 0)
	for _, sub := range m.subscriptions {
		if sub.Active {
			subs = append(subs, sub)
		}
	}
	m.mu.RUnlock()

	var results []DeliveryResult
	for _, sub := range subs {
		if !m.matchesSubscription(sub, event) {
			continue
		}

		notif := Notification{
			SubscriptionID: sub.ID,
			AgentID:        sub.AgentID,
			Topic:          event.Topic,
			Partition:      event.Partition,
			Offset:         event.Offset,
			Timestamp:      event.Timestamp.UTC().Format(time.RFC3339),
			Key:            event.Key,
			Value:          event.Value,
			Headers:        event.Headers,
			DeliveredAt:    time.Now().UTC().Format(time.RFC3339),
		}

		result := m.deliverNotification(ctx, sub, notif)
		results = append(results, result)

		// Update subscription delivery stats
		m.mu.Lock()
		if s, ok := m.subscriptions[sub.ID]; ok {
			s.DeliveryCount++
			now := time.Now()
			s.LastMatchAt = &now
		}
		m.mu.Unlock()
	}
	return results
}

// NotificationChannel returns the channel for MCP sampling integration.
func (m *Manager) NotificationChannel() <-chan Notification {
	return m.notifyCh
}

// matchesSubscription checks if a message event matches a subscription's filters.
func (m *Manager) matchesSubscription(sub *Subscription, event MessageEvent) bool {
	if sub.Paused {
		return false
	}

	if sub.ExpiresAt != nil && time.Now().After(*sub.ExpiresAt) {
		return false
	}

	if !match.Glob(sub.TopicPattern, event.Topic) {
		return false
	}

	if sub.KeyFilter != "" {
		keyStr := fmt.Sprintf("%v", event.Key)
		if !containsString(keyStr, sub.KeyFilter) {
			return false
		}
	}

	if sub.ValueFilter != "" {
		valueStr := fmt.Sprintf("%v", event.Value)
		if !containsString(valueStr, sub.ValueFilter) {
			return false
		}
	}

	if len(sub.Headers) > 0 {
		for k, v := range sub.Headers {
			if ev, ok := event.Headers[k]; !ok || (v != "" && ev != v) {
				return false
			}
		}
	}

	// Apply structured filter expressions
	for _, f := range sub.Filters {
		if !f.Match(event) {
			return false
		}
	}

	return true
}

// deliverNotification sends a notification via webhook or MCP channel with retry.
func (m *Manager) deliverNotification(ctx context.Context, sub *Subscription, notif Notification) DeliveryResult {
	result := DeliveryResult{
		SubscriptionID: sub.ID,
		Status:         "delivered",
	}

	// Push to MCP sampling channel (non-blocking)
	select {
	case m.notifyCh <- notif:
	default:
		m.logger.Warn("notification channel full, dropping", "sub", sub.ID)
	}

	// Webhook delivery with exponential backoff
	if sub.WebhookURL != "" {
		var lastErr error
		for attempt := 0; attempt <= sub.MaxRetries; attempt++ {
			result.Attempts = attempt + 1
			if attempt > 0 {
				backoff := time.Duration(math.Pow(2, float64(attempt-1))) * time.Second
				if backoff > 30*time.Second {
					backoff = 30 * time.Second
				}
				timer := time.NewTimer(backoff)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					result.Status = "failed"
					result.LastError = "context cancelled"
					return result
				}
			}

			err := m.sendWebhook(ctx, sub.WebhookURL, notif)
			if err == nil {
				result.Status = "delivered"
				return result
			}
			lastErr = err
			m.logger.Warn("webhook delivery failed, retrying",
				"sub", sub.ID, "attempt", attempt+1, "error", err)
		}
		result.Status = "failed"
		if lastErr != nil {
			result.LastError = lastErr.Error()
		}
	}

	return result
}

func (m *Manager) sendWebhook(ctx context.Context, url string, notif Notification) error {
	body, err := json.Marshal(notif)
	if err != nil {
		return fmt.Errorf("marshaling notification: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, jsonReader(body))
	if err != nil {
		return fmt.Errorf("creating webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "KafkaMCP-Watch/1.0")

	resp, err := m.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("sending webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("webhook returned status %d", resp.StatusCode)
}

// ActiveSubscriptionCount returns the number of active subscriptions.
func (m *Manager) ActiveSubscriptionCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, sub := range m.subscriptions {
		if sub.Active && !sub.Paused {
			count++
		}
	}
	return count
}

// PauseSubscription temporarily pauses a subscription without removing it.
func (m *Manager) PauseSubscription(subscriptionID, agentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sub, ok := m.subscriptions[subscriptionID]
	if !ok {
		return fmt.Errorf("subscription %q not found", subscriptionID)
	}
	if sub.AgentID != agentID {
		return fmt.Errorf("subscription %q belongs to a different agent", subscriptionID)
	}
	sub.Paused = true
	m.logger.Info("watch subscription paused", "id", subscriptionID, "agent", agentID)
	return nil
}

// ResumeSubscription resumes a paused subscription.
func (m *Manager) ResumeSubscription(subscriptionID, agentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	sub, ok := m.subscriptions[subscriptionID]
	if !ok {
		return fmt.Errorf("subscription %q not found", subscriptionID)
	}
	if sub.AgentID != agentID {
		return fmt.Errorf("subscription %q belongs to a different agent", subscriptionID)
	}
	sub.Paused = false
	m.logger.Info("watch subscription resumed", "id", subscriptionID, "agent", agentID)
	return nil
}

// ExpireStale removes subscriptions that have passed their expiration time.
func (m *Manager) ExpireStale() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	expired := 0
	for id, sub := range m.subscriptions {
		if sub.ExpiresAt != nil && now.After(*sub.ExpiresAt) {
			sub.Active = false
			delete(m.subscriptions, id)
			expired++
			m.logger.Info("watch subscription expired", "id", id, "agent", sub.AgentID)
		}
	}
	return expired
}

// GetSubscription returns a subscription by ID.
func (m *Manager) GetSubscription(subscriptionID string) (*Subscription, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	sub, ok := m.subscriptions[subscriptionID]
	if !ok {
		return nil, fmt.Errorf("subscription %q not found", subscriptionID)
	}
	return sub, nil
}

// Close shuts down the manager.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, sub := range m.subscriptions {
		sub.Active = false
		delete(m.subscriptions, id)
	}
	close(m.notifyCh)
}
