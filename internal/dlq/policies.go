package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// RemediationPolicy defines configurable per-DLQ-topic remediation policies.
type RemediationPolicy struct {
	TopicPattern    string          `json:"topic_pattern" yaml:"topic_pattern"`
	Action          string          `json:"action" yaml:"action"`                     // retry, route, skip, escalate
	MaxRetries      int             `json:"max_retries" yaml:"max_retries"`
	BackoffBaseMs   int64           `json:"backoff_base_ms" yaml:"backoff_base_ms"`
	BackoffMaxMs    int64           `json:"backoff_max_ms" yaml:"backoff_max_ms"`
	TargetTopic     string          `json:"target_topic,omitempty" yaml:"target_topic,omitempty"`
	EscalateAfter   int             `json:"escalate_after" yaml:"escalate_after"`
	EscalationWebhook string        `json:"escalation_webhook,omitempty" yaml:"escalation_webhook,omitempty"`
	ErrorPatterns   []ErrorPattern  `json:"error_patterns,omitempty" yaml:"error_patterns,omitempty"`
	Priority        int             `json:"priority" yaml:"priority"`
}

// ErrorPattern maps an error pattern to a specific remediation action.
type ErrorPattern struct {
	Pattern    string `json:"pattern" yaml:"pattern"`
	Action     string `json:"action" yaml:"action"`       // retry, route, skip, escalate
	MaxRetries int    `json:"max_retries,omitempty" yaml:"max_retries,omitempty"`
}

// EscalationEvent represents an escalation triggered by threshold breach.
type EscalationEvent struct {
	Topic         string    `json:"topic"`
	ErrorType     string    `json:"error_type"`
	FailureCount  int       `json:"failure_count"`
	Threshold     int       `json:"threshold"`
	Timestamp     time.Time `json:"timestamp"`
	Acknowledged  bool      `json:"acknowledged"`
}

// DryRunResult shows what would happen without actually remediating.
type DryRunResult struct {
	Topic           string            `json:"topic"`
	TotalMessages   int               `json:"total_messages"`
	WouldRetry      int               `json:"would_retry"`
	WouldRoute      int               `json:"would_route"`
	WouldSkip       int               `json:"would_skip"`
	WouldEscalate   int               `json:"would_escalate"`
	MatchedPolicies []string          `json:"matched_policies"`
	ErrorBreakdown  map[string]int    `json:"error_breakdown"`
	EstimatedSuccess float64          `json:"estimated_success_rate"`
}

// SchemaFixSuggestion provides schema-aware fix suggestions.
type SchemaFixSuggestion struct {
	Field       string `json:"field"`
	Issue       string `json:"issue"`
	Suggestion  string `json:"suggestion"`
	AutoFixable bool   `json:"auto_fixable"`
	FixAction   string `json:"fix_action,omitempty"`
}

// PolicyManager manages remediation policies.
type PolicyManager struct {
	mu       sync.RWMutex
	policies []RemediationPolicy
	logger   *slog.Logger
}

// NewPolicyManager creates a new DLQ policy manager.
func NewPolicyManager(logger *slog.Logger) *PolicyManager {
	return &PolicyManager{
		logger: logger,
	}
}

// AddPolicy adds or updates a remediation policy.
func (pm *PolicyManager) AddPolicy(policy RemediationPolicy) {
	if policy.MaxRetries <= 0 {
		policy.MaxRetries = 3
	}
	if policy.BackoffBaseMs <= 0 {
		policy.BackoffBaseMs = 1000
	}
	if policy.BackoffMaxMs <= 0 {
		policy.BackoffMaxMs = 30000
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Replace existing policy with same pattern
	for i, p := range pm.policies {
		if p.TopicPattern == policy.TopicPattern {
			pm.policies[i] = policy
			return
		}
	}
	pm.policies = append(pm.policies, policy)
}

// RemovePolicy removes a policy by topic pattern.
func (pm *PolicyManager) RemovePolicy(topicPattern string) bool {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for i, p := range pm.policies {
		if p.TopicPattern == topicPattern {
			pm.policies = append(pm.policies[:i], pm.policies[i+1:]...)
			return true
		}
	}
	return false
}

// FindPolicy returns the best matching policy for a DLQ topic.
func (pm *PolicyManager) FindPolicy(topic string) *RemediationPolicy {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var best *RemediationPolicy
	for i, p := range pm.policies {
		if matchTopicPattern(p.TopicPattern, topic) {
			if best == nil || p.Priority > best.Priority {
				best = &pm.policies[i]
			}
		}
	}
	return best
}

// ListPolicies returns all configured policies.
func (pm *PolicyManager) ListPolicies() []RemediationPolicy {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	result := make([]RemediationPolicy, len(pm.policies))
	copy(result, pm.policies)
	return result
}

// DryRun simulates remediation without applying changes.
func (r *Remediator) DryRun(ctx context.Context, mgr kafka.KafkaManager, cluster, topic string, limit int) (*DryRunResult, error) {
	if limit <= 0 {
		limit = 100
	}

	analysis, err := mgr.AnalyzeDLQ(ctx, cluster, topic, limit)
	if err != nil {
		return nil, fmt.Errorf("analyzing DLQ for dry run: %w", err)
	}

	result := &DryRunResult{
		Topic:          topic,
		TotalMessages:  analysis.TotalMessages,
		ErrorBreakdown: analysis.ErrorTypes,
	}

	// Find matching policy
	r.mu.RLock()
	var matchedPolicy *Policy
	for i, p := range r.policies {
		if matchTopicPattern(p.TopicPattern, topic) {
			matchedPolicy = &r.policies[i]
			result.MatchedPolicies = append(result.MatchedPolicies, p.TopicPattern)
			break
		}
	}
	r.mu.RUnlock()

	// Simulate actions based on error types
	for errType, count := range analysis.ErrorTypes {
		action := "retry" // default
		if matchedPolicy != nil {
			action = matchedPolicy.Action
		}

		// Override by error type
		switch {
		case strings.Contains(errType, "timeout") || strings.Contains(errType, "connection"):
			action = "retry"
		case strings.Contains(errType, "schema") || strings.Contains(errType, "serialization"):
			action = "route"
		case strings.Contains(errType, "auth") || strings.Contains(errType, "permission"):
			action = "skip"
		case strings.Contains(errType, "validation"):
			action = "route"
		}

		switch action {
		case "retry":
			result.WouldRetry += count
		case "route":
			result.WouldRoute += count
		case "skip":
			result.WouldSkip += count
		case "escalate":
			result.WouldEscalate += count
		}
	}

	// Estimate success rate
	if result.TotalMessages > 0 {
		retryableCount := result.WouldRetry
		result.EstimatedSuccess = float64(retryableCount) / float64(result.TotalMessages) * 0.7 // 70% retry success assumed
		if result.WouldRoute > 0 {
			result.EstimatedSuccess += float64(result.WouldRoute) / float64(result.TotalMessages) * 1.0
		}
		if result.EstimatedSuccess > 1.0 {
			result.EstimatedSuccess = 1.0
		}
	}

	return result, nil
}

// SuggestSchemaFixes analyzes DLQ messages and suggests schema-aware fixes.
func (r *Remediator) SuggestSchemaFixes(ctx context.Context, mgr kafka.KafkaManager, cluster, topic string, limit int) ([]SchemaFixSuggestion, error) {
	if limit <= 0 {
		limit = 20
	}

	msgs, err := mgr.Consume(ctx, cluster, topic, -1, "earliest", limit, 10000, "auto")
	if err != nil {
		return nil, fmt.Errorf("consuming DLQ messages for schema analysis: %w", err)
	}

	var suggestions []SchemaFixSuggestion
	fieldIssues := make(map[string]int)

	for _, msg := range msgs {
		// Try to parse message value as JSON
		var value map[string]any
		valueBytes, err := json.Marshal(msg.Value)
		if err != nil {
			continue
		}
		if err := json.Unmarshal(valueBytes, &value); err != nil {
			suggestions = append(suggestions, SchemaFixSuggestion{
				Field:       "_value",
				Issue:       "Message value is not valid JSON",
				Suggestion:  "Check producer serialization settings. Ensure schema type matches.",
				AutoFixable: false,
			})
			continue
		}

		// Check for common schema issues
		for field, v := range value {
			if v == nil {
				fieldIssues[field+":null"]++
			}
			if s, ok := v.(string); ok && s == "" {
				fieldIssues[field+":empty"]++
			}
		}

		// Check error headers for schema info
		if errHeader, ok := msg.Headers["x-error-type"]; ok {
			if strings.Contains(errHeader, "schema") {
				suggestions = append(suggestions, SchemaFixSuggestion{
					Field:       "_schema",
					Issue:       fmt.Sprintf("Schema error: %s", errHeader),
					Suggestion:  "Register a compatible schema version or update consumer deserializer.",
					AutoFixable: false,
				})
			}
		}
	}

	for issue, count := range fieldIssues {
		parts := strings.SplitN(issue, ":", 2)
		if len(parts) != 2 {
			continue
		}
		field, issueType := parts[0], parts[1]
		switch issueType {
		case "null":
			suggestions = append(suggestions, SchemaFixSuggestion{
				Field:       field,
				Issue:       fmt.Sprintf("Field %q is null in %d messages", field, count),
				Suggestion:  "Add default value in schema or make field nullable (union with null in Avro).",
				AutoFixable: true,
				FixAction:   "add_default",
			})
		case "empty":
			suggestions = append(suggestions, SchemaFixSuggestion{
				Field:       field,
				Issue:       fmt.Sprintf("Field %q is empty string in %d messages", field, count),
				Suggestion:  "Add validation at producer or set a meaningful default.",
				AutoFixable: false,
			})
		}
	}

	return suggestions, nil
}

// Escalate sends an escalation webhook notification.
func (r *Remediator) Escalate(ctx context.Context, topic, errorType string, failureCount, threshold int, webhookURL string) error {
	if webhookURL == "" {
		r.logger.Warn("escalation triggered but no webhook configured", "topic", topic, "error_type", errorType)
		return nil
	}

	event := EscalationEvent{
		Topic:        topic,
		ErrorType:    errorType,
		FailureCount: failureCount,
		Threshold:    threshold,
		Timestamp:    time.Now(),
	}

	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling escalation event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhookURL, strings.NewReader(string(body)))
	if err != nil {
		return fmt.Errorf("creating escalation request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "KafkaMCP-DLQ-Remediator/1.0")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("sending escalation webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("escalation webhook returned status %d", resp.StatusCode)
	}

	r.logger.Info("escalation webhook sent", "topic", topic, "error_type", errorType, "failures", failureCount)
	return nil
}

// CalculateBackoff computes exponential backoff duration.
func CalculateBackoff(attempt int, baseMs, maxMs int64) time.Duration {
	backoff := float64(baseMs) * math.Pow(2, float64(attempt))
	if int64(backoff) > maxMs {
		backoff = float64(maxMs)
	}
	return time.Duration(backoff) * time.Millisecond
}

func matchTopicPattern(pattern, topic string) bool {
	// Simple glob matching
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(topic, prefix)
	}
	if strings.HasPrefix(pattern, "*") {
		suffix := strings.TrimPrefix(pattern, "*")
		return strings.HasSuffix(topic, suffix)
	}
	return pattern == topic
}
