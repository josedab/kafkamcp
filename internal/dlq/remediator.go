package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// RemediationResult is the outcome of a DLQ remediation operation.
type RemediationResult struct {
	Topic          string            `json:"topic"`
	Action         string            `json:"action"`
	ProcessedCount int               `json:"processed_count"`
	SuccessCount   int               `json:"success_count"`
	FailureCount   int               `json:"failure_count"`
	ErrorTypes     map[string]int    `json:"error_types,omitempty"`
	Details        []RemediationItem `json:"details,omitempty"`
	Suggestions    []string          `json:"suggestions,omitempty"`
}

// RemediationItem describes the outcome for a single message.
type RemediationItem struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Status    string `json:"status"` // "retried", "routed", "skipped", "failed"
	Error     string `json:"error,omitempty"`
}

// Policy defines a remediation policy for a DLQ topic pattern.
type Policy struct {
	TopicPattern    string `json:"topic_pattern"`
	Action          string `json:"action"`           // retry, route, skip
	MaxRetries      int    `json:"max_retries"`
	BackoffMs       int64  `json:"backoff_ms"`
	TargetTopic     string `json:"target_topic"`
	EscalateAfter   int    `json:"escalate_after"`   // number of failures before escalation
}

// Remediator handles DLQ auto-remediation with configurable policies.
type Remediator struct {
	mu         sync.RWMutex
	policies   []Policy
	stats      RemediationStats
	logger     *slog.Logger
	httpClient *http.Client
}

// RemediationStats tracks remediation outcomes for observability.
type RemediationStats struct {
	TotalProcessed int64            `json:"total_processed"`
	TotalSuccess   int64            `json:"total_success"`
	TotalFailed    int64            `json:"total_failed"`
	ByTopic        map[string]int64 `json:"by_topic"`
}

// NewRemediator creates a new DLQ remediator.
func NewRemediator(logger *slog.Logger) *Remediator {
	return &Remediator{
		logger: logger,
		httpClient: &http.Client{Timeout: 10 * time.Second},
		stats: RemediationStats{
			ByTopic: make(map[string]int64),
		},
	}
}

// AddPolicy adds a remediation policy for DLQ topics matching the given pattern.
func (r *Remediator) AddPolicy(p Policy) {
	if p.MaxRetries <= 0 {
		p.MaxRetries = 3
	}
	if p.BackoffMs <= 0 {
		p.BackoffMs = 1000
	}
	r.mu.Lock()
	r.policies = append(r.policies, p)
	r.mu.Unlock()
}

// GetStats returns current remediation statistics.
func (r *Remediator) GetStats() RemediationStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stats
}

// Remediate performs a remediation action on DLQ messages.
func (r *Remediator) Remediate(ctx context.Context, mgr kafka.KafkaManager, cluster, topic, action, targetTopic string, limit int) (*RemediationResult, error) {
	if limit <= 0 {
		limit = 100
	}

	switch action {
	case "analyze":
		return r.analyzeForRemediation(ctx, mgr, cluster, topic, limit)
	case "retry":
		return r.retryMessages(ctx, mgr, cluster, topic, limit)
	case "route":
		if targetTopic == "" {
			return nil, fmt.Errorf("target_topic is required for route action")
		}
		return r.routeMessages(ctx, mgr, cluster, topic, targetTopic, limit)
	case "suggest":
		return r.suggestFixes(ctx, mgr, cluster, topic, limit)
	default:
		return nil, fmt.Errorf("unknown remediation action %q; must be: analyze, retry, route, suggest", action)
	}
}

func (r *Remediator) analyzeForRemediation(ctx context.Context, mgr kafka.KafkaManager, cluster, topic string, limit int) (*RemediationResult, error) {
	analysis, err := mgr.AnalyzeDLQ(ctx, cluster, topic, limit)
	if err != nil {
		return nil, fmt.Errorf("analyzing DLQ: %w", err)
	}

	result := &RemediationResult{
		Topic:          topic,
		Action:         "analyze",
		ProcessedCount: analysis.TotalMessages,
		ErrorTypes:     analysis.ErrorTypes,
	}

	// Generate remediation suggestions based on error types
	for errType, count := range analysis.ErrorTypes {
		switch {
		case strings.Contains(errType, "serialization") || strings.Contains(errType, "deserialization"):
			result.Suggestions = append(result.Suggestions,
				fmt.Sprintf("%d messages with %s errors — consider schema evolution or routing to correction topic", count, errType))
		case strings.Contains(errType, "timeout") || strings.Contains(errType, "connection"):
			result.Suggestions = append(result.Suggestions,
				fmt.Sprintf("%d messages with %s errors — safe to retry with backoff", count, errType))
		case strings.Contains(errType, "validation"):
			result.Suggestions = append(result.Suggestions,
				fmt.Sprintf("%d messages with %s errors — route to correction topic for data fixes", count, errType))
		default:
			result.Suggestions = append(result.Suggestions,
				fmt.Sprintf("%d messages with %s errors — review samples for remediation strategy", count, errType))
		}
	}

	return result, nil
}

func (r *Remediator) retryMessages(ctx context.Context, mgr kafka.KafkaManager, cluster, topic string, limit int) (*RemediationResult, error) {
	// Determine the original topic by stripping DLQ suffix
	originalTopic := deriveOriginalTopic(topic)

	msgs, err := mgr.Consume(ctx, cluster, topic, -1, "earliest", limit, 10000, "auto")
	if err != nil {
		return nil, fmt.Errorf("consuming DLQ messages: %w", err)
	}

	result := &RemediationResult{
		Topic:          topic,
		Action:         "retry",
		ProcessedCount: len(msgs),
	}

	for _, msg := range msgs {
		produceMsg := kafka.ProduceMessage{
			Value:   msg.Value,
			Headers: msg.Headers,
		}
		if msg.Key != nil {
			produceMsg.Key = fmt.Sprintf("%v", msg.Key)
		}

		// Add retry metadata header
		if produceMsg.Headers == nil {
			produceMsg.Headers = make(map[string]string)
		}
		produceMsg.Headers["x-dlq-retry-at"] = time.Now().UTC().Format(time.RFC3339)
		produceMsg.Headers["x-dlq-source"] = topic

		_, err := mgr.Produce(ctx, cluster, originalTopic, []kafka.ProduceMessage{produceMsg}, kafka.ProduceOptions{})
		item := RemediationItem{
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}
		if err != nil {
			item.Status = "failed"
			item.Error = categorizeDLQError(err)
			result.FailureCount++
		} else {
			item.Status = "retried"
			result.SuccessCount++
		}
		result.Details = append(result.Details, item)
	}

	r.updateStats(topic, result)
	return result, nil
}

func (r *Remediator) routeMessages(ctx context.Context, mgr kafka.KafkaManager, cluster, topic, targetTopic string, limit int) (*RemediationResult, error) {
	msgs, err := mgr.Consume(ctx, cluster, topic, -1, "earliest", limit, 10000, "auto")
	if err != nil {
		return nil, fmt.Errorf("consuming DLQ messages: %w", err)
	}

	result := &RemediationResult{
		Topic:          topic,
		Action:         "route",
		ProcessedCount: len(msgs),
	}

	for _, msg := range msgs {
		produceMsg := kafka.ProduceMessage{
			Value:   msg.Value,
			Headers: msg.Headers,
		}
		if msg.Key != nil {
			produceMsg.Key = fmt.Sprintf("%v", msg.Key)
		}
		if produceMsg.Headers == nil {
			produceMsg.Headers = make(map[string]string)
		}
		produceMsg.Headers["x-dlq-routed-from"] = topic
		produceMsg.Headers["x-dlq-routed-at"] = time.Now().UTC().Format(time.RFC3339)

		_, err := mgr.Produce(ctx, cluster, targetTopic, []kafka.ProduceMessage{produceMsg}, kafka.ProduceOptions{})
		item := RemediationItem{
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}
		if err != nil {
			item.Status = "failed"
			item.Error = categorizeDLQError(err)
			result.FailureCount++
		} else {
			item.Status = "routed"
			result.SuccessCount++
		}
		result.Details = append(result.Details, item)
	}

	r.updateStats(topic, result)
	return result, nil
}

func (r *Remediator) suggestFixes(ctx context.Context, mgr kafka.KafkaManager, cluster, topic string, limit int) (*RemediationResult, error) {
	analysis, err := mgr.AnalyzeDLQ(ctx, cluster, topic, limit)
	if err != nil {
		return nil, fmt.Errorf("analyzing DLQ for suggestions: %w", err)
	}

	result := &RemediationResult{
		Topic:          topic,
		Action:         "suggest",
		ProcessedCount: analysis.TotalMessages,
		ErrorTypes:     analysis.ErrorTypes,
	}

	// Analyze error patterns and suggest fixes
	for errType := range analysis.ErrorTypes {
		switch {
		case strings.Contains(errType, "schema") || strings.Contains(errType, "serialization"):
			result.Suggestions = append(result.Suggestions,
				"Schema mismatch detected. Check schema compatibility with `kafka_check_schema_compatibility`.",
				"Consider registering a new compatible schema version.",
				"Use `kafka_schema_diff` to identify breaking changes.")
		case strings.Contains(errType, "timeout"):
			result.Suggestions = append(result.Suggestions,
				"Timeout errors suggest downstream service issues.",
				"Safe to retry with exponential backoff.",
				"Consider increasing consumer timeout configuration.")
		case strings.Contains(errType, "validation"):
			result.Suggestions = append(result.Suggestions,
				"Validation errors indicate malformed data.",
				"Route to a correction topic for data cleansing.",
				"Review producer-side validation rules.")
		case strings.Contains(errType, "auth") || strings.Contains(errType, "permission"):
			result.Suggestions = append(result.Suggestions,
				"Authorization errors — verify ACLs and credentials.",
				"Do NOT retry until permissions are fixed.")
		default:
			result.Suggestions = append(result.Suggestions,
				fmt.Sprintf("Unknown error type %q — review sample messages for patterns.", errType),
				"Consider routing to a manual review topic.")
		}
	}

	originalTopic := deriveOriginalTopic(topic)
	result.Suggestions = append(result.Suggestions,
		fmt.Sprintf("Original topic appears to be %q", originalTopic))

	return result, nil
}

func (r *Remediator) updateStats(topic string, result *RemediationResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stats.TotalProcessed += int64(result.ProcessedCount)
	r.stats.TotalSuccess += int64(result.SuccessCount)
	r.stats.TotalFailed += int64(result.FailureCount)
	r.stats.ByTopic[topic] += int64(result.ProcessedCount)
}

// deriveOriginalTopic attempts to determine the original topic from a DLQ topic name.
func deriveOriginalTopic(dlqTopic string) string {
	suffixes := []string{"-dlq", ".dlq", "_dlq", ".dead-letter", "-dead-letter", "_dead_letter"}
	for _, suffix := range suffixes {
		if strings.HasSuffix(strings.ToLower(dlqTopic), suffix) {
			return dlqTopic[:len(dlqTopic)-len(suffix)]
		}
	}
	prefixes := []string{"dlq-", "dlq.", "dlq_", "dead-letter-", "dead-letter."}
	for _, prefix := range prefixes {
		if strings.HasPrefix(strings.ToLower(dlqTopic), prefix) {
			return dlqTopic[len(prefix):]
		}
	}
	return dlqTopic + "-retry"
}

func categorizeDLQError(err error) string {
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline"):
		return "timeout_error"
	case strings.Contains(msg, "connection") || strings.Contains(msg, "connect"):
		return "connection_error"
	case strings.Contains(msg, "produce") || strings.Contains(msg, "publish"):
		return "produce_failed"
	case strings.Contains(msg, "auth") || strings.Contains(msg, "permission"):
		return "authentication_error"
	case strings.Contains(msg, "serialization") || strings.Contains(msg, "marshal"):
		return "serialization_error"
	default:
		return "produce_failed"
	}
}
