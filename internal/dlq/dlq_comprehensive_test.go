package dlq

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

// --- deriveOriginalTopic additional edge cases ---

func TestDeriveOriginalTopicDeadLetterPrefix(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"dead-letter-orders", "orders"},
		{"dead-letter.payments", "payments"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := deriveOriginalTopic(tt.input)
			if got != tt.want {
				t.Errorf("deriveOriginalTopic(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDeriveOriginalTopicDeadLetterSuffix(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"orders-dead-letter", "orders"},
		{"payments_dead_letter", "payments"},
		{"events.dead-letter", "events"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := deriveOriginalTopic(tt.input)
			if got != tt.want {
				t.Errorf("deriveOriginalTopic(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDeriveOriginalTopicEmptyString(t *testing.T) {
	got := deriveOriginalTopic("")
	if got != "-retry" {
		t.Errorf("deriveOriginalTopic('') = %q, want '-retry'", got)
	}
}

func TestDeriveOriginalTopicOnlySuffix(t *testing.T) {
	got := deriveOriginalTopic("-dlq")
	if got != "" {
		t.Errorf("deriveOriginalTopic('-dlq') = %q, want ''", got)
	}
}

func TestDeriveOriginalTopicMixedCase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"Orders-DLQ", "Orders"},
		{"PAYMENTS.DLQ", "PAYMENTS"},
		{"Events_DLQ", "Events"},
		{"DLQ-orders", "orders"},
		{"DLQ.payments", "payments"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := deriveOriginalTopic(tt.input)
			if got != tt.want {
				t.Errorf("deriveOriginalTopic(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDeriveOriginalTopicWithDotsInName(t *testing.T) {
	got := deriveOriginalTopic("my.service.orders-dlq")
	if got != "my.service.orders" {
		t.Errorf("deriveOriginalTopic('my.service.orders-dlq') = %q, want 'my.service.orders'", got)
	}
}

func TestDeriveOriginalTopicWithHyphensInName(t *testing.T) {
	got := deriveOriginalTopic("my-service-orders.dlq")
	if got != "my-service-orders" {
		t.Errorf("deriveOriginalTopic('my-service-orders.dlq') = %q, want 'my-service-orders'", got)
	}
}

// --- Remediate dispatch tests ---

func TestRemediateUnknownActions(t *testing.T) {
	r := NewRemediator(slog.Default())
	actions := []string{"", "delete", "purge", "RETRY", "Analyze"}

	for _, action := range actions {
		t.Run("action_"+action, func(t *testing.T) {
			_, err := r.Remediate(context.Background(), nil, "", "topic-dlq", action, "", 10)
			if err == nil {
				t.Errorf("expected error for action %q", action)
			}
			if !strings.Contains(err.Error(), "unknown remediation action") {
				t.Errorf("expected 'unknown remediation action' in error, got %q", err.Error())
			}
		})
	}
}

func TestRemediateRouteWithoutTarget(t *testing.T) {
	r := NewRemediator(slog.Default())
	_, err := r.Remediate(context.Background(), nil, "cluster", "orders-dlq", "route", "", 50)
	if err == nil {
		t.Fatal("expected error for route without target")
	}
	if !strings.Contains(err.Error(), "target_topic is required") {
		t.Errorf("expected 'target_topic is required' in error, got %q", err.Error())
	}
}

func TestRemediateDefaultLimitIsApplied(t *testing.T) {
	r := NewRemediator(slog.Default())

	// With limit=0, should default to 100 internally but still fail on unknown action
	_, err := r.Remediate(context.Background(), nil, "", "topic", "unknown", "", 0)
	if err == nil {
		t.Error("expected error")
	}

	// With negative limit
	_, err = r.Remediate(context.Background(), nil, "", "topic", "unknown", "", -5)
	if err == nil {
		t.Error("expected error")
	}
}

// --- AddPolicy tests ---

func TestAddPolicyZeroMaxRetries(t *testing.T) {
	r := NewRemediator(slog.Default())
	r.AddPolicy(Policy{
		TopicPattern: "*-dlq",
		Action:       "retry",
		MaxRetries:   0,
		BackoffMs:    500,
	})

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.policies[0].MaxRetries != 3 {
		t.Errorf("expected MaxRetries default to 3 for zero value, got %d", r.policies[0].MaxRetries)
	}
	if r.policies[0].BackoffMs != 500 {
		t.Errorf("expected BackoffMs=500, got %d", r.policies[0].BackoffMs)
	}
}

func TestAddPolicyNegativeMaxRetries(t *testing.T) {
	r := NewRemediator(slog.Default())
	r.AddPolicy(Policy{
		TopicPattern: "*-dlq",
		Action:       "retry",
		MaxRetries:   -1,
		BackoffMs:    -500,
	})

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.policies[0].MaxRetries != 3 {
		t.Errorf("expected MaxRetries default to 3 for negative value, got %d", r.policies[0].MaxRetries)
	}
	if r.policies[0].BackoffMs != 1000 {
		t.Errorf("expected BackoffMs default to 1000 for negative value, got %d", r.policies[0].BackoffMs)
	}
}

func TestAddPolicyWithEscalation(t *testing.T) {
	r := NewRemediator(slog.Default())
	r.AddPolicy(Policy{
		TopicPattern:  "*-dlq",
		Action:        "retry",
		MaxRetries:    5,
		BackoffMs:     2000,
		EscalateAfter: 10,
	})

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.policies[0].EscalateAfter != 10 {
		t.Errorf("expected EscalateAfter=10, got %d", r.policies[0].EscalateAfter)
	}
}

func TestAddPolicyWithTargetTopic(t *testing.T) {
	r := NewRemediator(slog.Default())
	r.AddPolicy(Policy{
		TopicPattern: "*.dlq",
		Action:       "route",
		TargetTopic:  "correction-queue",
		MaxRetries:   1,
		BackoffMs:    100,
	})

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.policies[0].TargetTopic != "correction-queue" {
		t.Errorf("expected TargetTopic=correction-queue, got %s", r.policies[0].TargetTopic)
	}
	if r.policies[0].MaxRetries != 1 {
		t.Errorf("expected MaxRetries=1, got %d", r.policies[0].MaxRetries)
	}
}

func TestAddManyPolicies(t *testing.T) {
	r := NewRemediator(slog.Default())
	for i := 0; i < 10; i++ {
		r.AddPolicy(Policy{TopicPattern: "*-dlq", Action: "retry"})
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.policies) != 10 {
		t.Errorf("expected 10 policies, got %d", len(r.policies))
	}
}

// --- updateStats tests ---

func TestUpdateStatsMultipleTopics(t *testing.T) {
	r := NewRemediator(slog.Default())
	r.updateStats("orders-dlq", &RemediationResult{ProcessedCount: 5, SuccessCount: 4, FailureCount: 1})
	r.updateStats("payments-dlq", &RemediationResult{ProcessedCount: 3, SuccessCount: 3, FailureCount: 0})
	r.updateStats("orders-dlq", &RemediationResult{ProcessedCount: 2, SuccessCount: 1, FailureCount: 1})

	stats := r.GetStats()
	if stats.TotalProcessed != 10 {
		t.Errorf("expected TotalProcessed=10, got %d", stats.TotalProcessed)
	}
	if stats.TotalSuccess != 8 {
		t.Errorf("expected TotalSuccess=8, got %d", stats.TotalSuccess)
	}
	if stats.TotalFailed != 2 {
		t.Errorf("expected TotalFailed=2, got %d", stats.TotalFailed)
	}
	if stats.ByTopic["orders-dlq"] != 7 {
		t.Errorf("expected ByTopic[orders-dlq]=7, got %d", stats.ByTopic["orders-dlq"])
	}
	if stats.ByTopic["payments-dlq"] != 3 {
		t.Errorf("expected ByTopic[payments-dlq]=3, got %d", stats.ByTopic["payments-dlq"])
	}
}

func TestUpdateStatsZeroCounts(t *testing.T) {
	r := NewRemediator(slog.Default())
	r.updateStats("empty-dlq", &RemediationResult{ProcessedCount: 0, SuccessCount: 0, FailureCount: 0})

	stats := r.GetStats()
	if stats.TotalProcessed != 0 {
		t.Errorf("expected TotalProcessed=0, got %d", stats.TotalProcessed)
	}
	if stats.ByTopic["empty-dlq"] != 0 {
		t.Errorf("expected ByTopic[empty-dlq]=0, got %d", stats.ByTopic["empty-dlq"])
	}
}

// --- GetStats tests ---

func TestGetStatsAfterMultipleUpdates(t *testing.T) {
	r := NewRemediator(slog.Default())
	topics := []string{"topic-a-dlq", "topic-b-dlq", "topic-c-dlq"}
	for i, topic := range topics {
		r.updateStats(topic, &RemediationResult{
			ProcessedCount: (i + 1) * 10,
			SuccessCount:   (i + 1) * 8,
			FailureCount:   (i + 1) * 2,
		})
	}

	stats := r.GetStats()
	// 10+20+30 = 60
	if stats.TotalProcessed != 60 {
		t.Errorf("expected TotalProcessed=60, got %d", stats.TotalProcessed)
	}
	// 8+16+24 = 48
	if stats.TotalSuccess != 48 {
		t.Errorf("expected TotalSuccess=48, got %d", stats.TotalSuccess)
	}
	// 2+4+6 = 12
	if stats.TotalFailed != 12 {
		t.Errorf("expected TotalFailed=12, got %d", stats.TotalFailed)
	}
	if len(stats.ByTopic) != 3 {
		t.Errorf("expected 3 topics in ByTopic, got %d", len(stats.ByTopic))
	}
}

func TestGetStatsReturnsSnapshot(t *testing.T) {
	r := NewRemediator(slog.Default())
	r.updateStats("test-dlq", &RemediationResult{ProcessedCount: 5})

	stats1 := r.GetStats()
	r.updateStats("test-dlq", &RemediationResult{ProcessedCount: 10})
	stats2 := r.GetStats()

	// stats1 should not be affected by the second update
	if stats1.TotalProcessed != 5 {
		t.Errorf("expected stats1.TotalProcessed=5, got %d", stats1.TotalProcessed)
	}
	if stats2.TotalProcessed != 15 {
		t.Errorf("expected stats2.TotalProcessed=15, got %d", stats2.TotalProcessed)
	}
}

// --- Policy struct tests ---

func TestPolicyStructDefaults(t *testing.T) {
	p := Policy{}
	if p.TopicPattern != "" {
		t.Error("expected empty TopicPattern")
	}
	if p.Action != "" {
		t.Error("expected empty Action")
	}
	if p.MaxRetries != 0 {
		t.Error("expected zero MaxRetries")
	}
	if p.BackoffMs != 0 {
		t.Error("expected zero BackoffMs")
	}
	if p.TargetTopic != "" {
		t.Error("expected empty TargetTopic")
	}
	if p.EscalateAfter != 0 {
		t.Error("expected zero EscalateAfter")
	}
}

func TestPolicyStructFields(t *testing.T) {
	p := Policy{
		TopicPattern:  "orders-*",
		Action:        "route",
		MaxRetries:    5,
		BackoffMs:     2000,
		TargetTopic:   "correction",
		EscalateAfter: 3,
	}
	if p.TopicPattern != "orders-*" {
		t.Errorf("expected TopicPattern=orders-*, got %s", p.TopicPattern)
	}
	if p.Action != "route" {
		t.Errorf("expected Action=route, got %s", p.Action)
	}
	if p.MaxRetries != 5 {
		t.Errorf("expected MaxRetries=5, got %d", p.MaxRetries)
	}
	if p.BackoffMs != 2000 {
		t.Errorf("expected BackoffMs=2000, got %d", p.BackoffMs)
	}
	if p.TargetTopic != "correction" {
		t.Errorf("expected TargetTopic=correction, got %s", p.TargetTopic)
	}
	if p.EscalateAfter != 3 {
		t.Errorf("expected EscalateAfter=3, got %d", p.EscalateAfter)
	}
}

// --- RemediationResult struct tests ---

func TestRemediationResultStruct(t *testing.T) {
	r := RemediationResult{
		Topic:          "orders-dlq",
		Action:         "retry",
		ProcessedCount: 10,
		SuccessCount:   8,
		FailureCount:   2,
		ErrorTypes:     map[string]int{"timeout": 2},
		Suggestions:    []string{"retry with backoff"},
	}

	if r.Topic != "orders-dlq" {
		t.Errorf("expected Topic=orders-dlq, got %s", r.Topic)
	}
	if r.ProcessedCount != 10 {
		t.Errorf("expected ProcessedCount=10, got %d", r.ProcessedCount)
	}
	if r.SuccessCount != 8 {
		t.Errorf("expected SuccessCount=8, got %d", r.SuccessCount)
	}
	if r.FailureCount != 2 {
		t.Errorf("expected FailureCount=2, got %d", r.FailureCount)
	}
	if r.ErrorTypes["timeout"] != 2 {
		t.Errorf("expected ErrorTypes[timeout]=2, got %d", r.ErrorTypes["timeout"])
	}
	if len(r.Suggestions) != 1 {
		t.Errorf("expected 1 suggestion, got %d", len(r.Suggestions))
	}
}

func TestRemediationResultWithDetails(t *testing.T) {
	r := RemediationResult{
		Topic:  "orders-dlq",
		Action: "retry",
		Details: []RemediationItem{
			{Partition: 0, Offset: 100, Status: "retried"},
			{Partition: 1, Offset: 200, Status: "failed", Error: "timeout"},
		},
	}

	if len(r.Details) != 2 {
		t.Fatalf("expected 2 details, got %d", len(r.Details))
	}
	if r.Details[0].Status != "retried" {
		t.Errorf("expected first item status=retried, got %s", r.Details[0].Status)
	}
	if r.Details[1].Error != "timeout" {
		t.Errorf("expected second item error=timeout, got %s", r.Details[1].Error)
	}
}

// --- RemediationItem struct tests ---

func TestRemediationItemStruct(t *testing.T) {
	item := RemediationItem{
		Partition: 3,
		Offset:    42,
		Status:    "routed",
		Error:     "",
	}

	if item.Partition != 3 {
		t.Errorf("expected Partition=3, got %d", item.Partition)
	}
	if item.Offset != 42 {
		t.Errorf("expected Offset=42, got %d", item.Offset)
	}
	if item.Status != "routed" {
		t.Errorf("expected Status=routed, got %s", item.Status)
	}
	if item.Error != "" {
		t.Errorf("expected empty Error, got %s", item.Error)
	}
}

func TestRemediationItemWithError(t *testing.T) {
	item := RemediationItem{
		Partition: 0,
		Offset:    0,
		Status:    "failed",
		Error:     "connection refused",
	}

	if item.Status != "failed" {
		t.Errorf("expected Status=failed, got %s", item.Status)
	}
	if item.Error != "connection refused" {
		t.Errorf("expected Error='connection refused', got %s", item.Error)
	}
}

// --- RemediationStats struct tests ---

func TestRemediationStatsStruct(t *testing.T) {
	s := RemediationStats{
		TotalProcessed: 100,
		TotalSuccess:   90,
		TotalFailed:    10,
		ByTopic:        map[string]int64{"orders-dlq": 60, "payments-dlq": 40},
	}

	if s.TotalProcessed != 100 {
		t.Errorf("expected TotalProcessed=100, got %d", s.TotalProcessed)
	}
	if len(s.ByTopic) != 2 {
		t.Errorf("expected 2 topics, got %d", len(s.ByTopic))
	}
}

// --- NewRemediator with nil logger ---

func TestNewRemediatorNilLogger(t *testing.T) {
	r := NewRemediator(nil)
	if r == nil {
		t.Fatal("expected non-nil remediator with nil logger")
	}
	if r.stats.ByTopic == nil {
		t.Error("expected initialized ByTopic map")
	}
}

// --- Remediate context handling ---

func TestRemediateWithCancelledContext(t *testing.T) {
	r := NewRemediator(slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Unknown action still returns error regardless of cancelled context
	_, err := r.Remediate(ctx, nil, "", "topic-dlq", "invalid", "", 10)
	if err == nil {
		t.Error("expected error for invalid action")
	}
}
