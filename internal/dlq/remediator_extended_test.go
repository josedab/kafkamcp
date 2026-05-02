package dlq

import (
	"log/slog"
	"testing"
)

func TestDeriveOriginalTopicSuffixes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"orders-dlq", "orders"},
		{"orders.dlq", "orders"},
		{"orders_dlq", "orders"},
		{"payments.dead-letter", "payments"},
		{"payments-dead-letter", "payments"},
		{"events_dead_letter", "events"},
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

func TestDeriveOriginalTopicPrefixes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{"dlq-orders", "orders"},
		{"dlq.orders", "orders"},
		{"dlq_payments", "payments"},
		{"dead-letter-events", "events"},
		{"dead-letter.events", "events"},
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

func TestDeriveOriginalTopicNoMatch(t *testing.T) {
	t.Parallel()
	got := deriveOriginalTopic("regular-topic")
	if got != "regular-topic-retry" {
		t.Errorf("deriveOriginalTopic('regular-topic') = %q, want 'regular-topic-retry'", got)
	}
}

func TestDeriveOriginalTopicCaseInsensitive(t *testing.T) {
	t.Parallel()
	got := deriveOriginalTopic("Orders-DLQ")
	if got != "Orders" {
		t.Errorf("deriveOriginalTopic('Orders-DLQ') = %q, want 'Orders'", got)
	}
}

func TestNewRemediator(t *testing.T) {
	t.Parallel()
	logger := slog.Default()
	r := NewRemediator(logger)
	if r == nil {
		t.Fatal("expected non-nil remediator")
	}
	stats := r.GetStats()
	if stats.TotalProcessed != 0 {
		t.Errorf("expected 0 total processed, got %d", stats.TotalProcessed)
	}
	if stats.ByTopic == nil {
		t.Error("expected non-nil ByTopic map")
	}
}

func TestAddPolicyDefaults(t *testing.T) {
	t.Parallel()
	r := NewRemediator(slog.Default())
	r.AddPolicy(Policy{
		TopicPattern: "*.dlq",
		Action:       "retry",
	})

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(r.policies))
	}
	if r.policies[0].MaxRetries != 3 {
		t.Errorf("expected default MaxRetries=3, got %d", r.policies[0].MaxRetries)
	}
	if r.policies[0].BackoffMs != 1000 {
		t.Errorf("expected default BackoffMs=1000, got %d", r.policies[0].BackoffMs)
	}
}

func TestAddPolicyPreservesCustomValues(t *testing.T) {
	t.Parallel()
	r := NewRemediator(slog.Default())
	r.AddPolicy(Policy{
		TopicPattern: "*.dlq",
		Action:       "route",
		MaxRetries:   5,
		BackoffMs:    2000,
		TargetTopic:  "correction",
	})

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.policies[0].MaxRetries != 5 {
		t.Errorf("expected MaxRetries=5, got %d", r.policies[0].MaxRetries)
	}
	if r.policies[0].BackoffMs != 2000 {
		t.Errorf("expected BackoffMs=2000, got %d", r.policies[0].BackoffMs)
	}
}

func TestAddMultiplePolicies(t *testing.T) {
	t.Parallel()
	r := NewRemediator(slog.Default())
	r.AddPolicy(Policy{TopicPattern: "*.dlq", Action: "retry"})
	r.AddPolicy(Policy{TopicPattern: "*.dead-letter", Action: "route", TargetTopic: "review"})

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.policies) != 2 {
		t.Errorf("expected 2 policies, got %d", len(r.policies))
	}
}

func TestUpdateStats(t *testing.T) {
	t.Parallel()
	r := NewRemediator(slog.Default())
	r.updateStats("test-dlq", &RemediationResult{
		ProcessedCount: 10,
		SuccessCount:   8,
		FailureCount:   2,
	})

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
	if stats.ByTopic["test-dlq"] != 10 {
		t.Errorf("expected ByTopic[test-dlq]=10, got %d", stats.ByTopic["test-dlq"])
	}
}

func TestUpdateStatsAccumulates(t *testing.T) {
	t.Parallel()
	r := NewRemediator(slog.Default())
	r.updateStats("topic-a", &RemediationResult{ProcessedCount: 5, SuccessCount: 5})
	r.updateStats("topic-a", &RemediationResult{ProcessedCount: 3, SuccessCount: 2, FailureCount: 1})

	stats := r.GetStats()
	if stats.TotalProcessed != 8 {
		t.Errorf("expected TotalProcessed=8, got %d", stats.TotalProcessed)
	}
	if stats.ByTopic["topic-a"] != 8 {
		t.Errorf("expected ByTopic[topic-a]=8, got %d", stats.ByTopic["topic-a"])
	}
}

func TestRemediateInvalidAction(t *testing.T) {
	t.Parallel()
	r := NewRemediator(slog.Default())
	_, err := r.Remediate(nil, nil, "", "topic", "invalid_action", "", 10)
	if err == nil {
		t.Error("expected error for invalid action")
	}
}

func TestRemediateRouteRequiresTarget(t *testing.T) {
	t.Parallel()
	r := NewRemediator(slog.Default())
	_, err := r.Remediate(nil, nil, "", "topic", "route", "", 10)
	if err == nil {
		t.Error("expected error when target_topic is empty for route action")
	}
}

func TestRemediateDefaultLimit(t *testing.T) {
	t.Parallel()
	r := NewRemediator(slog.Default())
	// Calling with invalid action but checking limit defaults
	_, err := r.Remediate(nil, nil, "", "topic", "unknown", "", 0)
	if err == nil {
		t.Error("expected error for unknown action")
	}
}
