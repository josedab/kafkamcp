package dlq

import "testing"

func TestDeriveOriginalTopic(t *testing.T) {
	tests := []struct {
		dlqTopic string
		expected string
	}{
		{"orders-dlq", "orders"},
		{"orders.dlq", "orders"},
		{"orders_dlq", "orders"},
		{"orders.dead-letter", "orders"},
		{"dlq-orders", "orders"},
		{"dlq.payments", "payments"},
		{"random-topic", "random-topic-retry"},
	}
	for _, tt := range tests {
		result := deriveOriginalTopic(tt.dlqTopic)
		if result != tt.expected {
			t.Errorf("deriveOriginalTopic(%q) = %q, want %q", tt.dlqTopic, result, tt.expected)
		}
	}
}

func TestAddPolicy(t *testing.T) {
	r := NewRemediator(nil)
	r.AddPolicy(Policy{
		TopicPattern: "*-dlq",
		Action:       "retry",
	})
	if len(r.policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(r.policies))
	}
	if r.policies[0].MaxRetries != 3 {
		t.Errorf("expected default max_retries=3, got %d", r.policies[0].MaxRetries)
	}
	if r.policies[0].BackoffMs != 1000 {
		t.Errorf("expected default backoff_ms=1000, got %d", r.policies[0].BackoffMs)
	}
}

func TestGetStats(t *testing.T) {
	r := NewRemediator(nil)
	stats := r.GetStats()
	if stats.TotalProcessed != 0 {
		t.Errorf("expected 0 processed, got %d", stats.TotalProcessed)
	}
}
