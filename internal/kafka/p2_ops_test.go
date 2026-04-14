package kafka

import (
	"testing"
)

func TestExtractNumericField(t *testing.T) {
	tests := []struct {
		name  string
		value any
		field string
		want  float64
		ok    bool
	}{
		{"direct float", 42.5, "", 42.5, true},
		{"direct string number", "99.9", "", 99.9, true},
		{"nested field", map[string]any{"amount": 100.0}, "amount", 100.0, true},
		{"deep nested", map[string]any{"metrics": map[string]any{"latency": 42.0}}, "metrics.latency", 42.0, true},
		{"missing field", map[string]any{"a": 1.0}, "b", 0, false},
		{"non-numeric", map[string]any{"name": "test"}, "name", 0, false},
		{"empty field on non-number", "hello", "", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := extractNumericField(tt.value, tt.field)
			if ok != tt.ok {
				t.Errorf("ok = %v, want %v", ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractJoinKey(t *testing.T) {
	msg := Message{
		Key:   "order-123",
		Value: map[string]any{"customer": map[string]any{"id": "cust-456"}},
	}

	// Empty keyField uses message key
	if got := extractJoinKey(msg, ""); got != "order-123" {
		t.Errorf("expected 'order-123', got %q", got)
	}

	// Dot-notation field extraction
	if got := extractJoinKey(msg, "customer.id"); got != "cust-456" {
		t.Errorf("expected 'cust-456', got %q", got)
	}

	// Missing field
	if got := extractJoinKey(msg, "nonexistent"); got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestTopicMatchScore(t *testing.T) {
	// Exact match should score highest
	exact := topicMatchScore("show me orders.created", "orders.created")
	partial := topicMatchScore("show me orders.created", "orders.deleted")
	none := topicMatchScore("show me orders.created", "payments.completed")

	if exact <= partial {
		t.Errorf("exact (%d) should score higher than partial (%d)", exact, partial)
	}
	if partial <= none {
		t.Errorf("partial (%d) should score higher than none (%d)", partial, none)
	}
}

func TestSuggestSchemaEvolution(t *testing.T) {
	tests := []struct {
		change string
		minLen int
	}{
		{"add a new field for email", 1},
		{"remove deprecated field", 1},
		{"rename field from name to full_name", 1},
		{"change type of amount from int to long", 1},
		{"something completely unrelated", 1}, // generic suggestion
	}

	for _, tt := range tests {
		t.Run(tt.change, func(t *testing.T) {
			suggestions := SuggestSchemaEvolution(nil, tt.change)
			if len(suggestions) < tt.minLen {
				t.Errorf("expected at least %d suggestions, got %d", tt.minLen, len(suggestions))
			}
			for _, s := range suggestions {
				if s.Strategy == "" || s.Description == "" || len(s.Steps) == 0 {
					t.Errorf("incomplete suggestion: %+v", s)
				}
			}
		})
	}
}

func TestNewConnectClient(t *testing.T) {
	c := NewConnectClient("")
	if c != nil {
		t.Error("expected nil for empty URL")
	}

	c = NewConnectClient("http://localhost:8083")
	if c == nil {
		t.Error("expected non-nil client")
	}
	if c.baseURL != "http://localhost:8083" {
		t.Errorf("unexpected baseURL: %s", c.baseURL)
	}
}
