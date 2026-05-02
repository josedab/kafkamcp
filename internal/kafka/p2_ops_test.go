package kafka

import (
	"testing"
)

func TestExtractNumericField(t *testing.T) {
	t.Parallel()
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
		{"int value", map[string]any{"count": 5}, "count", 5.0, true},
		{"int64 value", map[string]any{"big": int64(9999999)}, "big", 9999999.0, true},
		{"string number in nested", map[string]any{"val": "3.14"}, "val", 3.14, true},
		{"nil value", nil, "", 0, false},
		{"nil value with field", nil, "x", 0, false},
		{"field on non-map", "hello", "x", 0, false},
		{"three level nesting", map[string]any{"a": map[string]any{"b": map[string]any{"c": 7.0}}}, "a.b.c", 7.0, true},
		{"zero float", 0.0, "", 0.0, true},
		{"negative float", -5.5, "", -5.5, true},
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
	t.Parallel()
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

	// Numeric key converted to string
	numMsg := Message{Key: 42.0, Value: map[string]any{}}
	if got := extractJoinKey(numMsg, ""); got != "42" {
		t.Errorf("expected '42', got %q", got)
	}

	// Nil key returns "<nil>" (fmt.Sprintf behavior)
	nilMsg := Message{Key: nil, Value: map[string]any{"id": "abc"}}
	if got := extractJoinKey(nilMsg, ""); got != "<nil>" {
		t.Errorf("expected '<nil>' for nil key, got %q", got)
	}

	// Top-level value field
	simpleMsg := Message{Key: "k", Value: map[string]any{"user_id": "u1"}}
	if got := extractJoinKey(simpleMsg, "user_id"); got != "u1" {
		t.Errorf("expected 'u1', got %q", got)
	}
}

func TestTopicMatchScore(t *testing.T) {
	t.Parallel()
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

	// Zero score for unrelated
	if s := topicMatchScore("hello world", "x.y.z"); s != 0 {
		t.Errorf("expected 0 for unrelated, got %d", s)
	}

	// Keyword associations boost score
	payScore := topicMatchScore("show me payment transactions", "payment.events")
	if payScore <= 0 {
		t.Errorf("expected positive score for keyword match, got %d", payScore)
	}

	// Short segments (<3 chars) should not match
	shortSeg := topicMatchScore("ab", "ab.cd")
	exactShort := topicMatchScore("ab.cd", "ab.cd")
	if shortSeg >= exactShort {
		t.Errorf("short segment (%d) should score less than exact (%d)", shortSeg, exactShort)
	}
}

func TestSuggestSchemaEvolution(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	c, err := NewConnectClient("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c != nil {
		t.Error("expected nil for empty URL")
	}

	c, err = NewConnectClient("http://localhost:8083")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c == nil {
		t.Error("expected non-nil client")
	}
	if c.baseURL != "http://localhost:8083" {
		t.Errorf("unexpected baseURL: %s", c.baseURL)
	}
}
