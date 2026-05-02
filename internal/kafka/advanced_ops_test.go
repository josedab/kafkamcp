package kafka

import (
	"testing"
)

func TestGenerateSampleDataEcommerce(t *testing.T) {
	t.Parallel()
	msgs := generateSampleData("ecommerce", 5)
	if len(msgs) != 5 {
		t.Errorf("expected 5 messages, got %d", len(msgs))
	}
	for _, msg := range msgs {
		if msg.Key == "" {
			t.Error("expected non-empty key for ecommerce message")
		}
		if msg.Value == nil {
			t.Error("expected non-nil value for ecommerce message")
		}
	}
}

func TestGenerateSampleDataIOT(t *testing.T) {
	t.Parallel()
	msgs := generateSampleData("iot", 3)
	if len(msgs) != 3 {
		t.Errorf("expected 3 messages, got %d", len(msgs))
	}
	for _, msg := range msgs {
		if msg.Key == "" {
			t.Error("expected non-empty key for iot message")
		}
	}
}

func TestGenerateSampleDataUserActivity(t *testing.T) {
	t.Parallel()
	msgs := generateSampleData("user_activity", 4)
	if len(msgs) != 4 {
		t.Errorf("expected 4 messages, got %d", len(msgs))
	}
}

func TestGenerateSampleDataLogs(t *testing.T) {
	t.Parallel()
	msgs := generateSampleData("logs", 2)
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages, got %d", len(msgs))
	}
}

func TestGenerateSampleDataDefault(t *testing.T) {
	t.Parallel()
	msgs := generateSampleData("unknown_pattern", 3)
	if len(msgs) != 3 {
		t.Errorf("expected 3 messages (default=ecommerce), got %d", len(msgs))
	}
}

func TestGenerateSampleDataZero(t *testing.T) {
	t.Parallel()
	msgs := generateSampleData("iot", 0)
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages, got %d", len(msgs))
	}
}

func TestPlaygroundPrefix(t *testing.T) {
	t.Parallel()
	if playgroundPrefix != "_playground_" {
		t.Errorf("expected '_playground_', got %q", playgroundPrefix)
	}
}

func TestAnalyzeACLSecurity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		acls             []ACLEntry
		wantWildcards    int
		wantOverPerm     int
		wantTotalFindings int
	}{
		{
			name:          "empty",
			acls:          nil,
			wantWildcards: 0,
			wantOverPerm:  0,
		},
		{
			name: "wildcard resource",
			acls: []ACLEntry{
				{Principal: "User:admin", ResourceName: "*", ResourceType: "Topic", Operation: "All"},
			},
			wantWildcards:    1,
			wantTotalFindings: 1,
		},
		{
			name: "over-permissioned principal",
			acls: func() []ACLEntry {
				acls := make([]ACLEntry, 12)
				for i := range acls {
					acls[i] = ACLEntry{Principal: "User:app", ResourceName: "topic-" + string(rune('a'+i)), ResourceType: "Topic", Operation: "Read"}
				}
				return acls
			}(),
			wantOverPerm:     1,
			wantTotalFindings: 1,
		},
		{
			name: "no issues",
			acls: []ACLEntry{
				{Principal: "User:app1", ResourceName: "orders", ResourceType: "Topic", Operation: "Read"},
				{Principal: "User:app2", ResourceName: "payments", ResourceType: "Topic", Operation: "Write"},
			},
			wantWildcards:    0,
			wantOverPerm:     0,
			wantTotalFindings: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := analyzeACLSecurity(tt.acls)
			if len(result.WildcardACLs) != tt.wantWildcards {
				t.Errorf("wildcard ACLs: got %d, want %d", len(result.WildcardACLs), tt.wantWildcards)
			}
			if len(result.OverPermissioned) != tt.wantOverPerm {
				t.Errorf("over-permissioned: got %d, want %d", len(result.OverPermissioned), tt.wantOverPerm)
			}
			if tt.wantTotalFindings > 0 && result.Findings != tt.wantTotalFindings {
				t.Errorf("total findings: got %d, want %d", result.Findings, tt.wantTotalFindings)
			}
		})
	}
}

func TestTopicMatchScoreExact(t *testing.T) {
	t.Parallel()
	score := topicMatchScore("show me orders", "orders")
	if score < 100 {
		t.Errorf("expected score >= 100 for exact match, got %d", score)
	}
}

func TestTopicMatchScorePartial(t *testing.T) {
	t.Parallel()
	score := topicMatchScore("show me recent payments", "payments.created")
	if score <= 0 {
		t.Errorf("expected positive score for partial match, got %d", score)
	}
}

func TestTopicMatchScoreNoMatch(t *testing.T) {
	t.Parallel()
	score := topicMatchScore("hello world", "orders.created")
	if score != 0 {
		t.Errorf("expected 0 score for no match, got %d", score)
	}
}

func TestTopicMatchScoreAssociations(t *testing.T) {
	t.Parallel()
	score := topicMatchScore("show me purchase history", "order-events")
	if score <= 0 {
		t.Errorf("expected positive score for keyword association, got %d", score)
	}
}

func TestExtractJoinKeyFromValue(t *testing.T) {
	t.Parallel()
	msg := Message{
		Value: map[string]any{
			"user": map[string]any{
				"id": "user-123",
			},
		},
	}
	key := extractJoinKey(msg, "user.id")
	if key != "user-123" {
		t.Errorf("expected 'user-123', got %q", key)
	}
}

func TestExtractJoinKeyFromMessageKey(t *testing.T) {
	t.Parallel()
	msg := Message{Key: "msg-key-123"}
	key := extractJoinKey(msg, "")
	if key != "msg-key-123" {
		t.Errorf("expected 'msg-key-123', got %q", key)
	}
}

func TestExtractJoinKeyMissing(t *testing.T) {
	t.Parallel()
	msg := Message{Value: map[string]any{"a": "b"}}
	key := extractJoinKey(msg, "nonexistent.path")
	if key != "" {
		t.Errorf("expected empty string, got %q", key)
	}
}

func TestExtractJoinKeyNonMapValue(t *testing.T) {
	t.Parallel()
	msg := Message{Value: "string-value"}
	key := extractJoinKey(msg, "some.field")
	if key != "" {
		t.Errorf("expected empty string for non-map value, got %q", key)
	}
}

func TestExtractNumericFieldEmpty(t *testing.T) {
	t.Parallel()
	val, ok := extractNumericField(42.5, "")
	if !ok || val != 42.5 {
		t.Errorf("expected 42.5, got %v (ok=%v)", val, ok)
	}
}

func TestExtractNumericFieldEmptyString(t *testing.T) {
	t.Parallel()
	val, ok := extractNumericField("3.14", "")
	if !ok || val != 3.14 {
		t.Errorf("expected 3.14, got %v (ok=%v)", val, ok)
	}
}

func TestExtractNumericFieldEmptyNonNumeric(t *testing.T) {
	t.Parallel()
	_, ok := extractNumericField("not-a-number", "")
	if ok {
		t.Error("expected false for non-numeric string")
	}
}

func TestSuggestSchemaEvolutionAddField(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "add a new field for email")
	if len(suggestions) < 2 {
		t.Errorf("expected at least 2 suggestions for add field, got %d", len(suggestions))
	}
	found := false
	for _, s := range suggestions {
		if s.Strategy == "add_optional_field" {
			found = true
		}
	}
	if !found {
		t.Error("expected 'add_optional_field' strategy")
	}
}

func TestSuggestSchemaEvolutionRemove(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "remove the old status field")
	found := false
	for _, s := range suggestions {
		if s.Strategy == "deprecate_then_remove" {
			found = true
		}
	}
	if !found {
		t.Error("expected 'deprecate_then_remove' strategy")
	}
}

func TestSuggestSchemaEvolutionRename(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "rename user_id to customer_id")
	if len(suggestions) < 2 {
		t.Errorf("expected at least 2 suggestions for rename, got %d", len(suggestions))
	}
}

func TestSuggestSchemaEvolutionTypeChange(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "change the type of amount from int to long")
	found := false
	for _, s := range suggestions {
		if s.Strategy == "widen_type" {
			found = true
		}
	}
	if !found {
		t.Error("expected 'widen_type' strategy")
	}
}

func TestSuggestSchemaEvolutionGeneric(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "do something unrecognized")
	if len(suggestions) == 0 {
		t.Error("expected at least 1 generic suggestion")
	}
	if suggestions[0].Strategy != "compatibility_check_first" {
		t.Errorf("expected 'compatibility_check_first', got %q", suggestions[0].Strategy)
	}
}

func TestDangerousConfigKeys(t *testing.T) {
	t.Parallel()
	if len(dangerousConfigKeys) < 3 {
		t.Errorf("expected at least 3 dangerous config keys, got %d", len(dangerousConfigKeys))
	}
}

func TestCorrelationHeaders(t *testing.T) {
	t.Parallel()
	if len(correlationHeaders) < 10 {
		t.Errorf("expected at least 10 correlation header variants, got %d", len(correlationHeaders))
	}
}
