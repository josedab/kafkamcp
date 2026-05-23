package evolution

import (
	"testing"
)

func TestComputeDiffAddedField(t *testing.T) {
	t.Parallel()
	schemaFrom := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "string"},
			map[string]any{"name": "name", "type": "string"},
		},
	}
	schemaTo := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "string"},
			map[string]any{"name": "name", "type": "string"},
			map[string]any{"name": "email", "type": "string", "default": ""},
		},
	}

	diff := ComputeDiff("user-value", 1, 2, schemaFrom, schemaTo)
	if diff.Summary.Added != 1 {
		t.Errorf("expected 1 added, got %d", diff.Summary.Added)
	}
	if diff.Summary.Unchanged != 2 {
		t.Errorf("expected 2 unchanged, got %d", diff.Summary.Unchanged)
	}
}

func TestComputeDiffRemovedField(t *testing.T) {
	t.Parallel()
	schemaFrom := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "string"},
			map[string]any{"name": "deprecated", "type": "string", "default": ""},
		},
	}
	schemaTo := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "string"},
		},
	}

	diff := ComputeDiff("test-value", 1, 2, schemaFrom, schemaTo)
	if diff.Summary.Removed != 1 {
		t.Errorf("expected 1 removed, got %d", diff.Summary.Removed)
	}
}

func TestComputeDiffModifiedField(t *testing.T) {
	t.Parallel()
	schemaFrom := map[string]any{
		"fields": []any{
			map[string]any{"name": "count", "type": "int"},
		},
	}
	schemaTo := map[string]any{
		"fields": []any{
			map[string]any{"name": "count", "type": "long"},
		},
	}

	diff := ComputeDiff("test-value", 1, 2, schemaFrom, schemaTo)
	if diff.Summary.Modified != 1 {
		t.Errorf("expected 1 modified, got %d", diff.Summary.Modified)
	}
	// int -> long is a compatible promotion
	if !diff.Compatible {
		t.Error("expected compatible promotion int -> long")
	}
}

func TestBreakingTypeChange(t *testing.T) {
	t.Parallel()
	tests := []struct {
		old, new string
		breaking bool
	}{
		{"int", "long", false},
		{"int", "string", true},
		{"float", "double", false},
		{"string", "int", true},
		{"string", "bytes", false},
	}
	for _, tt := range tests {
		result := isBreakingTypeChange(tt.old, tt.new)
		if result != tt.breaking {
			t.Errorf("isBreakingTypeChange(%q, %q) = %v, want %v", tt.old, tt.new, result, tt.breaking)
		}
	}
}

func TestAnalyzeConsumerImpact(t *testing.T) {
	t.Parallel()
	diff := &SchemaDiff{
		Subject:     "orders-value",
		VersionFrom: 1,
		VersionTo:   2,
		Compatible:  true,
		Fields: []FieldDiff{
			{Field: "email", Action: "added"},
		},
		Summary: DiffSummary{Added: 1},
	}

	groups := []string{"order-processor", "analytics"}
	topicsByGroup := map[string][]string{
		"order-processor": {"orders"},
		"analytics":       {"analytics-events"},
	}

	impact := AnalyzeConsumerImpact(diff, groups, topicsByGroup)
	if len(impact.Consumers) != 1 {
		t.Fatalf("expected 1 affected consumer, got %d", len(impact.Consumers))
	}
	if impact.Consumers[0].GroupID != "order-processor" {
		t.Errorf("expected order-processor, got %s", impact.Consumers[0].GroupID)
	}
	if !impact.CanAutoRoll {
		t.Error("expected CanAutoRoll=true for compatible change")
	}
}

func TestCreateRolloutPlan(t *testing.T) {
	t.Parallel()
	diff := &SchemaDiff{
		Subject:    "orders-value",
		Compatible: false,
		Summary:    DiffSummary{Modified: 1, Breaking: 1},
	}
	impact := &ImpactAnalysis{
		RiskLevel:   "high",
		CanAutoRoll: false,
	}

	plan := CreateRolloutPlan(diff, impact)
	if len(plan.Stages) < 4 {
		t.Errorf("expected at least 4 stages for breaking change, got %d", len(plan.Stages))
	}
	if plan.CanaryTopic == "" {
		t.Error("expected canary topic for high-risk change")
	}
}

func TestExtractTopicFromSubject(t *testing.T) {
	t.Parallel()
	tests := []struct {
		subject  string
		expected string
	}{
		{"orders-value", "orders"},
		{"orders-key", "orders"},
		{"orders.value", "orders"},
		{"plain-topic", "plain-topic"},
	}
	for _, tt := range tests {
		result := extractTopicFromSubject(tt.subject)
		if result != tt.expected {
			t.Errorf("extractTopicFromSubject(%q) = %q, want %q", tt.subject, result, tt.expected)
		}
	}
}
