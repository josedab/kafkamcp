package kafka

import (
	"context"
	"testing"
)

// ===================== extractNumericField tests =====================

func TestExtractNumericField_DirectValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    any
		wantVal  float64
		wantBool bool
	}{
		{"float64", 42.5, 42.5, true},
		{"string number", "99.9", 99.9, true},
		{"non-numeric string", "hello", 0, false},
		{"nil", nil, 0, false},
		{"map (not numeric)", map[string]any{"a": 1}, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := extractNumericField(tt.value, "")
			if ok != tt.wantBool {
				t.Errorf("ok = %v, want %v", ok, tt.wantBool)
			}
			if ok && val != tt.wantVal {
				t.Errorf("val = %v, want %v", val, tt.wantVal)
			}
		})
	}
}

func TestExtractNumericField_NestedFields(t *testing.T) {
	t.Parallel()
	value := map[string]any{
		"user": map[string]any{
			"age":    float64(30),
			"name":   "alice",
			"nested": map[string]any{"score": float64(95.5)},
		},
		"amount": float64(100),
	}

	tests := []struct {
		field    string
		wantVal  float64
		wantBool bool
	}{
		{"amount", 100, true},
		{"user.age", 30, true},
		{"user.nested.score", 95.5, true},
		{"user.name", 0, false}, // string, not numeric
		{"nonexistent", 0, false},
		{"user.nonexistent", 0, false},
		{"deeply.nested.missing", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			val, ok := extractNumericField(value, tt.field)
			if ok != tt.wantBool {
				t.Errorf("ok = %v, want %v for field %q", ok, tt.wantBool, tt.field)
			}
			if ok && val != tt.wantVal {
				t.Errorf("val = %v, want %v", val, tt.wantVal)
			}
		})
	}
}

func TestExtractNumericField_IntegerTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		value    any
		wantVal  float64
		wantBool bool
	}{
		{"int", map[string]any{"v": int(42)}, 42, true},
		{"int64", map[string]any{"v": int64(999)}, 999, true},
		{"string int", map[string]any{"v": "55"}, 55, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, ok := extractNumericField(tt.value, "v")
			if ok != tt.wantBool || val != tt.wantVal {
				t.Errorf("got (%v, %v), want (%v, %v)", val, ok, tt.wantVal, tt.wantBool)
			}
		})
	}
}

func TestExtractNumericField_NonMapInput(t *testing.T) {
	t.Parallel()
	val, ok := extractNumericField("not a map", "field")
	if ok {
		t.Errorf("expected false for non-map value with field, got val=%v", val)
	}
}

// ===================== SuggestSchemaEvolution tests =====================

func TestSuggestSchemaEvolution_AddField_Extended(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "add a new field for user email")
	if len(suggestions) == 0 {
		t.Fatal("expected suggestions for adding field")
	}
	hasAddOptional := false
	for _, s := range suggestions {
		if s.Strategy == "add_optional_field" {
			hasAddOptional = true
		}
	}
	if !hasAddOptional {
		t.Error("expected add_optional_field strategy")
	}
}

func TestSuggestSchemaEvolution_RemoveField_Extended(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "remove the legacy status field")
	if len(suggestions) == 0 {
		t.Fatal("expected suggestions for removing field")
	}
	hasDeprecate := false
	for _, s := range suggestions {
		if s.Strategy == "deprecate_then_remove" {
			hasDeprecate = true
		}
	}
	if !hasDeprecate {
		t.Error("expected deprecate_then_remove strategy")
	}
}

func TestSuggestSchemaEvolution_RenameField_Extended(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "rename the userId field to user_id")
	if len(suggestions) == 0 {
		t.Fatal("expected suggestions for renaming")
	}
}

func TestSuggestSchemaEvolution_TypeChange_Extended(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "change the amount type from int to long")
	if len(suggestions) == 0 {
		t.Fatal("expected suggestions for type change")
	}
}

func TestSuggestSchemaEvolution_GenericFallback(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "something completely different")
	if len(suggestions) == 0 {
		t.Fatal("expected at least a generic suggestion")
	}
	if suggestions[0].Strategy != "compatibility_check_first" {
		t.Errorf("expected compatibility_check_first for unknown change, got %s", suggestions[0].Strategy)
	}
}

// ===================== Aggregate & GetTopicGraph via mock =====================

func TestAggregate_MockManager_Count(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
			return &AggregateResult{
				Topic:        req.Topic,
				Field:        req.Field,
				Function:     req.Function,
				Value:        42,
				MessageCount: 100,
			}, nil
		},
	}

	result, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic:    "orders",
		Field:    "amount",
		Function: "count",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Value != 42 {
		t.Errorf("expected value 42, got %f", result.Value)
	}
	if result.Function != "count" {
		t.Errorf("expected function count, got %s", result.Function)
	}
}

func TestAggregate_MockManager_Avg(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
			return &AggregateResult{Value: 55.5, Function: "avg", MessageCount: 10}, nil
		},
	}

	result, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic:    "metrics",
		Field:    "temperature",
		Function: "avg",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Value != 55.5 {
		t.Errorf("expected avg 55.5, got %f", result.Value)
	}
}

func TestGetTopicGraph_MockManager_Success(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*TopicGraph, error) {
			return &TopicGraph{
				Nodes: []TopicGraphNode{
					{ID: "topic:orders", Type: "topic", Name: "orders"},
					{ID: "group:processor", Type: "consumer_group", Name: "processor"},
				},
				Edges: []TopicGraphEdge{
					{Source: "topic:orders", Target: "group:processor", Type: "consumes"},
				},
			}, nil
		},
	}

	graph, err := mock.GetTopicGraph(context.Background(), "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(graph.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(graph.Nodes))
	}
	if len(graph.Edges) != 1 {
		t.Errorf("expected 1 edge, got %d", len(graph.Edges))
	}
}

func TestGetTopicGraph_MockManager_EmptyCluster(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*TopicGraph, error) {
			return &TopicGraph{}, nil
		},
	}

	graph, err := mock.GetTopicGraph(context.Background(), "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(graph.Nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(graph.Nodes))
	}
}
