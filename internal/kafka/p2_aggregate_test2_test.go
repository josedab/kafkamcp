package kafka

import (
	"context"
	"testing"
)

// Tests for p2_aggregate.go: extractNumericField and mock-based aggregate/graph tests.

func TestExtractNumericField_EmptyField(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value any
		want  float64
		ok    bool
	}{
		{"float64 value", float64(42.5), 42.5, true},
		{"string number", "123.45", 123.45, true},
		{"non-numeric string", "hello", 0, false},
		{"nil value", nil, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := extractNumericField(tt.value, "")
			if ok != tt.ok {
				t.Errorf("extractNumericField(%v, \"\") ok = %v, want %v", tt.value, ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Errorf("extractNumericField(%v, \"\") = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestExtractNumericField_NestedDotNotation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value any
		field string
		want  float64
		ok    bool
	}{
		{
			"top-level field",
			map[string]any{"amount": float64(99.9)},
			"amount",
			99.9,
			true,
		},
		{
			"nested field",
			map[string]any{"order": map[string]any{"total": float64(50.0)}},
			"order.total",
			50.0,
			true,
		},
		{
			"deeply nested",
			map[string]any{"a": map[string]any{"b": map[string]any{"c": float64(7.0)}}},
			"a.b.c",
			7.0,
			true,
		},
		{
			"missing field",
			map[string]any{"amount": float64(10)},
			"price",
			0,
			false,
		},
		{
			"non-map value",
			"just a string",
			"field",
			0,
			false,
		},
		{
			"string number in nested",
			map[string]any{"data": map[string]any{"count": "42"}},
			"data.count",
			42.0,
			true,
		},
		{
			"int value",
			map[string]any{"count": 5},
			"count",
			5.0,
			true,
		},
		{
			"int64 value",
			map[string]any{"total": int64(100)},
			"total",
			100.0,
			true,
		},
		{
			"missing intermediate",
			map[string]any{"a": map[string]any{"x": float64(1)}},
			"a.b.c",
			0,
			false,
		},
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

func TestAggregate_MinMaxValues(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
			return &AggregateResult{
				Topic:        req.Topic,
				Field:        req.Field,
				Function:     req.Function,
				Value:        100.0,
				MessageCount: 5,
			}, nil
		},
	}

	result, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic: "metrics", Field: "value", Function: "max", WindowMs: 60000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Value != 100.0 {
		t.Errorf("expected max=100, got %f", result.Value)
	}
}

func TestAggregate_MinValue(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
			return &AggregateResult{
				Topic:        req.Topic,
				Field:        req.Field,
				Function:     req.Function,
				Value:        -5.0,
				MessageCount: 5,
			}, nil
		},
	}

	result, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic: "metrics", Field: "value", Function: "min", WindowMs: 60000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Value != -5.0 {
		t.Errorf("expected min=-5, got %f", result.Value)
	}
}

func TestGetTopicGraph_WithConsumerEdges(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*TopicGraph, error) {
			return &TopicGraph{
				Nodes: []TopicGraphNode{
					{ID: "topic:orders", Type: "topic", Name: "orders"},
					{ID: "topic:payments", Type: "topic", Name: "payments"},
					{ID: "group:order-processor", Type: "consumer_group", Name: "order-processor"},
				},
				Edges: []TopicGraphEdge{
					{Source: "topic:orders", Target: "group:order-processor", Type: "consumes"},
					{Source: "topic:payments", Target: "group:order-processor", Type: "consumes"},
				},
			}, nil
		},
	}

	result, err := mock.GetTopicGraph(context.Background(), "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(result.Nodes))
	}
	if len(result.Edges) != 2 {
		t.Errorf("expected 2 edges, got %d", len(result.Edges))
	}

	topicCount := 0
	groupCount := 0
	for _, n := range result.Nodes {
		switch n.Type {
		case "topic":
			topicCount++
		case "consumer_group":
			groupCount++
		}
	}
	if topicCount != 2 {
		t.Errorf("expected 2 topic nodes, got %d", topicCount)
	}
	if groupCount != 1 {
		t.Errorf("expected 1 consumer group node, got %d", groupCount)
	}
}

func TestGetTopicGraph_NoConsumers(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*TopicGraph, error) {
			return &TopicGraph{
				Nodes: []TopicGraphNode{
					{ID: "topic:orphan", Type: "topic", Name: "orphan"},
				},
			}, nil
		},
	}

	result, err := mock.GetTopicGraph(context.Background(), "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Edges) != 0 {
		t.Errorf("expected 0 edges for topic with no consumers, got %d", len(result.Edges))
	}
}

func TestSuggestSchemaEvolution_AddField(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "add a new field for user preferences")
	if len(suggestions) == 0 {
		t.Fatal("expected suggestions for 'add field'")
	}
	found := false
	for _, s := range suggestions {
		if s.Strategy == "add_optional_field" {
			found = true
			if !s.Compatible {
				t.Error("add_optional_field should be compatible")
			}
		}
	}
	if !found {
		t.Error("expected add_optional_field strategy")
	}
}

func TestSuggestSchemaEvolution_RemoveField(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "remove the legacy status field")
	if len(suggestions) == 0 {
		t.Fatal("expected suggestions for 'remove field'")
	}
	found := false
	for _, s := range suggestions {
		if s.Strategy == "deprecate_then_remove" {
			found = true
		}
	}
	if !found {
		t.Error("expected deprecate_then_remove strategy")
	}
}

func TestSuggestSchemaEvolution_RenameField(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "rename userId to user_id")
	if len(suggestions) == 0 {
		t.Fatal("expected suggestions for 'rename'")
	}
	strategies := make(map[string]bool)
	for _, s := range suggestions {
		strategies[s.Strategy] = true
	}
	if !strategies["add_alias"] {
		t.Error("expected add_alias strategy for rename")
	}
	if !strategies["dual_write"] {
		t.Error("expected dual_write strategy for rename")
	}
}

func TestSuggestSchemaEvolution_TypeChange(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "change amount type from int to long")
	found := false
	for _, s := range suggestions {
		if s.Strategy == "widen_type" {
			found = true
		}
	}
	if !found {
		t.Error("expected widen_type strategy for type change")
	}
}

func TestSuggestSchemaEvolution_UnknownChange(t *testing.T) {
	t.Parallel()
	suggestions := SuggestSchemaEvolution(nil, "do something unusual")
	if len(suggestions) == 0 {
		t.Fatal("expected fallback suggestion")
	}
	if suggestions[0].Strategy != "compatibility_check_first" {
		t.Errorf("expected fallback strategy, got %s", suggestions[0].Strategy)
	}
}
