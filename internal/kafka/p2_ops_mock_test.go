package kafka

import (
	"context"
	"fmt"
	"testing"
)

// ===================== Aggregate (mock-based) =====================

func TestAggregate_Count(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
			return &AggregateResult{
				Topic: req.Topic, Field: req.Field, Function: req.Function,
				Value: 3.0, MessageCount: 3,
			}, nil
		},
	}
	result, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic: "orders", Field: "amount", Function: "count", WindowMs: 60000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Value != 3.0 {
		t.Errorf("expected count=3, got %f", result.Value)
	}
}

func TestAggregate_InvalidFunction(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
			return nil, fmt.Errorf("unsupported function %q", req.Function)
		},
	}
	_, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic: "orders", Field: "amount", Function: "median",
	})
	if err == nil {
		t.Error("expected error for invalid function")
	}
}

func TestAggregate_EmptyTopic(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
			return &AggregateResult{
				Topic: req.Topic, Field: req.Field, Function: req.Function,
				MessageCount: 0, Value: 0,
			}, nil
		},
	}
	result, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic: "empty-topic", Field: "amount", Function: "sum",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.MessageCount != 0 {
		t.Errorf("expected 0 messages, got %d", result.MessageCount)
	}
	if result.Value != 0 {
		t.Errorf("expected 0 value, got %f", result.Value)
	}
}

func TestAggregate_Sum(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
			return &AggregateResult{
				Topic: req.Topic, Field: req.Field, Function: req.Function,
				Value: 60.0, MessageCount: 3,
			}, nil
		},
	}
	result, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic: "orders", Field: "amount", Function: "sum", WindowMs: 60000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Value != 60.0 {
		t.Errorf("expected sum=60, got %f", result.Value)
	}
}

func TestAggregate_Avg(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
			return &AggregateResult{
				Topic: req.Topic, Function: "avg", Value: 20.0, MessageCount: 3,
			}, nil
		},
	}
	result, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic: "orders", Field: "amount", Function: "avg",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Value != 20.0 {
		t.Errorf("expected avg=20, got %f", result.Value)
	}
}

func TestAggregate_MinMax(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
			switch req.Function {
			case "min":
				return &AggregateResult{Value: 5.0, MessageCount: 3}, nil
			case "max":
				return &AggregateResult{Value: 100.0, MessageCount: 3}, nil
			}
			return nil, fmt.Errorf("unexpected")
		},
	}

	minResult, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic: "t", Field: "v", Function: "min",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if minResult.Value != 5.0 {
		t.Errorf("expected min=5, got %f", minResult.Value)
	}

	maxResult, err := mock.Aggregate(context.Background(), "default", AggregateRequest{
		Topic: "t", Field: "v", Function: "max",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if maxResult.Value != 100.0 {
		t.Errorf("expected max=100, got %f", maxResult.Value)
	}
}

// ===================== GetTopicGraph (mock-based) =====================

func TestGetTopicGraph_Success(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*TopicGraph, error) {
			return &TopicGraph{
				Nodes: []TopicGraphNode{
					{ID: "topic:orders", Type: "topic", Name: "orders"},
					{ID: "topic:payments", Type: "topic", Name: "payments"},
					{ID: "group:processor", Type: "consumer_group", Name: "processor"},
				},
				Edges: []TopicGraphEdge{
					{Source: "topic:orders", Target: "group:processor", Type: "consumes"},
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
	if len(result.Edges) != 1 {
		t.Errorf("expected 1 edge, got %d", len(result.Edges))
	}
}

func TestGetTopicGraph_EmptyCluster(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*TopicGraph, error) {
			return &TopicGraph{}, nil
		},
	}
	result, err := mock.GetTopicGraph(context.Background(), "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(result.Nodes))
	}
}

func TestGetTopicGraph_Error(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*TopicGraph, error) {
			return nil, fmt.Errorf("cluster unavailable")
		},
	}
	_, err := mock.GetTopicGraph(context.Background(), "default")
	if err == nil {
		t.Error("expected error")
	}
}

// ===================== JoinTopics (mock-based) =====================

func TestJoinTopics_Success(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		JoinTopicsFn: func(ctx context.Context, cluster string, req JoinRequest) (*JoinResult, error) {
			return &JoinResult{
				Joined: []JoinedMessage{
					{Key: "k1", Left: "left-val", Right: "right-val", LeftTopic: req.LeftTopic, RightTopic: req.RightTopic},
				},
				LeftCount: 5, RightCount: 3, MatchCount: 1, WindowMs: req.WindowMs,
			}, nil
		},
	}
	result, err := mock.JoinTopics(context.Background(), "default", JoinRequest{
		LeftTopic: "orders", RightTopic: "payments", KeyField: "order_id", WindowMs: 60000,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.MatchCount != 1 {
		t.Errorf("expected 1 match, got %d", result.MatchCount)
	}
	if result.Joined[0].LeftTopic != "orders" {
		t.Errorf("expected left_topic=orders, got %s", result.Joined[0].LeftTopic)
	}
}

func TestJoinTopics_NoMatchingKeys(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		JoinTopicsFn: func(ctx context.Context, cluster string, req JoinRequest) (*JoinResult, error) {
			return &JoinResult{
				Joined: nil, LeftCount: 5, RightCount: 5, MatchCount: 0,
			}, nil
		},
	}
	result, err := mock.JoinTopics(context.Background(), "default", JoinRequest{
		LeftTopic: "a", RightTopic: "b", KeyField: "id",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.MatchCount != 0 {
		t.Errorf("expected 0 matches, got %d", result.MatchCount)
	}
}

func TestJoinTopics_EmptyTopics(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		JoinTopicsFn: func(ctx context.Context, cluster string, req JoinRequest) (*JoinResult, error) {
			return &JoinResult{LeftCount: 0, RightCount: 0, MatchCount: 0}, nil
		},
	}
	result, err := mock.JoinTopics(context.Background(), "default", JoinRequest{
		LeftTopic: "empty1", RightTopic: "empty2",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.LeftCount != 0 || result.RightCount != 0 {
		t.Error("expected zero counts for empty topics")
	}
}

func TestJoinTopics_ConsumeError(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		JoinTopicsFn: func(ctx context.Context, cluster string, req JoinRequest) (*JoinResult, error) {
			return nil, fmt.Errorf("consuming left topic: connection refused")
		},
	}
	_, err := mock.JoinTopics(context.Background(), "default", JoinRequest{
		LeftTopic: "a", RightTopic: "b",
	})
	if err == nil {
		t.Error("expected error")
	}
}

// ===================== NaturalLanguageQuery (mock-based) =====================

func TestNaturalLanguageQuery_Success(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		NaturalLanguageQueryFn: func(ctx context.Context, cluster, query string, topics []TopicSummary) (*NaturalLanguageQueryResult, error) {
			return &NaturalLanguageQueryResult{
				InterpretedAs: "Consume 10 messages from topic \"orders\"",
				Topic:         "orders",
				Messages: []Message{
					{Partition: 0, Offset: 1, Key: "k1", Value: "v1"},
				},
				Count: 1,
			}, nil
		},
	}
	result, err := mock.NaturalLanguageQuery(context.Background(), "default", "show me orders", []TopicSummary{
		{Name: "orders"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Topic != "orders" {
		t.Errorf("expected topic=orders, got %s", result.Topic)
	}
	if result.Count != 1 {
		t.Errorf("expected count=1, got %d", result.Count)
	}
}

func TestNaturalLanguageQuery_NoMatchingTopic(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		NaturalLanguageQueryFn: func(ctx context.Context, cluster, query string, topics []TopicSummary) (*NaturalLanguageQueryResult, error) {
			return nil, fmt.Errorf("could not determine topic from query")
		},
	}
	_, err := mock.NaturalLanguageQuery(context.Background(), "default", "xyz", nil)
	if err == nil {
		t.Error("expected error for no matching topic")
	}
}

func TestNaturalLanguageQuery_WithFilter(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		NaturalLanguageQueryFn: func(ctx context.Context, cluster, query string, topics []TopicSummary) (*NaturalLanguageQueryResult, error) {
			return &NaturalLanguageQueryResult{
				InterpretedAs: "Consume from \"orders\" filtered by \"error\"",
				Topic:         "orders",
				Filter:        "error",
				Messages:      []Message{{Key: "err-1", Value: "error msg"}},
				Count:         1,
			}, nil
		},
	}
	result, err := mock.NaturalLanguageQuery(context.Background(), "default", "show error orders", []TopicSummary{
		{Name: "orders"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Filter != "error" {
		t.Errorf("expected filter=error, got %s", result.Filter)
	}
}

func TestNaturalLanguageQuery_LimitEnforcement(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		NaturalLanguageQueryFn: func(ctx context.Context, cluster, query string, topics []TopicSummary) (*NaturalLanguageQueryResult, error) {
			return &NaturalLanguageQueryResult{
				Topic: "orders", Count: 5,
				Messages: make([]Message, 5),
			}, nil
		},
	}
	result, err := mock.NaturalLanguageQuery(context.Background(), "default", "last 5 orders", []TopicSummary{
		{Name: "orders"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Count != 5 {
		t.Errorf("expected count=5, got %d", result.Count)
	}
}

// ===================== MockKafkaManager call recording =====================

func TestMockCallRecording(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{}
	_, _ = mock.Aggregate(context.Background(), "c1", AggregateRequest{Topic: "t1"})
	_, _ = mock.Aggregate(context.Background(), "c2", AggregateRequest{Topic: "t2"})

	if mock.CallCount("Aggregate") != 2 {
		t.Errorf("expected 2 Aggregate calls, got %d", mock.CallCount("Aggregate"))
	}
	last := mock.LastCall("Aggregate")
	if last == nil {
		t.Fatal("expected last call")
	}
}
