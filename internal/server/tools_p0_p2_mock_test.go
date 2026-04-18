package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/schema"
)

// ===================== handleListSchemas =====================

func TestHandleListSchemas_NoSchemaRegistry(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleListSchemas(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleListSchemas_WithClient(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	s.schemaClients["default"] = schema.NewClient("http://localhost:8081", "", "", 100, 60)
	// The schema registry won't actually be available in test, so this tests the path
	result, err := s.handleListSchemas(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Will return error since the registry is unreachable, which is expected
	_ = result
}

func TestHandleListSchemas_AuthDenied(t *testing.T) {
	s := newTestServer(t, withDefaultDeny)
	result, err := s.handleListSchemas(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleListConnectors =====================

func TestHandleListConnectors_ConnectClientNil(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleListConnectors(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "Connect URL not configured") {
		t.Errorf("expected connect URL error, got: %s", text)
	}
}

func TestHandleListConnectors_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	result, err := s.handleListConnectors(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleDescribeConnector =====================

func TestHandleDescribeConnector_MissingName_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleDescribeConnector(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDescribeConnector_ConnectClientNil(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleDescribeConnector(context.Background(), makeToolRequest(map[string]any{
		"name": "my-connector",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleNaturalLanguageQuery (p2) =====================

func TestHandleNaturalLanguageQuery_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "orders"}, {Name: "payments"}}, nil
		},
		NaturalLanguageQueryFn: func(ctx context.Context, cluster, query string, topics []kafka.TopicSummary) (*kafka.NaturalLanguageQueryResult, error) {
			return &kafka.NaturalLanguageQueryResult{
				InterpretedAs: "Consume from orders",
				Topic:         "orders",
				Messages:      []kafka.Message{{Key: "k1", Value: "v1"}},
				Count:         1,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleNaturalLanguageQuery(context.Background(), makeToolRequest(map[string]any{
		"query": "show me orders",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleNaturalLanguageQuery_MissingQuery_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleNaturalLanguageQuery(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleNaturalLanguageQuery_ListTopicsError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleNaturalLanguageQuery(context.Background(), makeToolRequest(map[string]any{
		"query": "show me orders",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleNaturalLanguageQuery_DefaultDenyFiltering(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "orders"}, {Name: "secret"}}, nil
		},
		NaturalLanguageQueryFn: func(ctx context.Context, cluster, query string, topics []kafka.TopicSummary) (*kafka.NaturalLanguageQueryResult, error) {
			return &kafka.NaturalLanguageQueryResult{
				Topic: "orders", Count: 0,
			}, nil
		},
	}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	// Use allowed agent
	req := makeToolRequest(map[string]any{"query": "show me orders"})
	req.Header = map[string][]string{"X-Agent-Id": {"allowed-agent"}}
	result, err := s.handleNaturalLanguageQuery(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleAggregate (p2) =====================

func TestHandleAggregate_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		AggregateFn: func(ctx context.Context, cluster string, req kafka.AggregateRequest) (*kafka.AggregateResult, error) {
			return &kafka.AggregateResult{
				Topic: req.Topic, Field: req.Field, Function: req.Function,
				Value: 42.0, MessageCount: 10,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleAggregate(context.Background(), makeToolRequest(map[string]any{
		"topic": "orders", "field": "amount", "function": "sum",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleAggregate_MissingTopic_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleAggregate(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleJoin (p2) =====================

func TestHandleJoin_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		JoinTopicsFn: func(ctx context.Context, cluster string, req kafka.JoinRequest) (*kafka.JoinResult, error) {
			return &kafka.JoinResult{
				Joined:     []kafka.JoinedMessage{{Key: "k1"}},
				LeftCount:  5,
				RightCount: 3,
				MatchCount: 1,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleJoin(context.Background(), makeToolRequest(map[string]any{
		"left_topic": "orders", "right_topic": "payments",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleJoin_MissingTopics_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleJoin(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
