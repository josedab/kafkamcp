package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleCreateConnector =====================

func TestHandleCreateConnector_MissingName_Mock(t *testing.T) {
	s := newTestServer(t)
	result, err := s.handleCreateConnector(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCreateConnector_MissingConfig(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleCreateConnector(context.Background(), makeToolRequest(map[string]any{
		"name": "my-connector",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCreateConnector_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	result, err := s.handleCreateConnector(context.Background(), makeToolRequest(map[string]any{
		"name":   "my-connector",
		"config": map[string]any{"connector.class": "FileSource"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleUpdateConnector =====================

func TestHandleUpdateConnector_MissingName_Mock(t *testing.T) {
	s := newTestServer(t)
	result, err := s.handleUpdateConnector(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleUpdateConnector_MissingConfig(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleUpdateConnector(context.Background(), makeToolRequest(map[string]any{
		"name": "my-connector",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleDeleteConnector =====================

func TestHandleDeleteConnector_MissingName_Mock(t *testing.T) {
	s := newTestServer(t)
	result, err := s.handleDeleteConnector(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDeleteConnector_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	result, err := s.handleDeleteConnector(context.Background(), makeToolRequest(map[string]any{
		"name": "my-connector",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleRestartConnector =====================

func TestHandleRestartConnector_MissingName_Mock(t *testing.T) {
	s := newTestServer(t)
	result, err := s.handleRestartConnector(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleConnectorHealth =====================

func TestHandleConnectorHealth_MissingName_Mock(t *testing.T) {
	s := newTestServer(t)
	result, err := s.handleConnectorHealth(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleTraceMessage =====================

func TestHandleTraceMessage_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "orders"}, {Name: "payments"}}, nil
		},
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*kafka.TraceResult, error) {
			return &kafka.TraceResult{
				CorrelationID: correlationID,
				Hops: []kafka.TraceHop{
					{Topic: "orders", Partition: 0, Offset: 10, Timestamp: "2024-01-01T00:00:00.000Z"},
				},
				TopicsSearched: 2,
				TotalMessages:  1,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleTraceMessage(context.Background(), makeToolRequest(map[string]any{
		"correlation_id": "corr-123",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "corr-123") {
		t.Error("expected correlation_id in result")
	}
}

func TestHandleTraceMessage_MissingCorrelationID_Mock(t *testing.T) {
	s := newTestServer(t)
	result, err := s.handleTraceMessage(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTraceMessage_WithSpecificTopics(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		TraceMessageFn: func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*kafka.TraceResult, error) {
			if len(topics) != 2 {
				return nil, fmt.Errorf("expected 2 topics, got %d", len(topics))
			}
			return &kafka.TraceResult{CorrelationID: correlationID, TopicsSearched: len(topics)}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleTraceMessage(context.Background(), makeToolRequest(map[string]any{
		"correlation_id": "corr-123",
		"topics":         []any{"orders", "payments"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleListACLs =====================

func TestHandleListACLs_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*kafka.ACLResult, error) {
			return &kafka.ACLResult{
				ACLs: []kafka.ACLEntry{
					{Principal: "User:admin", ResourceName: "*", ResourceType: "Topic", Operation: "All"},
				},
				TotalACLs: 1,
				SecurityAnalysis: &kafka.SecurityAnalysis{
					WildcardACLs: []kafka.SecurityFinding{{Principal: "User:admin", Issue: "wildcard"}},
					Findings:     1,
				},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleListACLs(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "User:admin") {
		t.Error("expected principal in result")
	}
}

func TestHandleListACLs_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	result, err := s.handleListACLs(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleListACLs_Error(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListACLsFn: func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*kafka.ACLResult, error) {
			return nil, fmt.Errorf("unauthorized")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleListACLs(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleTimeTravel =====================

func TestHandleTimeTravel_SnapshotSuccess(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*kafka.TimeTravelSnapshot, error) {
			return &kafka.TimeTravelSnapshot{
				Topic: topic, Timestamp: timestamp, KeyCount: 5,
				State: map[string]any{"k1": "v1"},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleTimeTravel(context.Background(), makeToolRequest(map[string]any{
		"topic": "orders", "action": "snapshot", "timestamp": "2024-01-01T00:00:00Z",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleTimeTravel_MissingTopic_Mock(t *testing.T) {
	s := newTestServer(t)
	result, err := s.handleTimeTravel(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTimeTravel_DiffSuccess(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*kafka.TimeTravelDiffResult, error) {
			return &kafka.TimeTravelDiffResult{
				Topic: topic, TotalChanges: 2,
				Added: map[string]any{"new": "val"},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleTimeTravel(context.Background(), makeToolRequest(map[string]any{
		"topic": "orders", "action": "diff",
		"timestamp_from": "2024-01-01T00:00:00Z", "timestamp_to": "2024-01-02T00:00:00Z",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleTimeTravel_UnknownAction(t *testing.T) {
	s := newTestServer(t)
	result, err := s.handleTimeTravel(context.Background(), makeToolRequest(map[string]any{
		"topic": "orders", "action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== extractStringMapArg =====================

func TestExtractStringMapArg_ValidMap(t *testing.T) {
	req := makeToolRequest(map[string]any{
		"config": map[string]any{"key": "value"},
	})
	result := extractStringMapArg(req, "config")
	if result == nil || result["key"] != "value" {
		t.Errorf("expected {key: value}, got %v", result)
	}
}

func TestExtractStringMapArg_Missing(t *testing.T) {
	req := makeToolRequest(nil)
	result := extractStringMapArg(req, "config")
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}
