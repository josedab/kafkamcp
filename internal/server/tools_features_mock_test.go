package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/nlquery"
)

// ===================== handleGitOpsDiff =====================

func TestHandleGitOpsDiff_ValidDesiredState(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 2},
			}, nil
		},
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{
				Name:              topicName,
				ReplicationFactor: 2,
				Partitions:        make([]kafka.PartitionDetail, 3),
				Configs:           map[string]string{"retention.ms": "604800000"},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleGitOpsDiff(ctx, makeToolRequest(map[string]any{
		"topics": []any{
			map[string]any{"name": "orders", "partitions": float64(3), "replication_factor": float64(2)},
			map[string]any{"name": "new-topic", "partitions": float64(6)},
		},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "diff") {
		t.Error("expected diff in result")
	}
}

func TestHandleGitOpsDiff_EmptyDesiredState(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "orders", Partitions: 3}}, nil
		},
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{Name: topicName, Partitions: make([]kafka.PartitionDetail, 3), Configs: map[string]string{}}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleGitOpsDiff(context.Background(), makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleGitOpsDiff_ListTopicsError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleGitOpsDiff(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleNLQuery =====================

func TestHandleNLQuery_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "orders"}, {Name: "payments"}}, nil
		},
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 1, Key: "k1", Value: map[string]any{"status": "ok"}, Timestamp: "2024-01-01T00:00:00.000Z"},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleNLQuery(context.Background(), makeToolRequest(map[string]any{
		"query": "show me recent orders",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "orders") {
		t.Error("expected orders topic in result")
	}
}

func TestHandleNLQuery_MissingQuery_Mock(t *testing.T) {
	s := newTestServer(t)
	result, err := s.handleNLQuery(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleNLQuery_ListTopicsError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleNLQuery(context.Background(), makeToolRequest(map[string]any{
		"query": "show me orders",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleAnomalies =====================

func TestHandleAnomalies_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleAnomalies(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "alerts") {
		t.Error("expected alerts in result")
	}
}

func TestHandleAnomalies_NilDetector_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	s.anomalyDetector = nil
	result, err := s.handleAnomalies(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleAnomalies_SeverityFilter(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleAnomalies(context.Background(), makeToolRequest(map[string]any{
		"severity": "critical",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleAnomalies_MetricFilter(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleAnomalies(context.Background(), makeToolRequest(map[string]any{
		"metric": "throughput",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleAnomalies_LimitParam(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleAnomalies(context.Background(), makeToolRequest(map[string]any{
		"limit": float64(5),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== matchesFilters =====================

func TestMatchesFilters_EmptyFilters(t *testing.T) {
	msg := kafka.Message{Value: map[string]any{"status": "ok"}}
	if !matchesFilters(msg, nil) {
		t.Error("expected true for empty filters")
	}
}

func TestMatchesFilters_EqMatch(t *testing.T) {
	msg := kafka.Message{Value: map[string]any{"status": "error"}}
	filters := []nlquery.Filter{
		{Field: "status", Operator: "eq", Value: "error"},
	}
	if !matchesFilters(msg, filters) {
		t.Error("expected match for eq filter")
	}
}

func TestMatchesFilters_EqNoMatch(t *testing.T) {
	msg := kafka.Message{Value: map[string]any{"status": "ok"}}
	filters := []nlquery.Filter{
		{Field: "status", Operator: "eq", Value: "error"},
	}
	if matchesFilters(msg, filters) {
		t.Error("expected no match for eq filter")
	}
}

// ===================== extractFieldValue =====================

func TestExtractFieldValue_EmptyField(t *testing.T) {
	val := extractFieldValue("hello", "")
	if val != "hello" {
		t.Errorf("expected 'hello', got %v", val)
	}
}

func TestExtractFieldValue_MapField(t *testing.T) {
	val := extractFieldValue(map[string]any{"status": "ok"}, "status")
	if val != "ok" {
		t.Errorf("expected 'ok', got %v", val)
	}
}

func TestExtractFieldValue_NonMap(t *testing.T) {
	val := extractFieldValue("not-a-map", "field")
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

// ===================== intFromAny =====================

func TestIntFromAny_Types(t *testing.T) {
	if intFromAny(float64(42), 0) != 42 {
		t.Error("expected 42 from float64")
	}
	if intFromAny(int(7), 0) != 7 {
		t.Error("expected 7 from int")
	}
	if intFromAny(int64(99), 0) != 99 {
		t.Error("expected 99 from int64")
	}
	if intFromAny("invalid", 5) != 5 {
		t.Error("expected default 5 for invalid type")
	}
}

// ===================== estimateThroughput =====================

func TestEstimateThroughput_WithMessages(t *testing.T) {
	detail := &kafka.TopicDetail{
		Partitions: []kafka.PartitionDetail{
			{EarliestOffset: 0, LatestOffset: 3600},
		},
	}
	tp := estimateThroughput(detail)
	if tp != 1.0 {
		t.Errorf("expected 1.0 msg/s, got %f", tp)
	}
}

func TestEstimateThroughput_Empty(t *testing.T) {
	detail := &kafka.TopicDetail{
		Partitions: []kafka.PartitionDetail{
			{EarliestOffset: 0, LatestOffset: 0},
		},
	}
	tp := estimateThroughput(detail)
	if tp != 1 {
		t.Errorf("expected 1 (default), got %f", tp)
	}
}

// verify response JSON shape
func TestHandleAnomalies_ResponseShape(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, _ := s.handleAnomalies(context.Background(), makeToolRequest(nil))
	text := resultText(t, result)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		t.Fatalf("result should be valid JSON: %v", err)
	}
	if _, ok := parsed["alert_count"]; !ok {
		t.Error("expected 'alert_count' key in response")
	}
}
