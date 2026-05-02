package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

// newTestServerWithMock constructs a Server with a MockKafkaManager.
func newTestServerWithMock(t *testing.T, mock *kafka.MockKafkaManager) *Server {
	t.Helper()
	s := newTestServer(t)
	s.kafkaManager = mock
	return s
}

func assertNotError(t *testing.T, result *mcp.CallToolResult) {
	t.Helper()
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.IsError {
		t.Errorf("expected IsError=false, got error result")
	}
}

func resultText(t *testing.T, result *mcp.CallToolResult) string {
	t.Helper()
	if result == nil || len(result.Content) == 0 {
		return ""
	}
	tc, ok := result.Content[0].(mcp.TextContent)
	if !ok {
		t.Fatalf("expected TextContent, got %T", result.Content[0])
	}
	return tc.Text
}

// ===================== handleListTopics with mock =====================

func TestHandleListTopics_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 2, MessageCount: 1000},
				{Name: "events", Partitions: 6, ReplicationFactor: 3, MessageCount: 5000},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleListTopics(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	if !strings.Contains(text, "orders") || !strings.Contains(text, "events") {
		t.Errorf("expected topics in result, got: %s", text)
	}
	if mock.CallCount("ListTopics") != 1 {
		t.Error("expected ListTopics to be called once")
	}
}

func TestHandleListTopics_WithFilter(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders.created", Partitions: 3},
				{Name: "orders.deleted", Partitions: 3},
				{Name: "payments.created", Partitions: 6},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleListTopics(ctx, makeToolRequest(map[string]any{"filter": "orders.*"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	if !strings.Contains(text, "orders.created") {
		t.Error("expected orders.created in filtered result")
	}
	if strings.Contains(text, "payments.created") {
		t.Error("payments.created should be filtered out")
	}
}

func TestHandleListTopics_Error(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleListTopics(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "Failed to list topics") {
		t.Error("expected error message in result")
	}
}

func TestHandleListTopics_ClusterResolution(t *testing.T) {
	t.Parallel()
	var calledCluster string
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			calledCluster = cluster
			return nil, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	s.handleListTopics(ctx, makeToolRequest(map[string]any{"cluster": "staging"}))
	if calledCluster != "staging" {
		t.Errorf("expected cluster 'staging', got %q", calledCluster)
	}
}

// ===================== handleConsume with mock =====================

func TestHandleConsume_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 100, Key: "key1", Value: map[string]any{"status": "ok"}, Timestamp: "2024-01-01T00:00:00.000Z"},
				{Partition: 0, Offset: 101, Key: "key2", Value: "raw-string", Timestamp: "2024-01-01T00:00:01.000Z"},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleConsume(ctx, makeToolRequest(map[string]any{"topic": "orders"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	if !strings.Contains(text, "key1") || !strings.Contains(text, "key2") {
		t.Errorf("expected message keys in result, got: %s", text)
	}
}

func TestHandleConsume_Error(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return nil, fmt.Errorf("timeout waiting for messages")
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleConsume(ctx, makeToolRequest(map[string]any{"topic": "orders"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleProduce with mock =====================

func TestHandleProduce_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []kafka.ProduceMessage, opts kafka.ProduceOptions) ([]kafka.DeliveryResult, error) {
			results := make([]kafka.DeliveryResult, len(messages))
			for i := range messages {
				results[i] = kafka.DeliveryResult{
					Partition: 0,
					Offset:    int64(i + 1),
					Timestamp: "2024-01-01T00:00:00.000Z",
					Status:    "delivered",
				}
			}
			return results, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic":    "orders",
		"messages": []any{map[string]any{"key": "k1", "value": "v1"}},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	if !strings.Contains(text, "delivered") {
		t.Errorf("expected 'delivered' in result, got: %s", text)
	}
}

func TestHandleProduce_DeliveryFailure(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []kafka.ProduceMessage, opts kafka.ProduceOptions) ([]kafka.DeliveryResult, error) {
			return nil, fmt.Errorf("broker unavailable")
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleProduce(ctx, makeToolRequest(map[string]any{
		"topic":    "orders",
		"messages": []any{map[string]any{"value": "test"}},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleDescribeTopic with mock =====================

func TestHandleDescribeTopic_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{
				Name:              "orders",
				ReplicationFactor: 3,
				MessageCount:      42000,
				Configs:           map[string]string{"retention.ms": "604800000"},
				Partitions: []kafka.PartitionDetail{
					{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}, EarliestOffset: 0, LatestOffset: 14000},
					{ID: 1, Leader: 2, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 3, 1}, EarliestOffset: 0, LatestOffset: 14000},
					{ID: 2, Leader: 3, Replicas: []int32{3, 1, 2}, ISR: []int32{3, 1, 2}, EarliestOffset: 0, LatestOffset: 14000},
				},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleDescribeTopic(ctx, makeToolRequest(map[string]any{"topic": "orders"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	if !strings.Contains(text, "orders") {
		t.Error("expected topic name in result")
	}
	if !strings.Contains(text, "retention.ms") {
		t.Error("expected configs in result")
	}
}

func TestHandleDescribeTopic_NotFound(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return nil, fmt.Errorf("topic %q not found", topicName)
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleDescribeTopic(ctx, makeToolRequest(map[string]any{"topic": "nonexistent"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "Failed to describe topic") {
		t.Error("expected 'Failed to describe topic' in error")
	}
}

// ===================== handleListConsumerGroups with mock =====================

func TestHandleListConsumerGroups_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{
				{GroupID: "order-processor", State: "Stable", MemberCount: 3, TotalLag: 0},
				{GroupID: "analytics", State: "Empty", MemberCount: 0, TotalLag: 500},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleListConsumerGroups(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	if !strings.Contains(text, "order-processor") {
		t.Error("expected consumer group in result")
	}
}

// ===================== handleDescribeConsumerGroup with mock =====================

func TestHandleDescribeConsumerGroup_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return &kafka.ConsumerGroupDetail{
				GroupID:      "order-processor",
				State:        "Stable",
				ProtocolType: "consumer",
				Protocol:     "range",
				TotalLag:     42,
				Members: []kafka.GroupMemberDetail{
					{MemberID: "m1", ClientID: "client-1", Host: "/10.0.0.1"},
				},
				Offsets: []kafka.GroupOffsetDetail{
					{Topic: "orders", Partition: 0, CurrentOffset: 100, LogEndOffset: 142, Lag: 42},
				},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleDescribeConsumerGroup(ctx, makeToolRequest(map[string]any{"group_id": "order-processor"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if !strings.Contains(resultText(t, result), "order-processor") {
		t.Error("expected group_id in result")
	}
}

// ===================== handleSubscribe with mock =====================

func TestHandleSubscribe_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		SubscribeFn: func(ctx context.Context, cluster string, req kafka.SubscribeRequest) (*kafka.SubscribeResult, error) {
			return &kafka.SubscribeResult{
				Messages:   []kafka.Message{{Partition: 0, Offset: 10, Key: "k", Value: "v"}},
				Count:      1,
				Topic:      req.Topic,
				GroupID:    "test-group",
				AutoCommit: req.AutoCommit,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleSubscribe(ctx, makeToolRequest(map[string]any{
		"topic":    "events",
		"group_id": "test-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleAnalyzeDLQ with mock =====================

func TestHandleAnalyzeDLQ_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				Topic:         "orders-dlq",
				TotalMessages: 25,
				ErrorTypes:    map[string]int{"serialization": 10, "timeout": 15},
				Samples: []kafka.Message{
					{Partition: 0, Offset: 1, Key: "k", Value: "v"},
				},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleAnalyzeDLQ(ctx, makeToolRequest(map[string]any{"topic": "orders-dlq"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	if !strings.Contains(text, "serialization") {
		t.Error("expected error types in result")
	}
}

// ===================== handleSearch with mock =====================

func TestHandleSearch_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		SearchMessagesFn: func(ctx context.Context, cluster string, req kafka.SearchRequest) ([]kafka.SearchMessage, int, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 42, Key: "order-123", Value: map[string]any{"status": "failed"}},
			}, 100, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	result, err := s.handleSearch(ctx, makeToolRequest(map[string]any{
		"topic":          "orders",
		"value_contains": "failed",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== MCP response format verification =====================

func TestHandleListTopics_ResponseFormat(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "test", Partitions: 1}}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleListTopics(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	text := resultText(t, result)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		t.Fatalf("result should be valid JSON: %v", err)
	}
	if _, ok := parsed["topics"]; !ok {
		t.Error("expected 'topics' key in response")
	}
	if _, ok := parsed["total_count"]; !ok {
		t.Error("expected 'total_count' key in response")
	}
}

// ===================== Error propagation =====================

func TestHandleConsume_ErrorPropagation(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return nil, fmt.Errorf("topic \"nonexistent\" not found")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, _ := s.handleConsume(context.Background(), makeToolRequest(map[string]any{"topic": "nonexistent"}))
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "Failed to consume") {
		t.Error("expected error message to propagate")
	}
}

// ===================== handleResetOffsets with mock =====================

func TestHandleResetOffsets_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ResetOffsetsFn: func(ctx context.Context, cluster string, req kafka.ResetOffsetsRequest) (*kafka.ResetOffsetsResult, error) {
			return &kafka.ResetOffsetsResult{
				Offsets: []kafka.GroupOffsetDetail{
					{Topic: "orders", Partition: 0, CurrentOffset: 0},
				},
				DryRun: req.DryRun,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleResetOffsets(context.Background(), makeToolRequest(map[string]any{
		"group_id": "my-group",
		"topic":    "orders",
		"strategy": "earliest",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleCreateTopic with mock =====================

func TestHandleCreateTopic_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		CreateTopicFn: func(ctx context.Context, cluster string, req kafka.CreateTopicRequest) error {
			return nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleCreateTopic(context.Background(), makeToolRequest(map[string]any{
		"name":       "new-topic",
		"partitions": float64(3),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	if mock.CallCount("CreateTopic") != 1 {
		t.Error("expected CreateTopic to be called")
	}
}

func TestHandleCreateTopic_Error(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		CreateTopicFn: func(ctx context.Context, cluster string, req kafka.CreateTopicRequest) error {
			return fmt.Errorf("topic already exists")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, _ := s.handleCreateTopic(context.Background(), makeToolRequest(map[string]any{"name": "existing"}))
	assertIsError(t, result)
}

// ===================== handleDeleteTopic with mock =====================

func TestHandleDeleteTopic_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DeleteTopicFn: func(ctx context.Context, cluster, topicName string) error {
			return nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleDeleteTopic(context.Background(), makeToolRequest(map[string]any{"name": "old-topic"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleAlterTopic with mock =====================

func TestHandleAlterTopic_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		AlterTopicFn: func(ctx context.Context, cluster string, req kafka.AlterTopicRequest) error {
			return nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleAlterTopic(context.Background(), makeToolRequest(map[string]any{
		"name":    "my-topic",
		"configs": map[string]any{"retention.ms": "86400000"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== Auth denied paths with mock =====================

func TestHandleListTopics_AuthDenied(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	ctx := context.Background()

	result, err := s.handleListTopics(ctx, makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if mock.CallCount("ListTopics") != 0 {
		t.Error("ListTopics should not be called when auth denied")
	}
}

func TestHandleConsume_AuthDenied(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock

	result, _ := s.handleConsume(context.Background(), makeToolRequest(map[string]any{"topic": "orders"}))
	assertIsError(t, result)
	if mock.CallCount("Consume") != 0 {
		t.Error("Consume should not be called when auth denied")
	}
}

func TestHandleProduce_AuthDenied(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock

	result, _ := s.handleProduce(context.Background(), makeToolRequest(map[string]any{
		"topic":    "orders",
		"messages": []any{map[string]any{"value": "test"}},
	}))
	assertIsError(t, result)
	if mock.CallCount("Produce") != 0 {
		t.Error("Produce should not be called when auth denied")
	}
}
