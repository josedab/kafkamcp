package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleWorkflows =====================

func TestHandleWorkflows_List(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWorkflows(context.Background(), makeToolRequest(map[string]any{
		"action": "list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "templates") {
		t.Error("expected templates in result")
	}
}

func TestHandleWorkflows_Describe(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWorkflows(context.Background(), makeToolRequest(map[string]any{
		"action":   "describe",
		"template": "dlq-investigation",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleWorkflows_DescribeMissingTemplate(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWorkflows(context.Background(), makeToolRequest(map[string]any{
		"action": "describe",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWorkflows_ExecuteWithParams(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWorkflows(context.Background(), makeToolRequest(map[string]any{
		"action":   "execute",
		"template": "dlq-investigation",
		"topic":    "my-dlq",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleWorkflows_ExecuteMissingTemplate_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWorkflows(context.Background(), makeToolRequest(map[string]any{
		"action": "execute",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWorkflows_ExecuteNotFound(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWorkflows(context.Background(), makeToolRequest(map[string]any{
		"action":   "execute",
		"template": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleCostAnalyze =====================

func TestHandleCostAnalyze_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{
				Name:              topicName,
				ReplicationFactor: 3,
				Partitions: []kafka.PartitionDetail{
					{EarliestOffset: 0, LatestOffset: 10000},
				},
				Configs: map[string]string{"retention.ms": "604800000"},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleCostAnalyze(context.Background(), makeToolRequest(map[string]any{
		"topic": "orders",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleCostAnalyze_MissingTopic_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleCostAnalyze(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCostAnalyze_DescribeError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return nil, fmt.Errorf("topic not found")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleCostAnalyze(context.Background(), makeToolRequest(map[string]any{
		"topic": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleKnowledgeGraphQuery =====================

func TestHandleKnowledgeGraphQuery_MissingQuery(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleKnowledgeGraphQuery(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleKnowledgeGraphQuery_EmptyGraph(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleKnowledgeGraphQuery(context.Background(), makeToolRequest(map[string]any{
		"query": "show all topics",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== refreshKnowledgeGraph =====================

func TestRefreshKnowledgeGraph_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "orders", Partitions: 3}}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{{GroupID: "processor", State: "Stable"}}, nil
		},
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return &kafka.ConsumerGroupDetail{
				GroupID: groupID,
				Offsets: []kafka.GroupOffsetDetail{{Topic: "orders", Partition: 0}},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	err := s.refreshKnowledgeGraph(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRefreshKnowledgeGraph_ListTopicsError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	s := newTestServerWithMock(t, mock)
	err := s.refreshKnowledgeGraph(context.Background(), "")
	if err == nil {
		t.Error("expected error")
	}
}
