package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleCostWaste =====================

func TestHandleCostWaste_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 3, MessageCount: 50000, RetentionMs: 604800000},
				{Name: "idle-topic", Partitions: 6, ReplicationFactor: 3, MessageCount: 0, RetentionMs: 2592000000},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleCostWaste(context.Background(), makeToolRequest(map[string]any{
		"provider": "aws-msk",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)

	var parsed map[string]any
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
}

func TestHandleCostWaste_EmptyCluster(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleCostWaste(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleCostWaste_ListTopicsError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, fmt.Errorf("broker unavailable")
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleCostWaste(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "Failed to list topics") {
		t.Error("expected error message in result")
	}
}

// ===================== handleCostWhatIf =====================

func TestHandleCostWhatIf_RetentionChange(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{
				Name:              "orders",
				ReplicationFactor: 3,
				Configs:           map[string]string{"retention.ms": "604800000"},
				Partitions: []kafka.PartitionDetail{
					{ID: 0, EarliestOffset: 0, LatestOffset: 10000},
					{ID: 1, EarliestOffset: 0, LatestOffset: 10000},
					{ID: 2, EarliestOffset: 0, LatestOffset: 10000},
				},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleCostWhatIf(context.Background(), makeToolRequest(map[string]any{
		"topic":            "orders",
		"new_retention_ms": float64(86400000),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "scenario") {
		t.Error("expected scenario in result")
	}
}

func TestHandleCostWhatIf_CompressionChange(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{
				Name:              "events",
				ReplicationFactor: 3,
				Configs:           map[string]string{"retention.ms": "604800000", "compression.type": "none"},
				Partitions: []kafka.PartitionDetail{
					{ID: 0, EarliestOffset: 0, LatestOffset: 5000},
				},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleCostWhatIf(context.Background(), makeToolRequest(map[string]any{
		"topic":           "events",
		"new_compression": "lz4",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleCostWhatIf_PartitionCountChange(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{
				Name:              "orders",
				ReplicationFactor: 3,
				Configs:           map[string]string{"retention.ms": "604800000"},
				Partitions: []kafka.PartitionDetail{
					{ID: 0, EarliestOffset: 0, LatestOffset: 10000},
				},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleCostWhatIf(context.Background(), makeToolRequest(map[string]any{
		"topic":          "orders",
		"new_partitions": float64(6),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleCostWhatIf_TopicRequired(t *testing.T) {
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})

	result, err := s.handleCostWhatIf(context.Background(), makeToolRequest(map[string]any{}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "required") {
		t.Error("expected 'required' error for missing topic")
	}
}

func TestHandleCostWhatIf_DescribeError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return nil, fmt.Errorf("topic not found")
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleCostWhatIf(context.Background(), makeToolRequest(map[string]any{
		"topic": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
