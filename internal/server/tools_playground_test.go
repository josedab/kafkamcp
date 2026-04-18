package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handlePlayground - create =====================

func TestHandlePlayground_CreateSuccess(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PlaygroundCreateFn: func(ctx context.Context, cluster, agentID, name string, partitions int32) (*kafka.PlaygroundTopicResult, error) {
			return &kafka.PlaygroundTopicResult{
				TopicName:   "_playground_" + agentID + "_" + name,
				Partitions:  partitions,
				Status:      "created",
				RetentionMs: 3600000,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "create",
		"name":   "my-test",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "created") {
		t.Error("expected 'created' status in result")
	}
	if mock.CallCount("PlaygroundCreate") != 1 {
		t.Error("expected PlaygroundCreate called once")
	}
}

func TestHandlePlayground_CreateError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PlaygroundCreateFn: func(ctx context.Context, cluster, agentID, name string, partitions int32) (*kafka.PlaygroundTopicResult, error) {
			return nil, fmt.Errorf("topic already exists")
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "create",
		"name":   "existing",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlePlayground_CreatePartitionCap(t *testing.T) {
	var gotPartitions int32
	mock := &kafka.MockKafkaManager{
		PlaygroundCreateFn: func(ctx context.Context, cluster, agentID, name string, partitions int32) (*kafka.PlaygroundTopicResult, error) {
			gotPartitions = partitions
			return &kafka.PlaygroundTopicResult{
				TopicName:  "_playground_test",
				Partitions: partitions,
				Status:     "created",
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	// Requesting 20 partitions should be capped at 12
	s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action":     "create",
		"name":       "test",
		"partitions": float64(20),
	}))
	if gotPartitions > 12 {
		t.Errorf("expected partitions capped at 12, got %d", gotPartitions)
	}
}

// ===================== handlePlayground - produce_sample =====================

func TestHandlePlayground_ProduceSampleSuccess(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PlaygroundProduceSampleFn: func(ctx context.Context, cluster, agentID, name, pattern string, count int) (map[string]any, error) {
			return map[string]any{
				"topic":    "_playground_agent_test",
				"pattern":  pattern,
				"produced": count,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action":       "produce_sample",
		"name":         "test",
		"data_pattern": "iot",
		"count":        float64(10),
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "iot") {
		t.Error("expected pattern in result")
	}
}

func TestHandlePlayground_ProduceSampleError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PlaygroundProduceSampleFn: func(ctx context.Context, cluster, agentID, name, pattern string, count int) (map[string]any, error) {
			return nil, fmt.Errorf("topic not found")
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "produce_sample",
		"name":   "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandlePlayground_ProduceSampleCountCap(t *testing.T) {
	var gotCount int
	mock := &kafka.MockKafkaManager{
		PlaygroundProduceSampleFn: func(ctx context.Context, cluster, agentID, name, pattern string, count int) (map[string]any, error) {
			gotCount = count
			return map[string]any{"produced": count}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "produce_sample",
		"name":   "test",
		"count":  float64(500),
	}))
	if gotCount > 100 {
		t.Errorf("expected count capped at 100, got %d", gotCount)
	}
}

// ===================== handlePlayground - list =====================

func TestHandlePlayground_ListSuccess(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PlaygroundListFn: func(ctx context.Context, cluster, agentID string) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "_playground_agent_test1", Partitions: 3},
				{Name: "_playground_agent_test2", Partitions: 3},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "list",
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
	count := parsed["count"].(float64)
	if int(count) != 2 {
		t.Errorf("expected 2 topics, got %v", count)
	}
}

func TestHandlePlayground_ListEmpty(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PlaygroundListFn: func(ctx context.Context, cluster, agentID string) ([]kafka.TopicSummary, error) {
			return nil, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandlePlayground_ListError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PlaygroundListFn: func(ctx context.Context, cluster, agentID string) ([]kafka.TopicSummary, error) {
			return nil, fmt.Errorf("cluster unavailable")
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handlePlayground - cleanup =====================

func TestHandlePlayground_CleanupSuccess(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PlaygroundCleanupFn: func(ctx context.Context, cluster, agentID string) (int, error) {
			return 3, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "cleanup",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "cleaned_up") {
		t.Error("expected 'cleaned_up' status")
	}
	if !strings.Contains(text, "3") {
		t.Error("expected 3 topics deleted")
	}
}

func TestHandlePlayground_CleanupError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PlaygroundCleanupFn: func(ctx context.Context, cluster, agentID string) (int, error) {
			return 0, fmt.Errorf("cleanup failed")
		},
	}
	s := newTestServerWithMock(t, mock)

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "cleanup",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handlePlayground - invalid action =====================

func TestHandlePlayground_InvalidAction(t *testing.T) {
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "destroy",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "unknown action") {
		t.Error("expected 'unknown action' error")
	}
}

// ===================== handlePlayground - auth denied =====================

func TestHandlePlayground_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock

	result, err := s.handlePlayground(context.Background(), makeToolRequest(map[string]any{
		"action": "create",
		"name":   "test",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if mock.CallCount("PlaygroundCreate") != 0 {
		t.Error("PlaygroundCreate should not be called when auth denied")
	}
}
