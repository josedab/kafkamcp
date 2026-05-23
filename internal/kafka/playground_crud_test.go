package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestPlaygroundCreate_TopicNameFormat(t *testing.T) {
	t.Parallel()
	var createdTopic string
	mock := &MockKafkaManager{
		PlaygroundCreateFn: func(ctx context.Context, cluster, agentID, name string, partitions int32) (*PlaygroundTopicResult, error) {
			createdTopic = fmt.Sprintf("%s%s_%s", playgroundPrefix, agentID, name)
			return &PlaygroundTopicResult{
				TopicName:   createdTopic,
				Partitions:  partitions,
				Status:      "created",
				RetentionMs: 3600000,
			}, nil
		},
	}

	result, err := mock.PlaygroundCreate(context.Background(), "default", "agent-1", "test-topic", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(result.TopicName, "_playground_") {
		t.Errorf("expected topic to start with '_playground_', got %q", result.TopicName)
	}
	if !strings.Contains(result.TopicName, "agent-1") {
		t.Errorf("expected topic to contain agent ID, got %q", result.TopicName)
	}
	if !strings.Contains(result.TopicName, "test-topic") {
		t.Errorf("expected topic to contain name, got %q", result.TopicName)
	}
}

func TestPlaygroundCreate_RetentionConfig(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundCreateFn: func(ctx context.Context, cluster, agentID, name string, partitions int32) (*PlaygroundTopicResult, error) {
			return &PlaygroundTopicResult{
				TopicName:   "_playground_agent_test",
				Partitions:  partitions,
				Status:      "created",
				RetentionMs: 3600000,
			}, nil
		},
	}

	result, err := mock.PlaygroundCreate(context.Background(), "default", "agent", "test", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.RetentionMs != 3600000 {
		t.Errorf("expected retention 3600000ms (1hr), got %d", result.RetentionMs)
	}
	if result.Status != "created" {
		t.Errorf("expected status 'created', got %q", result.Status)
	}
}

func TestPlaygroundCreate_Error(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundCreateFn: func(ctx context.Context, cluster, agentID, name string, partitions int32) (*PlaygroundTopicResult, error) {
			return nil, fmt.Errorf("creating playground topic: topic already exists")
		},
	}

	_, err := mock.PlaygroundCreate(context.Background(), "default", "agent", "existing", 3)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPlaygroundCreate_CallArgs(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundCreateFn: func(ctx context.Context, cluster, agentID, name string, partitions int32) (*PlaygroundTopicResult, error) {
			return &PlaygroundTopicResult{TopicName: "_playground_a_t", Partitions: partitions, Status: "created"}, nil
		},
	}

	_, _ = mock.PlaygroundCreate(context.Background(), "prod", "my-agent", "my-topic", 6)
	if mock.CallCount("PlaygroundCreate") != 1 {
		t.Error("expected PlaygroundCreate called once")
	}
	call := mock.LastCall("PlaygroundCreate")
	if call.Args[0] != "prod" {
		t.Errorf("expected cluster 'prod', got %v", call.Args[0])
	}
	if call.Args[1] != "my-agent" {
		t.Errorf("expected agentID 'my-agent', got %v", call.Args[1])
	}
}

func TestPlaygroundProduceSample_MessageCount(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundProduceSampleFn: func(ctx context.Context, cluster, agentID, name, pattern string, count int) (map[string]any, error) {
			return map[string]any{
				"topic":    "_playground_agent_test",
				"pattern":  pattern,
				"produced": count,
			}, nil
		},
	}

	result, err := mock.PlaygroundProduceSample(context.Background(), "default", "agent", "test", "ecommerce", 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	produced := result["produced"].(int)
	if produced != 10 {
		t.Errorf("expected 10 produced, got %d", produced)
	}
}

func TestPlaygroundProduceSample_Patterns(t *testing.T) {
	t.Parallel()
	patterns := []string{"ecommerce", "iot", "logs", "user_activity"}
	for _, pattern := range patterns {
		t.Run(pattern, func(t *testing.T) {
			msgs := generateSampleData(pattern, 5)
			if len(msgs) != 5 {
				t.Errorf("expected 5 messages for pattern %q, got %d", pattern, len(msgs))
			}
			for _, msg := range msgs {
				if msg.Key == "" {
					t.Errorf("expected non-empty key for pattern %q", pattern)
				}
				if msg.Value == nil {
					t.Errorf("expected non-nil value for pattern %q", pattern)
				}
				// Verify it's valid JSON
				var parsed map[string]any
				rawBytes, ok := msg.Value.(json.RawMessage)
				if !ok {
					t.Errorf("expected json.RawMessage value for pattern %q, got %T", pattern, msg.Value)
					continue
				}
				if err := json.Unmarshal(rawBytes, &parsed); err != nil {
					t.Errorf("expected valid JSON for pattern %q: %v", pattern, err)
				}
			}
		})
	}
}

func TestPlaygroundList_FiltersPlaygroundPrefix(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundListFn: func(ctx context.Context, cluster, agentID string) ([]TopicSummary, error) {
			return []TopicSummary{
				{Name: "_playground_agent1_test1", Partitions: 3},
				{Name: "_playground_agent1_test2", Partitions: 3},
			}, nil
		},
	}

	topics, err := mock.PlaygroundList(context.Background(), "default", "agent1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(topics) != 2 {
		t.Errorf("expected 2 playground topics, got %d", len(topics))
	}
	for _, topic := range topics {
		if !strings.HasPrefix(topic.Name, "_playground_") {
			t.Errorf("expected playground prefix, got %q", topic.Name)
		}
	}
}

func TestPlaygroundList_EmptyResult(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundListFn: func(ctx context.Context, cluster, agentID string) ([]TopicSummary, error) {
			return nil, nil
		},
	}

	topics, err := mock.PlaygroundList(context.Background(), "default", "agent1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topics != nil {
		t.Errorf("expected nil, got %v", topics)
	}
}

func TestPlaygroundList_Error(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundListFn: func(ctx context.Context, cluster, agentID string) ([]TopicSummary, error) {
			return nil, fmt.Errorf("cluster unavailable")
		},
	}

	_, err := mock.PlaygroundList(context.Background(), "default", "agent1")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestPlaygroundCleanup_Success(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundCleanupFn: func(ctx context.Context, cluster, agentID string) (int, error) {
			return 3, nil
		},
	}

	deleted, err := mock.PlaygroundCleanup(context.Background(), "default", "agent1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted != 3 {
		t.Errorf("expected 3 deleted, got %d", deleted)
	}
}

func TestPlaygroundCleanup_PartialFailures(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundCleanupFn: func(ctx context.Context, cluster, agentID string) (int, error) {
			// 2 out of 3 deleted (1 failed)
			return 2, nil
		},
	}

	deleted, err := mock.PlaygroundCleanup(context.Background(), "default", "agent1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted != 2 {
		t.Errorf("expected 2 deleted (partial), got %d", deleted)
	}
}

func TestPlaygroundCleanup_EmptyList(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundCleanupFn: func(ctx context.Context, cluster, agentID string) (int, error) {
			return 0, nil
		},
	}

	deleted, err := mock.PlaygroundCleanup(context.Background(), "default", "agent1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deleted, got %d", deleted)
	}
}

func TestPlaygroundCleanup_Error(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		PlaygroundCleanupFn: func(ctx context.Context, cluster, agentID string) (int, error) {
			return 0, fmt.Errorf("list failed")
		},
	}

	_, err := mock.PlaygroundCleanup(context.Background(), "default", "agent1")
	if err == nil {
		t.Fatal("expected error")
	}
}
