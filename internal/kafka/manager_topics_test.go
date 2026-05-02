package kafka

import (
	"context"
	"fmt"
	"testing"
)

// --------------- MockKafkaManager: ListTopics ---------------

func TestMock_ListTopics_Default(t *testing.T) {
	t.Parallel()
	m := &MockKafkaManager{}
	topics, err := m.ListTopics(context.Background(), "default", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if topics != nil {
		t.Errorf("expected nil topics from default mock, got %v", topics)
	}
	if m.CallCount("ListTopics") != 1 {
		t.Errorf("expected 1 call to ListTopics, got %d", m.CallCount("ListTopics"))
	}
}

func TestMock_ListTopics_CustomFn(t *testing.T) {
	t.Parallel()
	m := &MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error) {
			return []TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 2, MessageCount: 1000},
				{Name: "events", Partitions: 6, ReplicationFactor: 3, MessageCount: 5000},
			}, nil
		},
	}

	topics, err := m.ListTopics(context.Background(), "prod", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(topics))
	}
	if topics[0].Name != "orders" {
		t.Errorf("expected first topic 'orders', got %q", topics[0].Name)
	}
	if topics[1].Partitions != 6 {
		t.Errorf("expected second topic partitions=6, got %d", topics[1].Partitions)
	}
}

func TestMock_ListTopics_Error(t *testing.T) {
	t.Parallel()
	m := &MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error) {
			return nil, fmt.Errorf("cluster %q not found", cluster)
		},
	}

	_, err := m.ListTopics(context.Background(), "nonexistent", false)
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestMock_ListTopics_IncludeInternal(t *testing.T) {
	t.Parallel()
	m := &MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error) {
			topics := []TopicSummary{
				{Name: "orders", Partitions: 3, IsInternal: false},
			}
			if includeInternal {
				topics = append(topics, TopicSummary{Name: "__consumer_offsets", Partitions: 50, IsInternal: true})
			}
			return topics, nil
		},
	}

	topics, err := m.ListTopics(context.Background(), "prod", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(topics) != 2 {
		t.Errorf("expected 2 topics with internal, got %d", len(topics))
	}

	topics, err = m.ListTopics(context.Background(), "prod", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(topics) != 1 {
		t.Errorf("expected 1 topic without internal, got %d", len(topics))
	}
}

// --------------- MockKafkaManager: DescribeTopic ---------------

func TestMock_DescribeTopic_Default(t *testing.T) {
	t.Parallel()
	m := &MockKafkaManager{}
	detail, err := m.DescribeTopic(context.Background(), "default", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if detail != nil {
		t.Errorf("expected nil detail from default mock, got %v", detail)
	}
	if m.CallCount("DescribeTopic") != 1 {
		t.Errorf("expected 1 call to DescribeTopic, got %d", m.CallCount("DescribeTopic"))
	}
}

func TestMock_DescribeTopic_CustomFn(t *testing.T) {
	t.Parallel()
	m := &MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*TopicDetail, error) {
			if topicName == "orders" {
				return &TopicDetail{
					Name:              "orders",
					ReplicationFactor: 3,
					Partitions: []PartitionDetail{
						{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}, EarliestOffset: 0, LatestOffset: 500},
						{ID: 1, Leader: 2, Replicas: []int32{2, 3, 1}, ISR: []int32{2, 3, 1}, EarliestOffset: 0, LatestOffset: 600},
						{ID: 2, Leader: 3, Replicas: []int32{3, 1, 2}, ISR: []int32{3, 1, 2}, EarliestOffset: 0, LatestOffset: 400},
					},
					Configs: map[string]string{
						"retention.ms":   "604800000",
						"cleanup.policy": "delete",
					},
					MessageCount: 1500,
				}, nil
			}
			return nil, fmt.Errorf("topic %q not found", topicName)
		},
	}

	detail, err := m.DescribeTopic(context.Background(), "prod", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if detail.Name != "orders" {
		t.Errorf("expected topic name 'orders', got %q", detail.Name)
	}
	if len(detail.Partitions) != 3 {
		t.Errorf("expected 3 partitions, got %d", len(detail.Partitions))
	}
	if detail.ReplicationFactor != 3 {
		t.Errorf("expected replication factor 3, got %d", detail.ReplicationFactor)
	}
	if detail.MessageCount != 1500 {
		t.Errorf("expected message count 1500, got %d", detail.MessageCount)
	}
	if detail.Configs["retention.ms"] != "604800000" {
		t.Errorf("expected retention.ms=604800000, got %q", detail.Configs["retention.ms"])
	}
}

func TestMock_DescribeTopic_NotFound(t *testing.T) {
	t.Parallel()
	m := &MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*TopicDetail, error) {
			return nil, fmt.Errorf("topic %q not found", topicName)
		},
	}

	_, err := m.DescribeTopic(context.Background(), "prod", "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent topic")
	}
}

func TestMock_DescribeTopic_CallRecording(t *testing.T) {
	t.Parallel()
	m := &MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*TopicDetail, error) {
			return &TopicDetail{Name: topicName}, nil
		},
	}

	_, _ = m.DescribeTopic(context.Background(), "prod", "orders")
	_, _ = m.DescribeTopic(context.Background(), "staging", "events")

	if m.CallCount("DescribeTopic") != 2 {
		t.Errorf("expected 2 calls to DescribeTopic, got %d", m.CallCount("DescribeTopic"))
	}

	last := m.LastCall("DescribeTopic")
	if last == nil {
		t.Fatal("expected last call to be recorded")
	}
	if last.Args[0] != "staging" || last.Args[1] != "events" {
		t.Errorf("expected last call args [staging, events], got %v", last.Args)
	}
}

// --------------- Manager: ListTopics/DescribeTopic with nonexistent cluster ---------------

func TestManager_ListTopics_NonexistentCluster(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.ListTopics(context.Background(), "nonexistent", false)
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestManager_DescribeTopic_NonexistentCluster(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.DescribeTopic(context.Background(), "nonexistent", "orders")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}
