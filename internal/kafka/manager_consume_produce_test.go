package kafka

import (
	"context"
	"fmt"
	"testing"
)

// --------------- MockKafkaManager: Consume ---------------

func TestMock_Consume_Default(t *testing.T) {
	m := &MockKafkaManager{}
	msgs, err := m.Consume(context.Background(), "default", "orders", 0, "earliest", 10, 5000, "auto")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msgs != nil {
		t.Errorf("expected nil messages from default mock, got %v", msgs)
	}
	if m.CallCount("Consume") != 1 {
		t.Errorf("expected 1 call to Consume, got %d", m.CallCount("Consume"))
	}
}

func TestMock_Consume_CustomFn(t *testing.T) {
	m := &MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
			return []Message{
				{Partition: 0, Offset: 100, Key: "key1", Value: map[string]any{"id": 1}, Timestamp: "2024-01-01T00:00:00Z"},
				{Partition: 0, Offset: 101, Key: "key2", Value: map[string]any{"id": 2}, Timestamp: "2024-01-01T00:00:01Z"},
				{Partition: 0, Offset: 102, Key: "key3", Value: map[string]any{"id": 3}, Timestamp: "2024-01-01T00:00:02Z"},
			}, nil
		},
	}

	msgs, err := m.Consume(context.Background(), "prod", "orders", 0, "earliest", 100, 5000, "auto")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}
	if msgs[0].Offset != 100 {
		t.Errorf("expected first message offset=100, got %d", msgs[0].Offset)
	}
	if msgs[2].Key != "key3" {
		t.Errorf("expected third message key='key3', got %v", msgs[2].Key)
	}
}

func TestMock_Consume_Error(t *testing.T) {
	m := &MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
			return nil, fmt.Errorf("cluster %q not found", cluster)
		},
	}

	_, err := m.Consume(context.Background(), "nonexistent", "orders", 0, "earliest", 10, 5000, "auto")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestMock_Consume_PartitionFiltering(t *testing.T) {
	m := &MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
			if partition == -1 {
				return []Message{
					{Partition: 0, Offset: 10},
					{Partition: 1, Offset: 20},
				}, nil
			}
			return []Message{
				{Partition: partition, Offset: 10},
			}, nil
		},
	}

	// All partitions
	msgs, _ := m.Consume(context.Background(), "prod", "orders", -1, "earliest", 100, 5000, "auto")
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages for all partitions, got %d", len(msgs))
	}

	// Specific partition
	msgs, _ = m.Consume(context.Background(), "prod", "orders", 0, "earliest", 100, 5000, "auto")
	if len(msgs) != 1 {
		t.Errorf("expected 1 message for partition 0, got %d", len(msgs))
	}
}

func TestMock_Consume_CallRecording(t *testing.T) {
	m := &MockKafkaManager{}
	_, _ = m.Consume(context.Background(), "prod", "orders", 0, "earliest", 10, 5000, "auto")
	_, _ = m.Consume(context.Background(), "prod", "events", 1, "latest", 50, 3000, "json")

	if m.CallCount("Consume") != 2 {
		t.Errorf("expected 2 calls to Consume, got %d", m.CallCount("Consume"))
	}

	last := m.LastCall("Consume")
	if last == nil {
		t.Fatal("expected last call to be recorded")
	}
	if last.Args[1] != "events" {
		t.Errorf("expected last call topic='events', got %v", last.Args[1])
	}
}

// --------------- MockKafkaManager: Produce ---------------

func TestMock_Produce_Default(t *testing.T) {
	m := &MockKafkaManager{}
	results, err := m.Produce(context.Background(), "default", "orders", nil, ProduceOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results from default mock, got %v", results)
	}
	if m.CallCount("Produce") != 1 {
		t.Errorf("expected 1 call to Produce, got %d", m.CallCount("Produce"))
	}
}

func TestMock_Produce_CustomFn(t *testing.T) {
	m := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			results := make([]DeliveryResult, len(messages))
			for i := range messages {
				results[i] = DeliveryResult{
					Partition: 0,
					Offset:    int64(100 + i),
					Status:    "success",
				}
			}
			return results, nil
		},
	}

	messages := []ProduceMessage{
		{Key: "key1", Value: `{"id": 1}`},
		{Key: "key2", Value: `{"id": 2}`},
	}
	results, err := m.Produce(context.Background(), "prod", "orders", messages, ProduceOptions{Acks: "all"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 delivery results, got %d", len(results))
	}
	if results[0].Offset != 100 {
		t.Errorf("expected first result offset=100, got %d", results[0].Offset)
	}
	if results[1].Status != "success" {
		t.Errorf("expected second result status='success', got %v", results[1].Status)
	}
}

func TestMock_Produce_Error(t *testing.T) {
	m := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			return nil, fmt.Errorf("produce failed: topic %q not found", topic)
		},
	}

	_, err := m.Produce(context.Background(), "prod", "nonexistent", []ProduceMessage{{Key: "k", Value: "v"}}, ProduceOptions{})
	if err == nil {
		t.Fatal("expected error for nonexistent topic")
	}
}

func TestMock_Produce_EmptyMessages(t *testing.T) {
	m := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			return []DeliveryResult{}, nil
		},
	}

	results, err := m.Produce(context.Background(), "prod", "orders", []ProduceMessage{}, ProduceOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty messages, got %d", len(results))
	}
}

func TestMock_Produce_AcksOptions(t *testing.T) {
	var capturedOpts ProduceOptions
	m := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			capturedOpts = opts
			return []DeliveryResult{{Partition: 0, Offset: 0}}, nil
		},
	}

	_, _ = m.Produce(context.Background(), "prod", "orders", []ProduceMessage{{Key: "k", Value: "v"}}, ProduceOptions{Acks: "0", Serializer: "json"})
	if capturedOpts.Acks != "0" {
		t.Errorf("expected acks='0', got %q", capturedOpts.Acks)
	}
	if capturedOpts.Serializer != "json" {
		t.Errorf("expected serializer='json', got %q", capturedOpts.Serializer)
	}
}

// --------------- Manager: Consume/Produce with nonexistent cluster ---------------

func TestManager_Consume_NonexistentCluster(t *testing.T) {
	m := testManager()
	_, err := m.Consume(context.Background(), "nonexistent", "orders", 0, "earliest", 10, 5000, "auto")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestManager_Produce_NonexistentCluster(t *testing.T) {
	m := testManager()
	_, err := m.Produce(context.Background(), "nonexistent", "orders", []ProduceMessage{{Key: "k", Value: "v"}}, ProduceOptions{})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}
