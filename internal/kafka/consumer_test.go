package kafka

import (
	"context"
	"errors"
	"testing"
)

func TestConsume_MockManager_DefaultOffset(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
			return []Message{
				{Partition: 0, Offset: 0, Key: "k1", Value: "v1"},
				{Partition: 0, Offset: 1, Key: "k2", Value: "v2"},
			}, nil
		},
	}

	msgs, err := mock.Consume(context.Background(), "default", "test-topic", -1, "earliest", 10, 5000, "auto")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if mock.CallCount("Consume") != 1 {
		t.Errorf("expected 1 Consume call, got %d", mock.CallCount("Consume"))
	}
}

func TestConsume_MockManager_OffsetPositioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		offset     string
		partition  int32
		wantCount  int
	}{
		{"earliest offset all partitions", "earliest", -1, 3},
		{"latest offset all partitions", "latest", -1, 1},
		{"specific offset single partition", "42", 0, 2},
		{"specific partition", "earliest", 2, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var capturedOffset string
			var capturedPartition int32
			mock := &MockKafkaManager{
				ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
					capturedOffset = offset
					capturedPartition = partition
					msgs := make([]Message, tt.wantCount)
					for i := range msgs {
						msgs[i] = Message{Partition: partition, Offset: int64(i), Key: "k", Value: "v"}
					}
					return msgs, nil
				},
			}

			msgs, err := mock.Consume(context.Background(), "default", "test-topic", tt.partition, tt.offset, 100, 5000, "auto")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(msgs) != tt.wantCount {
				t.Errorf("got %d messages, want %d", len(msgs), tt.wantCount)
			}
			if capturedOffset != tt.offset {
				t.Errorf("offset = %q, want %q", capturedOffset, tt.offset)
			}
			if capturedPartition != tt.partition {
				t.Errorf("partition = %d, want %d", capturedPartition, tt.partition)
			}
		})
	}
}

func TestConsume_MockManager_PartitionDistribution(t *testing.T) {
	t.Parallel()

	mock := &MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
			// Simulate messages from multiple partitions
			return []Message{
				{Partition: 0, Offset: 0, Key: "k0", Value: "v0"},
				{Partition: 1, Offset: 0, Key: "k1", Value: "v1"},
				{Partition: 2, Offset: 0, Key: "k2", Value: "v2"},
			}, nil
		},
	}

	msgs, err := mock.Consume(context.Background(), "default", "multi-part-topic", -1, "earliest", 100, 5000, "auto")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(msgs))
	}

	partitions := make(map[int32]bool)
	for _, m := range msgs {
		partitions[m.Partition] = true
	}
	if len(partitions) != 3 {
		t.Errorf("expected messages from 3 partitions, got %d", len(partitions))
	}
}

func TestConsume_MockManager_Deserialization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		deserializer string
		value        any
	}{
		{"auto deserializer", "auto", map[string]any{"key": "value"}},
		{"json deserializer", "json", `{"key":"value"}`},
		{"string deserializer", "string", "plain text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var capturedDeserializer string
			mock := &MockKafkaManager{
				ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
					capturedDeserializer = deserializer
					return []Message{{Partition: 0, Offset: 0, Value: tt.value}}, nil
				},
			}

			msgs, err := mock.Consume(context.Background(), "default", "test-topic", 0, "earliest", 1, 5000, tt.deserializer)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(msgs) != 1 {
				t.Fatalf("expected 1 message, got %d", len(msgs))
			}
			if capturedDeserializer != tt.deserializer {
				t.Errorf("deserializer = %q, want %q", capturedDeserializer, tt.deserializer)
			}
		})
	}
}

func TestConsume_MockManager_Error(t *testing.T) {
	t.Parallel()

	mock := &MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
			return nil, errors.New("broker unavailable")
		},
	}

	msgs, err := mock.Consume(context.Background(), "default", "test-topic", 0, "earliest", 10, 5000, "auto")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if msgs != nil {
		t.Errorf("expected nil messages on error, got %d", len(msgs))
	}
	if err.Error() != "broker unavailable" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestConsume_MockManager_EmptyResult(t *testing.T) {
	t.Parallel()

	mock := &MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
			return []Message{}, nil
		},
	}

	msgs, err := mock.Consume(context.Background(), "default", "empty-topic", -1, "earliest", 10, 5000, "auto")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages, got %d", len(msgs))
	}
}

func TestConsume_MockManager_Headers(t *testing.T) {
	t.Parallel()

	mock := &MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
			return []Message{
				{
					Partition: 0,
					Offset:    0,
					Key:       "k1",
					Value:     "v1",
					Headers:   map[string]string{"trace-id": "abc123", "content-type": "application/json"},
				},
			}, nil
		},
	}

	msgs, err := mock.Consume(context.Background(), "default", "test-topic", 0, "earliest", 1, 5000, "auto")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatal("expected 1 message")
	}
	if msgs[0].Headers["trace-id"] != "abc123" {
		t.Errorf("expected header trace-id=abc123, got %q", msgs[0].Headers["trace-id"])
	}
}

func TestSubscribe_MockManager_CallRecording(t *testing.T) {
	t.Parallel()

	mock := &MockKafkaManager{
		SubscribeFn: func(ctx context.Context, cluster string, req SubscribeRequest) (*SubscribeResult, error) {
			return &SubscribeResult{
				Messages: []Message{{Partition: 0, Offset: 0, Value: "v"}},
				Count:    1,
				Topic:    req.Topic,
				GroupID:  req.GroupID,
			}, nil
		},
	}

	result, err := mock.Subscribe(context.Background(), "default", SubscribeRequest{
		Topic:   "test-topic",
		GroupID: "test-group",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Count != 1 {
		t.Errorf("expected count 1, got %d", result.Count)
	}
	if mock.CallCount("Subscribe") != 1 {
		t.Errorf("expected 1 Subscribe call, got %d", mock.CallCount("Subscribe"))
	}

	call := mock.LastCall("Subscribe")
	if call == nil {
		t.Fatal("expected last call to be recorded")
	}
	if call.Method != "Subscribe" {
		t.Errorf("expected method Subscribe, got %q", call.Method)
	}
}
