package kafka

import (
	"context"
	"errors"
	"testing"
)

func TestProduce_MockManager_SingleMessage(t *testing.T) {
	t.Parallel()

	mock := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			results := make([]DeliveryResult, len(messages))
			for i := range messages {
				results[i] = DeliveryResult{
					Partition: 0,
					Offset:    int64(i),
					Status:    "delivered",
					Timestamp: "2025-01-01T00:00:00.000Z",
				}
			}
			return results, nil
		},
	}

	results, err := mock.Produce(context.Background(), "default", "test-topic",
		[]ProduceMessage{{Key: "k1", Value: "v1"}},
		ProduceOptions{Acks: "all"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != "delivered" {
		t.Errorf("expected status delivered, got %q", results[0].Status)
	}
}

func TestProduce_MockManager_AcksModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		acks string
	}{
		{"no ack", "0"},
		{"leader ack", "1"},
		{"all acks", "all"},
		{"default empty acks", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var capturedAcks string
			mock := &MockKafkaManager{
				ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
					capturedAcks = opts.Acks
					return []DeliveryResult{{Status: "delivered"}}, nil
				},
			}

			_, err := mock.Produce(context.Background(), "default", "test-topic",
				[]ProduceMessage{{Value: "v1"}},
				ProduceOptions{Acks: tt.acks},
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if capturedAcks != tt.acks {
				t.Errorf("acks = %q, want %q", capturedAcks, tt.acks)
			}
		})
	}
}

func TestProduce_MockManager_HeaderPropagation(t *testing.T) {
	t.Parallel()

	var capturedHeaders map[string]string
	mock := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			if len(messages) > 0 {
				capturedHeaders = messages[0].Headers
			}
			return []DeliveryResult{{Status: "delivered"}}, nil
		},
	}

	headers := map[string]string{
		"correlation-id": "req-123",
		"content-type":   "application/json",
		"x-custom":       "custom-value",
	}

	_, err := mock.Produce(context.Background(), "default", "test-topic",
		[]ProduceMessage{{Key: "k1", Value: "v1", Headers: headers}},
		ProduceOptions{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for k, v := range headers {
		if capturedHeaders[k] != v {
			t.Errorf("header %q = %q, want %q", k, capturedHeaders[k], v)
		}
	}
}

func TestProduce_MockManager_ErrorCounting(t *testing.T) {
	t.Parallel()

	mock := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			results := []DeliveryResult{
				{Status: "delivered", Partition: 0, Offset: 0},
				{Status: "error: delivery failed"},
				{Status: "delivered", Partition: 0, Offset: 1},
			}
			return results, errors.New("1 of 3 messages failed delivery")
		},
	}

	results, err := mock.Produce(context.Background(), "default", "test-topic",
		[]ProduceMessage{{Value: "v1"}, {Value: "v2"}, {Value: "v3"}},
		ProduceOptions{Acks: "all"},
	)
	if err == nil {
		t.Fatal("expected error for partial failure")
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	var failCount int
	for _, r := range results {
		if r.Status != "delivered" {
			failCount++
		}
	}
	if failCount != 1 {
		t.Errorf("expected 1 failure, got %d", failCount)
	}
}

func TestProduce_MockManager_MultipleMessages(t *testing.T) {
	t.Parallel()

	mock := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			results := make([]DeliveryResult, len(messages))
			for i := range messages {
				results[i] = DeliveryResult{
					Partition: 0,
					Offset:    int64(i),
					Status:    "delivered",
				}
			}
			return results, nil
		},
	}

	msgs := make([]ProduceMessage, 5)
	for i := range msgs {
		msgs[i] = ProduceMessage{Key: "k", Value: "v"}
	}

	results, err := mock.Produce(context.Background(), "default", "test-topic", msgs, ProduceOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
}

func TestProduce_MockManager_PartitionOverride(t *testing.T) {
	t.Parallel()

	var capturedPartition *int32
	mock := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			if len(messages) > 0 {
				capturedPartition = messages[0].Partition
			}
			return []DeliveryResult{{Status: "delivered", Partition: 3}}, nil
		},
	}

	p := int32(3)
	_, err := mock.Produce(context.Background(), "default", "test-topic",
		[]ProduceMessage{{Key: "k1", Value: "v1", Partition: &p}},
		ProduceOptions{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedPartition == nil || *capturedPartition != 3 {
		t.Errorf("expected partition override 3")
	}
}

func TestProduce_MockManager_ClusterError(t *testing.T) {
	t.Parallel()

	mock := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			return nil, errors.New("cluster not found")
		},
	}

	results, err := mock.Produce(context.Background(), "nonexistent", "test-topic",
		[]ProduceMessage{{Value: "v1"}},
		ProduceOptions{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if results != nil {
		t.Errorf("expected nil results on error")
	}
}

func TestProduce_MockManager_EmptyMessages(t *testing.T) {
	t.Parallel()

	mock := &MockKafkaManager{
		ProduceFn: func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
			return []DeliveryResult{}, nil
		},
	}

	results, err := mock.Produce(context.Background(), "default", "test-topic",
		[]ProduceMessage{},
		ProduceOptions{},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}
