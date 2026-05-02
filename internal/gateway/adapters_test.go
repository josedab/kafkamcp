//go:build gateway_nats && gateway_kinesis && gateway_pulsar

package gateway

import (
	"context"
	"log/slog"
	"testing"
)

func TestNATSAdapter_ConnectNoEndpoints(t *testing.T) {
	t.Parallel()
	adapter := NewNATSAdapter(BackendConfig{}, slog.Default())
	err := adapter.Connect(context.Background())
	if err == nil {
		t.Fatal("expected error for no endpoints")
	}
}

func TestNATSAdapter_OperationsWhenDisconnected(t *testing.T) {
	t.Parallel()
	adapter := NewNATSAdapter(BackendConfig{
		Endpoints: []string{"localhost:4222"},
	}, slog.Default())
	ctx := context.Background()

	if _, err := adapter.Produce(ctx, ProduceRequest{Topic: "t"}); err == nil {
		t.Error("expected error from Produce when disconnected")
	}
	if _, err := adapter.Consume(ctx, ConsumeOptions{Topic: "t"}); err == nil {
		t.Error("expected error from Consume when disconnected")
	}
	if _, err := adapter.ListTopics(ctx); err == nil {
		t.Error("expected error from ListTopics when disconnected")
	}
	if err := adapter.CreateTopic(ctx, "t", 1, nil); err == nil {
		t.Error("expected error from CreateTopic when disconnected")
	}
	if err := adapter.DeleteTopic(ctx, "t"); err == nil {
		t.Error("expected error from DeleteTopic when disconnected")
	}
	if err := adapter.Healthy(ctx); err == nil {
		t.Error("expected error from Healthy when disconnected")
	}
}

func TestNATSAdapter_Protocol(t *testing.T) {
	t.Parallel()
	adapter := NewNATSAdapter(BackendConfig{}, slog.Default())
	if adapter.Protocol() != ProtocolNATS {
		t.Errorf("expected NATS, got %s", adapter.Protocol())
	}
}

func TestNATSAdapter_FullLifecycle(t *testing.T) {
	t.Parallel()
	adapter := NewNATSAdapter(BackendConfig{
		Endpoints: []string{"localhost:4222"},
	}, slog.Default())
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := adapter.Healthy(ctx); err != nil {
		t.Fatalf("healthy: %v", err)
	}

	// Create topic
	if err := adapter.CreateTopic(ctx, "test-subj", 1, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Duplicate create
	if err := adapter.CreateTopic(ctx, "test-subj", 1, nil); err == nil {
		t.Error("expected error for duplicate subject")
	}

	// List
	topics, err := adapter.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(topics) != 1 || topics[0].Name != "test-subj" {
		t.Errorf("unexpected topics: %v", topics)
	}

	// Produce
	res, err := adapter.Produce(ctx, ProduceRequest{Topic: "test-subj", Value: []byte("hi")})
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if res.Protocol != ProtocolNATS {
		t.Errorf("expected NATS protocol in result, got %s", res.Protocol)
	}

	// Consume
	msgs, err := adapter.Consume(ctx, ConsumeOptions{Topic: "test-subj"})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if msgs == nil {
		t.Error("expected non-nil messages slice")
	}

	// Delete
	if err := adapter.DeleteTopic(ctx, "test-subj"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Delete non-existent
	if err := adapter.DeleteTopic(ctx, "test-subj"); err == nil {
		t.Error("expected error for deleting non-existent subject")
	}

	// Close
	if err := adapter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestPulsarAdapter_ConnectNoEndpoints(t *testing.T) {
	t.Parallel()
	adapter := NewPulsarAdapter(BackendConfig{}, slog.Default())
	err := adapter.Connect(context.Background())
	if err == nil {
		t.Fatal("expected error for no endpoints")
	}
}

func TestPulsarAdapter_OperationsWhenDisconnected(t *testing.T) {
	t.Parallel()
	adapter := NewPulsarAdapter(BackendConfig{
		Endpoints: []string{"localhost:6650"},
	}, slog.Default())
	ctx := context.Background()

	if _, err := adapter.Produce(ctx, ProduceRequest{Topic: "t"}); err == nil {
		t.Error("expected error from Produce when disconnected")
	}
	if _, err := adapter.Consume(ctx, ConsumeOptions{Topic: "t"}); err == nil {
		t.Error("expected error from Consume when disconnected")
	}
	if _, err := adapter.ListTopics(ctx); err == nil {
		t.Error("expected error from ListTopics when disconnected")
	}
	if err := adapter.CreateTopic(ctx, "t", 1, nil); err == nil {
		t.Error("expected error from CreateTopic when disconnected")
	}
	if err := adapter.DeleteTopic(ctx, "t"); err == nil {
		t.Error("expected error from DeleteTopic when disconnected")
	}
	if err := adapter.Healthy(ctx); err == nil {
		t.Error("expected error from Healthy when disconnected")
	}
}

func TestPulsarAdapter_Protocol(t *testing.T) {
	t.Parallel()
	adapter := NewPulsarAdapter(BackendConfig{}, slog.Default())
	if adapter.Protocol() != ProtocolPulsar {
		t.Errorf("expected Pulsar, got %s", adapter.Protocol())
	}
}

func TestPulsarAdapter_FullLifecycle(t *testing.T) {
	t.Parallel()
	adapter := NewPulsarAdapter(BackendConfig{
		Endpoints: []string{"localhost:6650"},
	}, slog.Default())
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Create
	if err := adapter.CreateTopic(ctx, "p-topic", 3, map[string]string{"retention": "1h"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := adapter.CreateTopic(ctx, "p-topic", 3, nil); err == nil {
		t.Error("expected error for duplicate topic")
	}

	// List
	topics, err := adapter.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(topics) != 1 || topics[0].Partitions != 3 {
		t.Errorf("unexpected topics: %v", topics)
	}

	// Produce
	res, err := adapter.Produce(ctx, ProduceRequest{Topic: "p-topic", Value: []byte("data")})
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if res.Protocol != ProtocolPulsar {
		t.Errorf("expected Pulsar protocol, got %s", res.Protocol)
	}

	// Delete
	if err := adapter.DeleteTopic(ctx, "p-topic"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := adapter.DeleteTopic(ctx, "p-topic"); err == nil {
		t.Error("expected error for non-existent topic")
	}

	if err := adapter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestRabbitMQAdapter_ConnectNoEndpoints(t *testing.T) {
	t.Parallel()
	adapter := NewRabbitMQAdapter(BackendConfig{}, slog.Default())
	err := adapter.Connect(context.Background())
	if err == nil {
		t.Fatal("expected error for no endpoints")
	}
}

func TestRabbitMQAdapter_OperationsWhenDisconnected(t *testing.T) {
	t.Parallel()
	adapter := NewRabbitMQAdapter(BackendConfig{
		Endpoints: []string{"localhost:5672"},
	}, slog.Default())
	ctx := context.Background()

	if _, err := adapter.Produce(ctx, ProduceRequest{Topic: "q"}); err == nil {
		t.Error("expected error from Produce when disconnected")
	}
	if _, err := adapter.Consume(ctx, ConsumeOptions{Topic: "q"}); err == nil {
		t.Error("expected error from Consume when disconnected")
	}
	if _, err := adapter.ListTopics(ctx); err == nil {
		t.Error("expected error from ListTopics when disconnected")
	}
	if err := adapter.CreateTopic(ctx, "q", 1, nil); err == nil {
		t.Error("expected error from CreateTopic when disconnected")
	}
	if err := adapter.DeleteTopic(ctx, "q"); err == nil {
		t.Error("expected error from DeleteTopic when disconnected")
	}
	if err := adapter.Healthy(ctx); err == nil {
		t.Error("expected error from Healthy when disconnected")
	}
}

func TestRabbitMQAdapter_Protocol(t *testing.T) {
	t.Parallel()
	adapter := NewRabbitMQAdapter(BackendConfig{}, slog.Default())
	if adapter.Protocol() != ProtocolRabbitMQ {
		t.Errorf("expected RabbitMQ, got %s", adapter.Protocol())
	}
}

func TestRabbitMQAdapter_FullLifecycle(t *testing.T) {
	t.Parallel()
	adapter := NewRabbitMQAdapter(BackendConfig{
		Endpoints: []string{"localhost:5672"},
	}, slog.Default())
	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Create with config
	if err := adapter.CreateTopic(ctx, "my-queue", 0, map[string]string{"durable": "true"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := adapter.CreateTopic(ctx, "my-queue", 0, nil); err == nil {
		t.Error("expected error for duplicate queue")
	}

	// List
	topics, err := adapter.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(topics) != 1 || topics[0].Name != "my-queue" {
		t.Errorf("unexpected topics: %v", topics)
	}
	// Verify RabbitMQ adds "type": "queue" property
	if topics[0].Properties["type"] != "queue" {
		t.Error("expected 'type'='queue' property on RabbitMQ topic")
	}

	// Produce
	res, err := adapter.Produce(ctx, ProduceRequest{Topic: "my-queue", Value: []byte("msg")})
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if res.Protocol != ProtocolRabbitMQ {
		t.Errorf("expected RabbitMQ protocol, got %s", res.Protocol)
	}

	// Consume
	msgs, err := adapter.Consume(ctx, ConsumeOptions{Topic: "my-queue"})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if msgs == nil {
		t.Error("expected non-nil messages slice")
	}

	// Delete
	if err := adapter.DeleteTopic(ctx, "my-queue"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := adapter.DeleteTopic(ctx, "my-queue"); err == nil {
		t.Error("expected error for non-existent queue")
	}

	if err := adapter.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}
