package gateway

import (
	"context"
	"log/slog"
	"testing"
)

func TestRegisterBackend(t *testing.T) {
	m := NewManager(slog.Default())

	err := m.RegisterBackend(BackendConfig{
		Name:      "test-pulsar",
		Protocol:  ProtocolPulsar,
		Endpoints: []string{"localhost:6650"},
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Duplicate should fail
	err = m.RegisterBackend(BackendConfig{
		Name:      "test-pulsar",
		Protocol:  ProtocolPulsar,
		Endpoints: []string{"localhost:6650"},
	})
	if err == nil {
		t.Fatal("expected error for duplicate backend")
	}
}

func TestListBackends(t *testing.T) {
	m := NewManager(slog.Default())

	m.RegisterBackend(BackendConfig{
		Name:      "test-nats",
		Protocol:  ProtocolNATS,
		Endpoints: []string{"localhost:4222"},
	})

	backends := m.ListBackends()
	if len(backends) != 1 {
		t.Fatalf("expected 1 backend, got %d", len(backends))
	}
	if backends[0].Protocol != ProtocolNATS {
		t.Fatalf("expected NATS protocol, got %s", backends[0].Protocol)
	}
}

func TestPulsarAdapter(t *testing.T) {
	adapter := NewPulsarAdapter(BackendConfig{
		Endpoints: []string{"localhost:6650"},
	}, slog.Default())

	ctx := context.Background()

	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	if err := adapter.Healthy(ctx); err != nil {
		t.Fatalf("healthy check failed: %v", err)
	}

	if err := adapter.CreateTopic(ctx, "test-topic", 3, nil); err != nil {
		t.Fatalf("create topic failed: %v", err)
	}

	topics, err := adapter.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list topics failed: %v", err)
	}
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}

	if err := adapter.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
}

func TestRabbitMQAdapter(t *testing.T) {
	adapter := NewRabbitMQAdapter(BackendConfig{
		Endpoints: []string{"localhost:5672"},
	}, slog.Default())

	ctx := context.Background()
	adapter.Connect(ctx)

	err := adapter.CreateTopic(ctx, "test-queue", 0, nil)
	if err != nil {
		t.Fatalf("create queue failed: %v", err)
	}

	err = adapter.DeleteTopic(ctx, "test-queue")
	if err != nil {
		t.Fatalf("delete queue failed: %v", err)
	}
}

func TestNATSAdapter(t *testing.T) {
	adapter := NewNATSAdapter(BackendConfig{
		Endpoints: []string{"localhost:4222"},
	}, slog.Default())

	ctx := context.Background()
	adapter.Connect(ctx)

	result, err := adapter.Produce(ctx, ProduceRequest{Topic: "test.subject", Value: []byte("hello")})
	if err != nil {
		t.Fatalf("produce failed: %v", err)
	}
	if result.Protocol != ProtocolNATS {
		t.Fatalf("expected NATS protocol, got %s", result.Protocol)
	}
}
