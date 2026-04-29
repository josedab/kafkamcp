package gateway

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// ============= EventHubs Adapter Tests =============

func TestEventHubsAdapter_Protocol(t *testing.T) {
	a := NewEventHubsAdapter(BackendConfig{}, testLogger())
	if a.Protocol() != ProtocolEventHubs {
		t.Errorf("expected %s, got %s", ProtocolEventHubs, a.Protocol())
	}
}

func TestEventHubsAdapter_ConnectNoEndpoints(t *testing.T) {
	a := NewEventHubsAdapter(BackendConfig{}, testLogger())
	err := a.Connect(context.Background())
	if err == nil {
		t.Fatal("expected error for no endpoints")
	}
}

func TestEventHubsAdapter_OperationsWhenDisconnected(t *testing.T) {
	a := NewEventHubsAdapter(BackendConfig{}, testLogger())

	_, err := a.Produce(context.Background(), ProduceRequest{Topic: "test"})
	if err == nil {
		t.Error("expected error for produce when disconnected")
	}

	_, err = a.Consume(context.Background(), ConsumeOptions{Topic: "test"})
	if err == nil {
		t.Error("expected error for consume when disconnected")
	}

	_, err = a.ListTopics(context.Background())
	if err == nil {
		t.Error("expected error for list topics when disconnected")
	}

	err = a.CreateTopic(context.Background(), "test", 2, nil)
	if err == nil {
		t.Error("expected error for create topic when disconnected")
	}

	err = a.DeleteTopic(context.Background(), "test")
	if err == nil {
		t.Error("expected error for delete topic when disconnected")
	}

	err = a.Healthy(context.Background())
	if err == nil {
		t.Error("expected error for healthy when disconnected")
	}
}

func TestEventHubsAdapter_FullLifecycle(t *testing.T) {
	a := NewEventHubsAdapter(BackendConfig{
		Endpoints: []string{"eventhub.servicebus.windows.net:9093"},
	}, testLogger())

	ctx := context.Background()

	if err := a.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := a.Healthy(ctx); err != nil {
		t.Fatalf("healthy: %v", err)
	}

	// Create topic
	if err := a.CreateTopic(ctx, "events-hub", 4, map[string]string{"env": "test"}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Duplicate create
	if err := a.CreateTopic(ctx, "events-hub", 4, nil); err == nil {
		t.Error("expected error for duplicate topic")
	}

	// Default partitions
	if err := a.CreateTopic(ctx, "events-default", 0, nil); err != nil {
		t.Fatalf("create with default partitions: %v", err)
	}

	// List topics
	topics, err := a.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	if len(topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(topics))
	}

	// Produce
	result, err := a.Produce(ctx, ProduceRequest{Topic: "events-hub", Key: "k1", Value: []byte("v1")})
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if result.Protocol != ProtocolEventHubs {
		t.Errorf("expected protocol %s, got %s", ProtocolEventHubs, result.Protocol)
	}

	// Consume
	msgs, err := a.Consume(ctx, ConsumeOptions{Topic: "events-hub"})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if msgs == nil {
		t.Error("expected non-nil messages slice")
	}

	// Delete topic
	if err := a.DeleteTopic(ctx, "events-hub"); err != nil {
		t.Fatalf("delete topic: %v", err)
	}

	// Delete non-existent
	if err := a.DeleteTopic(ctx, "nonexistent"); err == nil {
		t.Error("expected error for deleting non-existent topic")
	}

	// Close
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// After close, should be disconnected
	if err := a.Healthy(ctx); err == nil {
		t.Error("expected error after close")
	}
}

// ============= Kinesis Adapter Tests =============

func TestKinesisAdapter_Protocol(t *testing.T) {
	a := NewKinesisAdapter(BackendConfig{}, testLogger())
	if a.Protocol() != ProtocolKinesis {
		t.Errorf("expected %s, got %s", ProtocolKinesis, a.Protocol())
	}
}

func TestKinesisAdapter_ConnectNoEndpoints(t *testing.T) {
	a := NewKinesisAdapter(BackendConfig{}, testLogger())
	err := a.Connect(context.Background())
	if err == nil {
		t.Fatal("expected error for no endpoints")
	}
}

func TestKinesisAdapter_OperationsWhenDisconnected(t *testing.T) {
	a := NewKinesisAdapter(BackendConfig{}, testLogger())

	_, err := a.Produce(context.Background(), ProduceRequest{Topic: "test"})
	if err == nil {
		t.Error("expected error for produce when disconnected")
	}

	_, err = a.Consume(context.Background(), ConsumeOptions{Topic: "test"})
	if err == nil {
		t.Error("expected error for consume when disconnected")
	}

	_, err = a.ListTopics(context.Background())
	if err == nil {
		t.Error("expected error for list topics when disconnected")
	}

	err = a.CreateTopic(context.Background(), "test", 2, nil)
	if err == nil {
		t.Error("expected error for create topic when disconnected")
	}

	err = a.DeleteTopic(context.Background(), "test")
	if err == nil {
		t.Error("expected error for delete topic when disconnected")
	}

	err = a.Healthy(context.Background())
	if err == nil {
		t.Error("expected error for healthy when disconnected")
	}
}

func TestKinesisAdapter_FullLifecycle(t *testing.T) {
	a := NewKinesisAdapter(BackendConfig{
		Endpoints: []string{"kinesis.us-east-1.amazonaws.com"},
	}, testLogger())

	ctx := context.Background()

	if err := a.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := a.Healthy(ctx); err != nil {
		t.Fatalf("healthy: %v", err)
	}

	// Create stream with partitions
	if err := a.CreateTopic(ctx, "my-stream", 3, map[string]string{"env": "test"}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Duplicate create
	if err := a.CreateTopic(ctx, "my-stream", 3, nil); err == nil {
		t.Error("expected error for duplicate stream")
	}

	// Create with default shards (partitions < 1)
	if err := a.CreateTopic(ctx, "default-stream", 0, nil); err != nil {
		t.Fatalf("create with default shards: %v", err)
	}

	// List streams
	topics, err := a.ListTopics(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(topics) != 2 {
		t.Errorf("expected 2 streams, got %d", len(topics))
	}

	// Produce
	result, err := a.Produce(ctx, ProduceRequest{Topic: "my-stream", Key: "pk1"})
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if result.Protocol != ProtocolKinesis {
		t.Errorf("expected protocol %s, got %s", ProtocolKinesis, result.Protocol)
	}

	// Consume
	msgs, err := a.Consume(ctx, ConsumeOptions{Topic: "my-stream"})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if msgs == nil {
		t.Error("expected non-nil messages slice")
	}

	// Delete
	if err := a.DeleteTopic(ctx, "my-stream"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Delete non-existent
	if err := a.DeleteTopic(ctx, "nonexistent"); err == nil {
		t.Error("expected error for deleting non-existent stream")
	}

	// Close
	if err := a.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := a.Healthy(ctx); err == nil {
		t.Error("expected error after close")
	}
}

// ============= categorizeBackendError Tests =============

func TestCategorizeBackendError(t *testing.T) {
	tests := []struct {
		name string
		err  string
		want string
	}{
		{"timeout", "operation timed out: deadline exceeded", "health_check_timeout"},
		{"deadline", "context deadline exceeded", "health_check_timeout"},
		{"connection refused", "dial: connection refused", "connection_failed"},
		{"connect failed", "failed to connect to broker", "connection_failed"},
		{"auth failure", "SASL authentication failed", "authentication_failed"},
		{"permission", "permission denied for topic", "authentication_failed"},
		{"tls error", "tls: certificate verification failed", "tls_error"},
		{"certificate", "x509: certificate has expired", "tls_error"},
		{"generic", "some unknown error", "health_check_failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := categorizeBackendError(
				&mockError{msg: tt.err},
			)
			if got != tt.want {
				t.Errorf("categorizeBackendError(%q) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

type mockError struct{ msg string }

func (e *mockError) Error() string { return e.msg }

// ============= Manager EventHubs/Kinesis Registration =============

func TestManagerRegisterEventHubs(t *testing.T) {
	m := NewManager(testLogger())
	err := m.RegisterBackend(BackendConfig{
		Name:      "eventhubs-test",
		Protocol:  ProtocolEventHubs,
		Endpoints: []string{"ns.servicebus.windows.net:9093"},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	b, err := m.GetBackend("eventhubs-test")
	if err != nil {
		t.Fatalf("get backend: %v", err)
	}
	if b.Protocol() != ProtocolEventHubs {
		t.Errorf("expected %s, got %s", ProtocolEventHubs, b.Protocol())
	}
}

func TestManagerRegisterKinesis(t *testing.T) {
	m := NewManager(testLogger())
	err := m.RegisterBackend(BackendConfig{
		Name:      "kinesis-test",
		Protocol:  ProtocolKinesis,
		Endpoints: []string{"kinesis.us-east-1.amazonaws.com"},
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	b, err := m.GetBackend("kinesis-test")
	if err != nil {
		t.Fatalf("get backend: %v", err)
	}
	if b.Protocol() != ProtocolKinesis {
		t.Errorf("expected %s, got %s", ProtocolKinesis, b.Protocol())
	}
}

func TestManagerRegisterDuplicate(t *testing.T) {
	m := NewManager(testLogger())
	_ = m.RegisterBackend(BackendConfig{
		Name: "dup", Protocol: ProtocolNATS, Endpoints: []string{"localhost:4222"},
	})
	err := m.RegisterBackend(BackendConfig{
		Name: "dup", Protocol: ProtocolNATS, Endpoints: []string{"localhost:4222"},
	})
	if err == nil {
		t.Error("expected error for duplicate backend")
	}
}

func TestManagerListBackends_HealthCheck(t *testing.T) {
	m := NewManager(testLogger())
	_ = m.RegisterBackend(BackendConfig{
		Name: "nats-health", Protocol: ProtocolNATS, Endpoints: []string{"localhost:4222"},
	})

	statuses := m.ListBackends()
	if len(statuses) != 1 {
		t.Fatalf("expected 1 backend, got %d", len(statuses))
	}

	// Not connected, so unhealthy
	if statuses[0].Status != "unhealthy" {
		t.Errorf("expected unhealthy for disconnected backend, got %s", statuses[0].Status)
	}
	if statuses[0].Error == "" {
		t.Error("expected non-empty error for unhealthy backend")
	}
}

func TestManagerListBackends_Healthy(t *testing.T) {
	m := NewManager(testLogger())
	_ = m.RegisterBackend(BackendConfig{
		Name: "nats-ok", Protocol: ProtocolNATS, Endpoints: []string{"localhost:4222"},
	})
	_ = m.Connect(context.Background())

	statuses := m.ListBackends()
	if len(statuses) != 1 {
		t.Fatalf("expected 1 backend, got %d", len(statuses))
	}
	if statuses[0].Status != "healthy" {
		t.Errorf("expected healthy for connected backend, got %s", statuses[0].Status)
	}
}
