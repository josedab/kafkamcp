//go:build gateway_pulsar && gateway_rabbitmq && gateway_nats

package gateway

import (
	"context"
	"fmt"
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

// mockBackend implements StreamBackend for testing.
type mockBackend struct {
	protocol    Protocol
	connectErr  error
	closeErr    error
	produceRes  *ProduceResult
	produceErr  error
	consumeRes  []Message
	consumeErr  error
	topicList   []TopicInfo
	topicErr    error
	createErr   error
	healthyErr  error
}

func (m *mockBackend) Protocol() Protocol                         { return m.protocol }
func (m *mockBackend) Connect(ctx context.Context) error          { return m.connectErr }
func (m *mockBackend) Close() error                               { return m.closeErr }
func (m *mockBackend) Healthy(ctx context.Context) error          { return m.healthyErr }
func (m *mockBackend) DeleteTopic(ctx context.Context, name string) error { return nil }
func (m *mockBackend) Produce(ctx context.Context, req ProduceRequest) (*ProduceResult, error) {
	return m.produceRes, m.produceErr
}
func (m *mockBackend) Consume(ctx context.Context, opts ConsumeOptions) ([]Message, error) {
	return m.consumeRes, m.consumeErr
}
func (m *mockBackend) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	return m.topicList, m.topicErr
}
func (m *mockBackend) CreateTopic(ctx context.Context, name string, partitions int, config map[string]string) error {
	return m.createErr
}

func TestConnect(t *testing.T) {
	m := NewManager(slog.Default())
	// Inject mock backend directly
	m.backends["test"] = &mockBackend{protocol: ProtocolNATS}
	m.configs["test"] = &BackendConfig{Name: "test"}

	err := m.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
}

func TestConnectError(t *testing.T) {
	m := NewManager(slog.Default())
	m.backends["test"] = &mockBackend{protocol: ProtocolNATS, connectErr: fmt.Errorf("refused")}
	m.configs["test"] = &BackendConfig{Name: "test"}

	err := m.Connect(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetBackendSuccess(t *testing.T) {
	m := NewManager(slog.Default())
	mb := &mockBackend{protocol: ProtocolPulsar}
	m.backends["my-pulsar"] = mb

	b, err := m.GetBackend("my-pulsar")
	if err != nil {
		t.Fatalf("GetBackend failed: %v", err)
	}
	if b.Protocol() != ProtocolPulsar {
		t.Errorf("expected pulsar, got %s", b.Protocol())
	}
}

func TestGetBackendNotFound(t *testing.T) {
	m := NewManager(slog.Default())
	_, err := m.GetBackend("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent backend")
	}
}

func TestProduceSuccess(t *testing.T) {
	m := NewManager(slog.Default())
	m.backends["test"] = &mockBackend{
		protocol:   ProtocolNATS,
		produceRes: &ProduceResult{Topic: "test-topic", Partition: 0, Offset: 42},
	}

	res, err := m.Produce(context.Background(), "test", ProduceRequest{Topic: "test-topic", Value: []byte("hello")})
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}
	if res.Offset != 42 {
		t.Errorf("expected offset 42, got %d", res.Offset)
	}
}

func TestProduceBackendNotFound(t *testing.T) {
	m := NewManager(slog.Default())
	_, err := m.Produce(context.Background(), "missing", ProduceRequest{})
	if err == nil {
		t.Fatal("expected error for missing backend")
	}
}

func TestProduceBackendError(t *testing.T) {
	m := NewManager(slog.Default())
	m.backends["test"] = &mockBackend{protocol: ProtocolNATS, produceErr: fmt.Errorf("disconnected")}

	_, err := m.Produce(context.Background(), "test", ProduceRequest{})
	if err == nil {
		t.Fatal("expected error from disconnected backend")
	}
}

func TestConsumeSuccess(t *testing.T) {
	m := NewManager(slog.Default())
	m.backends["test"] = &mockBackend{
		protocol: ProtocolNATS,
		consumeRes: []Message{
			{ID: "msg-1", Topic: "test", Value: []byte("hello")},
		},
	}

	msgs, err := m.Consume(context.Background(), "test", ConsumeOptions{Topic: "test"})
	if err != nil {
		t.Fatalf("Consume failed: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected 1 message, got %d", len(msgs))
	}
}

func TestConsumeBackendNotFound(t *testing.T) {
	m := NewManager(slog.Default())
	_, err := m.Consume(context.Background(), "missing", ConsumeOptions{})
	if err == nil {
		t.Fatal("expected error for missing backend")
	}
}

func TestListTopicsSuccess(t *testing.T) {
	m := NewManager(slog.Default())
	m.backends["test"] = &mockBackend{
		protocol: ProtocolNATS,
		topicList: []TopicInfo{
			{Name: "topic1", Protocol: ProtocolNATS},
			{Name: "topic2", Protocol: ProtocolNATS},
		},
	}

	topics, err := m.ListTopics(context.Background(), "test")
	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}
	if len(topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(topics))
	}
}

func TestListTopicsBackendNotFound(t *testing.T) {
	m := NewManager(slog.Default())
	_, err := m.ListTopics(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error for missing backend")
	}
}

func TestCreateTopicSuccess(t *testing.T) {
	m := NewManager(slog.Default())
	m.backends["test"] = &mockBackend{protocol: ProtocolNATS}

	err := m.CreateTopic(context.Background(), "test", "new-topic", 3, nil)
	if err != nil {
		t.Fatalf("CreateTopic failed: %v", err)
	}
}

func TestCreateTopicBackendNotFound(t *testing.T) {
	m := NewManager(slog.Default())
	err := m.CreateTopic(context.Background(), "missing", "topic", 1, nil)
	if err == nil {
		t.Fatal("expected error for missing backend")
	}
}

func TestCloseSuccess(t *testing.T) {
	m := NewManager(slog.Default())
	m.backends["b1"] = &mockBackend{protocol: ProtocolNATS}
	m.backends["b2"] = &mockBackend{protocol: ProtocolPulsar}

	err := m.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestClosePartialFailure(t *testing.T) {
	m := NewManager(slog.Default())
	m.backends["b1"] = &mockBackend{protocol: ProtocolNATS, closeErr: fmt.Errorf("close failed")}
	m.backends["b2"] = &mockBackend{protocol: ProtocolPulsar}

	err := m.Close()
	if err == nil {
		t.Fatal("expected error from partial close failure")
	}
}

func TestCloseEmpty(t *testing.T) {
	m := NewManager(slog.Default())
	err := m.Close()
	if err != nil {
		t.Fatalf("Close on empty manager should not error: %v", err)
	}
}

func TestRegisterBackendEmptyName(t *testing.T) {
	m := NewManager(slog.Default())
	err := m.RegisterBackend(BackendConfig{Protocol: ProtocolNATS})
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestRegisterBackendUnsupportedProtocol(t *testing.T) {
	m := NewManager(slog.Default())
	err := m.RegisterBackend(BackendConfig{Name: "test", Protocol: "redis"})
	if err == nil {
		t.Fatal("expected error for unsupported protocol")
	}
}
