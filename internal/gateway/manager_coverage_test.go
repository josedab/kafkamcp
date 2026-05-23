package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// mockBackendCov implements StreamBackend for coverage tests.
type mockBackendCov struct {
	protocol   Protocol
	connectErr error
	closeErr   error
	produceRes *ProduceResult
	produceErr error
	consumeRes []Message
	consumeErr error
	topicList  []TopicInfo
	topicErr   error
	createErr  error
	healthyErr error
}

func (m *mockBackendCov) Protocol() Protocol                                 { return m.protocol }
func (m *mockBackendCov) Connect(ctx context.Context) error                  { return m.connectErr }
func (m *mockBackendCov) Close() error                                       { return m.closeErr }
func (m *mockBackendCov) Healthy(ctx context.Context) error                  { return m.healthyErr }
func (m *mockBackendCov) DeleteTopic(ctx context.Context, name string) error { return nil }
func (m *mockBackendCov) Produce(ctx context.Context, req ProduceRequest) (*ProduceResult, error) {
	return m.produceRes, m.produceErr
}
func (m *mockBackendCov) Consume(ctx context.Context, opts ConsumeOptions) ([]Message, error) {
	return m.consumeRes, m.consumeErr
}
func (m *mockBackendCov) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	return m.topicList, m.topicErr
}
func (m *mockBackendCov) CreateTopic(ctx context.Context, name string, partitions int, config map[string]string) error {
	return m.createErr
}

func TestCategorizeBackendError_Timeout(t *testing.T) {
	t.Parallel()
	got := categorizeBackendError(fmt.Errorf("connection timeout after 5s"))
	if got != "health_check_timeout" {
		t.Errorf("expected health_check_timeout, got %q", got)
	}
}

func TestCategorizeBackendError_Deadline(t *testing.T) {
	t.Parallel()
	got := categorizeBackendError(fmt.Errorf("context deadline exceeded"))
	if got != "health_check_timeout" {
		t.Errorf("expected health_check_timeout, got %q", got)
	}
}

func TestCategorizeBackendError_ConnectionRefused(t *testing.T) {
	t.Parallel()
	got := categorizeBackendError(fmt.Errorf("connection refused"))
	if got != "connection_failed" {
		t.Errorf("expected connection_failed, got %q", got)
	}
}

func TestCategorizeBackendError_Auth(t *testing.T) {
	t.Parallel()
	got := categorizeBackendError(fmt.Errorf("authentication failed"))
	if got != "authentication_failed" {
		t.Errorf("expected authentication_failed, got %q", got)
	}
}

func TestCategorizeBackendError_Permission(t *testing.T) {
	t.Parallel()
	got := categorizeBackendError(fmt.Errorf("permission denied"))
	if got != "authentication_failed" {
		t.Errorf("expected authentication_failed, got %q", got)
	}
}

func TestCategorizeBackendError_TLS(t *testing.T) {
	t.Parallel()
	got := categorizeBackendError(fmt.Errorf("tls handshake error"))
	if got != "tls_error" {
		t.Errorf("expected tls_error, got %q", got)
	}
}

func TestCategorizeBackendError_Certificate(t *testing.T) {
	t.Parallel()
	got := categorizeBackendError(fmt.Errorf("certificate expired"))
	if got != "tls_error" {
		t.Errorf("expected tls_error, got %q", got)
	}
}

func TestCategorizeBackendError_Unknown(t *testing.T) {
	t.Parallel()
	got := categorizeBackendError(fmt.Errorf("something weird happened"))
	if got != "health_check_failed" {
		t.Errorf("expected health_check_failed, got %q", got)
	}
}

func TestConnectEmptyRegistry(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	err := m.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect on empty manager should succeed: %v", err)
	}
}

func TestListBackends_HealthyAndUnhealthy(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	m.backends["healthy"] = &mockBackendCov{protocol: "test"}
	m.configs["healthy"] = &BackendConfig{Name: "healthy", Endpoints: []string{"a:1", "b:2"}}
	m.backends["unhealthy"] = &mockBackendCov{protocol: "test", healthyErr: fmt.Errorf("timeout reached")}
	m.configs["unhealthy"] = &BackendConfig{Name: "unhealthy", Endpoints: []string{"c:3"}}

	statuses := m.ListBackends()
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}

	statusMap := make(map[string]BackendStatus)
	for _, s := range statuses {
		statusMap[s.Name] = s
	}

	if statusMap["healthy"].Status != "healthy" {
		t.Errorf("expected healthy status, got %q", statusMap["healthy"].Status)
	}
	if statusMap["healthy"].EndpointCount != 2 {
		t.Errorf("expected 2 endpoints, got %d", statusMap["healthy"].EndpointCount)
	}
	if statusMap["unhealthy"].Status != "unhealthy" {
		t.Errorf("expected unhealthy status, got %q", statusMap["unhealthy"].Status)
	}
	if statusMap["unhealthy"].Error == "" {
		t.Error("expected error string for unhealthy backend")
	}
}

func TestRegisterBackend_ValidConfig_NoFactory(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	err := m.RegisterBackend(BackendConfig{Name: "x", Protocol: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for unsupported protocol")
	}
	if !strings.Contains(err.Error(), "unsupported") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestProduceRoutesToCorrectBackend(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	m.backends["alpha"] = &mockBackendCov{
		protocol:   "alpha",
		produceRes: &ProduceResult{Topic: "t1", Partition: 0, Offset: 10},
	}
	m.backends["beta"] = &mockBackendCov{
		protocol:   "beta",
		produceRes: &ProduceResult{Topic: "t2", Partition: 1, Offset: 20},
	}

	res, err := m.Produce(context.Background(), "beta", ProduceRequest{Topic: "t2"})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if res.Offset != 20 {
		t.Errorf("expected offset 20 from beta, got %d", res.Offset)
	}
}

func TestConsumeRoutesToCorrectBackend(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	m.backends["alpha"] = &mockBackendCov{
		protocol:   "alpha",
		consumeRes: []Message{{ID: "a1"}},
	}
	m.backends["beta"] = &mockBackendCov{
		protocol:   "beta",
		consumeRes: []Message{{ID: "b1"}, {ID: "b2"}},
	}

	msgs, err := m.Consume(context.Background(), "beta", ConsumeOptions{Topic: "t"})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if len(msgs) != 2 {
		t.Errorf("expected 2 msgs from beta, got %d", len(msgs))
	}
}

func TestListTopicsRoutesToCorrectBackend(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	m.backends["alpha"] = &mockBackendCov{
		protocol:  "alpha",
		topicList: []TopicInfo{{Name: "alpha-topic"}},
	}
	m.backends["beta"] = &mockBackendCov{
		protocol:  "beta",
		topicList: []TopicInfo{{Name: "beta-topic1"}, {Name: "beta-topic2"}},
	}

	topics, err := m.ListTopics(context.Background(), "alpha")
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	if len(topics) != 1 || topics[0].Name != "alpha-topic" {
		t.Errorf("unexpected topics from alpha: %+v", topics)
	}
}

func TestCreateTopicRoutesToCorrectBackend(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	m.backends["alpha"] = &mockBackendCov{protocol: "alpha", createErr: fmt.Errorf("fail")}
	m.backends["beta"] = &mockBackendCov{protocol: "beta"}

	if err := m.CreateTopic(context.Background(), "beta", "new-topic", 3, nil); err != nil {
		t.Errorf("expected success on beta: %v", err)
	}
	if err := m.CreateTopic(context.Background(), "alpha", "new-topic", 3, nil); err == nil {
		t.Error("expected error from alpha")
	}
}

func TestClosePartialFailure_ReturnsFirstError(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	m.backends["b1"] = &mockBackendCov{closeErr: fmt.Errorf("b1 close error")}
	m.backends["b2"] = &mockBackendCov{closeErr: fmt.Errorf("b2 close error")}

	err := m.Close()
	if err == nil {
		t.Fatal("expected error from close")
	}
}

func TestConcurrentProduce(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	m.backends["a"] = &mockBackendCov{
		protocol:   "a",
		produceRes: &ProduceResult{Topic: "t", Offset: 1},
	}
	m.backends["b"] = &mockBackendCov{
		protocol:   "b",
		produceRes: &ProduceResult{Topic: "t", Offset: 2},
	}

	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, err := m.Produce(context.Background(), "a", ProduceRequest{Topic: "t"})
			if err != nil {
				errs <- err
			}
		}()
		go func() {
			defer wg.Done()
			_, err := m.Produce(context.Background(), "b", ProduceRequest{Topic: "t"})
			if err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent produce error: %v", err)
	}
}

func TestGetBackendDisconnected(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	m.backends["disc"] = &mockBackendCov{
		protocol:   "test",
		healthyErr: fmt.Errorf("connection refused"),
	}

	b, err := m.GetBackend("disc")
	if err != nil {
		t.Fatalf("GetBackend should succeed even for unhealthy backend: %v", err)
	}
	if b.Protocol() != "test" {
		t.Errorf("unexpected protocol: %s", b.Protocol())
	}
}
