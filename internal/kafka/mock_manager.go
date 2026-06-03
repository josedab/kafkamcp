package kafka

import (
	"context"
	"sync"

	"github.com/josedab/kafkamcp/internal/config"
)

// MockKafkaManager implements the supported KafkaManager surface for tests.
type MockKafkaManager struct {
	mu    sync.Mutex
	Calls []MockCall

	DefaultClusterNameFn    func() string
	ClusterNamesFn          func() []string
	GetClusterConfigFn      func(string) (*config.ClusterConfig, error)
	PingFn                  func(context.Context) error
	CloseFn                 func()
	ListTopicsFn            func(context.Context, string, bool) ([]TopicSummary, error)
	DescribeTopicFn         func(context.Context, string, string) (*TopicDetail, error)
	CreateTopicFn           func(context.Context, string, CreateTopicRequest) error
	AlterTopicFn            func(context.Context, string, AlterTopicRequest) error
	DeleteTopicFn           func(context.Context, string, string) error
	ConsumeFn               func(context.Context, string, string, int32, string, int, int, string) ([]Message, error)
	ProduceFn               func(context.Context, string, string, []ProduceMessage, ProduceOptions) ([]DeliveryResult, error)
	SearchMessagesFn        func(context.Context, string, SearchRequest) ([]SearchMessage, int, error)
	ListConsumerGroupsFn    func(context.Context, string) ([]ConsumerGroupSummary, error)
	DescribeConsumerGroupFn func(context.Context, string, string) (*ConsumerGroupDetail, error)
	ResetOffsetsFn          func(context.Context, string, ResetOffsetsRequest) (*ResetOffsetsResult, error)
	GetClusterMetadataFn    func(context.Context, string) (*ClusterMetadata, error)
	AnalyzeDLQFn            func(context.Context, string, string, int) (*AnalyzeDLQResult, error)
	ListACLsFn              func(context.Context, string, string, string, bool) (*ACLResult, error)
	CreateACLFn             func(context.Context, string, ACLEntry) error
	DeleteACLFn             func(context.Context, string, ACLEntry) (int, error)
}

// MockCall records one method invocation on MockKafkaManager.
type MockCall struct {
	Method string
	Args   []any
}

var _ KafkaManager = (*MockKafkaManager)(nil)

func (m *MockKafkaManager) record(method string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Calls = append(m.Calls, MockCall{Method: method, Args: args})
}

// CallCount returns the number of recorded calls for a method.
func (m *MockKafkaManager) CallCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, call := range m.Calls {
		if call.Method == method {
			count++
		}
	}
	return count
}

// LastCall returns the most recent recorded call for a method.
func (m *MockKafkaManager) LastCall(method string) *MockCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.Calls) - 1; i >= 0; i-- {
		if m.Calls[i].Method == method {
			call := m.Calls[i]
			return &call
		}
	}
	return nil
}

// DefaultClusterName returns the configured mock default cluster.
func (m *MockKafkaManager) DefaultClusterName() string {
	m.record("DefaultClusterName")
	if m.DefaultClusterNameFn != nil {
		return m.DefaultClusterNameFn()
	}
	return "default"
}

// ClusterNames returns the configured mock cluster names.
func (m *MockKafkaManager) ClusterNames() []string {
	m.record("ClusterNames")
	if m.ClusterNamesFn != nil {
		return m.ClusterNamesFn()
	}
	return []string{"default"}
}

// GetClusterConfig returns the configured mock cluster.
func (m *MockKafkaManager) GetClusterConfig(name string) (*config.ClusterConfig, error) {
	m.record("GetClusterConfig", name)
	if m.GetClusterConfigFn != nil {
		return m.GetClusterConfigFn(name)
	}
	return &config.ClusterConfig{Name: "default", BootstrapServers: "localhost:9092"}, nil
}

// Ping executes the configured mock connectivity check.
func (m *MockKafkaManager) Ping(ctx context.Context) error {
	m.record("Ping")
	if m.PingFn != nil {
		return m.PingFn(ctx)
	}
	return nil
}

// Close records closure and invokes the configured close hook.
func (m *MockKafkaManager) Close() {
	m.record("Close")
	if m.CloseFn != nil {
		m.CloseFn()
	}
}

// ListTopics executes the configured topic-listing mock.
func (m *MockKafkaManager) ListTopics(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error) {
	m.record("ListTopics", cluster, includeInternal)
	if m.ListTopicsFn != nil {
		return m.ListTopicsFn(ctx, cluster, includeInternal)
	}
	return nil, nil
}

// DescribeTopic executes the configured topic-description mock.
func (m *MockKafkaManager) DescribeTopic(ctx context.Context, cluster, topic string) (*TopicDetail, error) {
	m.record("DescribeTopic", cluster, topic)
	if m.DescribeTopicFn != nil {
		return m.DescribeTopicFn(ctx, cluster, topic)
	}
	return nil, nil
}

// CreateTopic executes the configured topic-creation mock.
func (m *MockKafkaManager) CreateTopic(ctx context.Context, cluster string, req CreateTopicRequest) error {
	m.record("CreateTopic", cluster, req)
	if m.CreateTopicFn != nil {
		return m.CreateTopicFn(ctx, cluster, req)
	}
	return nil
}

// AlterTopic executes the configured topic-alteration mock.
func (m *MockKafkaManager) AlterTopic(ctx context.Context, cluster string, req AlterTopicRequest) error {
	m.record("AlterTopic", cluster, req)
	if m.AlterTopicFn != nil {
		return m.AlterTopicFn(ctx, cluster, req)
	}
	return nil
}

// DeleteTopic executes the configured topic-deletion mock.
func (m *MockKafkaManager) DeleteTopic(ctx context.Context, cluster, topic string) error {
	m.record("DeleteTopic", cluster, topic)
	if m.DeleteTopicFn != nil {
		return m.DeleteTopicFn(ctx, cluster, topic)
	}
	return nil
}

// Consume executes the configured message-consumption mock.
func (m *MockKafkaManager) Consume(ctx context.Context, cluster, topic string, partition int32, offset string, limit, timeoutMs int, deserializer string) ([]Message, error) {
	m.record("Consume", cluster, topic, partition, offset, limit, timeoutMs, deserializer)
	if m.ConsumeFn != nil {
		return m.ConsumeFn(ctx, cluster, topic, partition, offset, limit, timeoutMs, deserializer)
	}
	return nil, nil
}

// Produce executes the configured message-production mock.
func (m *MockKafkaManager) Produce(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
	m.record("Produce", cluster, topic, messages, opts)
	if m.ProduceFn != nil {
		return m.ProduceFn(ctx, cluster, topic, messages, opts)
	}
	return nil, nil
}

// SearchMessages executes the configured message-search mock.
func (m *MockKafkaManager) SearchMessages(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
	m.record("SearchMessages", cluster, req)
	if m.SearchMessagesFn != nil {
		return m.SearchMessagesFn(ctx, cluster, req)
	}
	return nil, 0, nil
}

// ListConsumerGroups executes the configured group-listing mock.
func (m *MockKafkaManager) ListConsumerGroups(ctx context.Context, cluster string) ([]ConsumerGroupSummary, error) {
	m.record("ListConsumerGroups", cluster)
	if m.ListConsumerGroupsFn != nil {
		return m.ListConsumerGroupsFn(ctx, cluster)
	}
	return nil, nil
}

// DescribeConsumerGroup executes the configured group-description mock.
func (m *MockKafkaManager) DescribeConsumerGroup(ctx context.Context, cluster, group string) (*ConsumerGroupDetail, error) {
	m.record("DescribeConsumerGroup", cluster, group)
	if m.DescribeConsumerGroupFn != nil {
		return m.DescribeConsumerGroupFn(ctx, cluster, group)
	}
	return nil, nil
}

// ResetOffsets executes the configured offset-reset mock.
func (m *MockKafkaManager) ResetOffsets(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error) {
	m.record("ResetOffsets", cluster, req)
	if m.ResetOffsetsFn != nil {
		return m.ResetOffsetsFn(ctx, cluster, req)
	}
	return nil, nil
}

// GetClusterMetadata executes the configured cluster-metadata mock.
func (m *MockKafkaManager) GetClusterMetadata(ctx context.Context, cluster string) (*ClusterMetadata, error) {
	m.record("GetClusterMetadata", cluster)
	if m.GetClusterMetadataFn != nil {
		return m.GetClusterMetadataFn(ctx, cluster)
	}
	return nil, nil
}

// AnalyzeDLQ executes the configured DLQ-analysis mock.
func (m *MockKafkaManager) AnalyzeDLQ(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
	m.record("AnalyzeDLQ", cluster, topic, limit)
	if m.AnalyzeDLQFn != nil {
		return m.AnalyzeDLQFn(ctx, cluster, topic, limit)
	}
	return nil, nil
}

// ListACLs executes the configured ACL-listing mock.
func (m *MockKafkaManager) ListACLs(ctx context.Context, cluster, principal, resourceType string, analysis bool) (*ACLResult, error) {
	m.record("ListACLs", cluster, principal, resourceType, analysis)
	if m.ListACLsFn != nil {
		return m.ListACLsFn(ctx, cluster, principal, resourceType, analysis)
	}
	return nil, nil
}

// CreateACL executes the configured ACL-creation mock.
func (m *MockKafkaManager) CreateACL(ctx context.Context, cluster string, rule ACLEntry) error {
	m.record("CreateACL", cluster, rule)
	if m.CreateACLFn != nil {
		return m.CreateACLFn(ctx, cluster, rule)
	}
	return nil
}

// DeleteACL executes the configured ACL-deletion mock.
func (m *MockKafkaManager) DeleteACL(ctx context.Context, cluster string, rule ACLEntry) (int, error) {
	m.record("DeleteACL", cluster, rule)
	if m.DeleteACLFn != nil {
		return m.DeleteACLFn(ctx, cluster, rule)
	}
	return 0, nil
}
