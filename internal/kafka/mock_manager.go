package kafka

import (
	"context"
	"sync"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

// MockKafkaManager implements KafkaManager for testing.
// It supports configurable return values per method and records all calls for assertion.
type MockKafkaManager struct {
	mu    sync.Mutex
	Calls []MockCall

	// Configurable return values
	DefaultClusterNameFn     func() string
	ClusterNamesFn           func() []string
	GetClusterConfigFn       func(string) (*config.ClusterConfig, error)
	PingFn                   func(context.Context) error
	CloseFn                  func()
	ListTopicsFn             func(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error)
	DescribeTopicFn          func(ctx context.Context, cluster, topicName string) (*TopicDetail, error)
	CreateTopicFn            func(ctx context.Context, cluster string, req CreateTopicRequest) error
	AlterTopicFn             func(ctx context.Context, cluster string, req AlterTopicRequest) error
	DeleteTopicFn            func(ctx context.Context, cluster, topicName string) error
	ConsumeFn                func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error)
	ProduceFn                func(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error)
	SubscribeFn              func(ctx context.Context, cluster string, req SubscribeRequest) (*SubscribeResult, error)
	SearchMessagesFn         func(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error)
	ListConsumerGroupsFn     func(ctx context.Context, cluster string) ([]ConsumerGroupSummary, error)
	DescribeConsumerGroupFn  func(ctx context.Context, cluster, groupID string) (*ConsumerGroupDetail, error)
	ResetOffsetsFn           func(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error)
	GetClusterMetadataFn     func(ctx context.Context, cluster string) (*ClusterMetadata, error)
	AggregateFn              func(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error)
	GetTopicGraphFn          func(ctx context.Context, cluster string) (*TopicGraph, error)
	JoinTopicsFn             func(ctx context.Context, cluster string, req JoinRequest) (*JoinResult, error)
	NaturalLanguageQueryFn   func(ctx context.Context, cluster, query string, topics []TopicSummary) (*NaturalLanguageQueryResult, error)
	AnalyzeDLQFn             func(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error)
	StartStreamFn            func(ctx context.Context, cluster string, cfg StreamConfig) (*StreamResult, error)
	PollStreamFn             func(sessionID string, timeoutMs int) (*StreamResult, error)
	ControlStreamFn          func(sessionID, action string) (*StreamResult, error)
	ListStreamsFn            func() []StreamSession
	TimeTravelSnapshotFn     func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error)
	TimeTravelDiffFn         func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error)
	GetRecommendationsFn     func(ctx context.Context, cluster, topic string) (*RecommendationResult, error)
	TraceMessageFn           func(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error)
	ListACLsFn               func(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error)
	CompareConfigsFn         func(ctx context.Context, sourceCluster, targetCluster, topic string) (*ConfigCompareResult, error)
	AuditConfigsFn           func(ctx context.Context, cluster, topic string, template map[string]string) (*ConfigAuditResult, error)
	PlaygroundCreateFn       func(ctx context.Context, cluster, agentID, name string, partitions int32) (*PlaygroundTopicResult, error)
	PlaygroundProduceSampleFn func(ctx context.Context, cluster, agentID, name, pattern string, count int) (map[string]any, error)
	PlaygroundListFn         func(ctx context.Context, cluster, agentID string) ([]TopicSummary, error)
	PlaygroundCleanupFn      func(ctx context.Context, cluster, agentID string) (int, error)
}

// MockCall records a single method invocation.
type MockCall struct {
	Method string
	Args   []any
}

// Verify MockKafkaManager implements KafkaManager at compile time.
var _ KafkaManager = (*MockKafkaManager)(nil)

func (m *MockKafkaManager) record(method string, args ...any) {
	m.mu.Lock()
	m.Calls = append(m.Calls, MockCall{Method: method, Args: args})
	m.mu.Unlock()
}

// CallCount returns the number of times the given method was called.
func (m *MockKafkaManager) CallCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, c := range m.Calls {
		if c.Method == method {
			count++
		}
	}
	return count
}

// LastCall returns the last call record for the given method, or nil.
func (m *MockKafkaManager) LastCall(method string) *MockCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.Calls) - 1; i >= 0; i-- {
		if m.Calls[i].Method == method {
			return &m.Calls[i]
		}
	}
	return nil
}

func (m *MockKafkaManager) DefaultClusterName() string {
	m.record("DefaultClusterName")
	if m.DefaultClusterNameFn != nil {
		return m.DefaultClusterNameFn()
	}
	return "default"
}

func (m *MockKafkaManager) ClusterNames() []string {
	m.record("ClusterNames")
	if m.ClusterNamesFn != nil {
		return m.ClusterNamesFn()
	}
	return []string{"default"}
}

func (m *MockKafkaManager) GetClusterConfig(clusterName string) (*config.ClusterConfig, error) {
	m.record("GetClusterConfig", clusterName)
	if m.GetClusterConfigFn != nil {
		return m.GetClusterConfigFn(clusterName)
	}
	return &config.ClusterConfig{Name: "default", BootstrapServers: "localhost:9092"}, nil
}

func (m *MockKafkaManager) Ping(ctx context.Context) error {
	m.record("Ping")
	if m.PingFn != nil {
		return m.PingFn(ctx)
	}
	return nil
}

func (m *MockKafkaManager) Close() {
	m.record("Close")
	if m.CloseFn != nil {
		m.CloseFn()
	}
}

func (m *MockKafkaManager) ListTopics(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error) {
	m.record("ListTopics", cluster, includeInternal)
	if m.ListTopicsFn != nil {
		return m.ListTopicsFn(ctx, cluster, includeInternal)
	}
	return nil, nil
}

func (m *MockKafkaManager) DescribeTopic(ctx context.Context, cluster, topicName string) (*TopicDetail, error) {
	m.record("DescribeTopic", cluster, topicName)
	if m.DescribeTopicFn != nil {
		return m.DescribeTopicFn(ctx, cluster, topicName)
	}
	return nil, nil
}

func (m *MockKafkaManager) CreateTopic(ctx context.Context, cluster string, req CreateTopicRequest) error {
	m.record("CreateTopic", cluster, req)
	if m.CreateTopicFn != nil {
		return m.CreateTopicFn(ctx, cluster, req)
	}
	return nil
}

func (m *MockKafkaManager) AlterTopic(ctx context.Context, cluster string, req AlterTopicRequest) error {
	m.record("AlterTopic", cluster, req)
	if m.AlterTopicFn != nil {
		return m.AlterTopicFn(ctx, cluster, req)
	}
	return nil
}

func (m *MockKafkaManager) DeleteTopic(ctx context.Context, cluster, topicName string) error {
	m.record("DeleteTopic", cluster, topicName)
	if m.DeleteTopicFn != nil {
		return m.DeleteTopicFn(ctx, cluster, topicName)
	}
	return nil
}

func (m *MockKafkaManager) Consume(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
	m.record("Consume", cluster, topic, partition, offset, limit, timeoutMs, deserializer)
	if m.ConsumeFn != nil {
		return m.ConsumeFn(ctx, cluster, topic, partition, offset, limit, timeoutMs, deserializer)
	}
	return nil, nil
}

func (m *MockKafkaManager) Produce(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
	m.record("Produce", cluster, topic, messages, opts)
	if m.ProduceFn != nil {
		return m.ProduceFn(ctx, cluster, topic, messages, opts)
	}
	return nil, nil
}

func (m *MockKafkaManager) Subscribe(ctx context.Context, cluster string, req SubscribeRequest) (*SubscribeResult, error) {
	m.record("Subscribe", cluster, req)
	if m.SubscribeFn != nil {
		return m.SubscribeFn(ctx, cluster, req)
	}
	return nil, nil
}

func (m *MockKafkaManager) SearchMessages(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
	m.record("SearchMessages", cluster, req)
	if m.SearchMessagesFn != nil {
		return m.SearchMessagesFn(ctx, cluster, req)
	}
	return nil, 0, nil
}

func (m *MockKafkaManager) ListConsumerGroups(ctx context.Context, cluster string) ([]ConsumerGroupSummary, error) {
	m.record("ListConsumerGroups", cluster)
	if m.ListConsumerGroupsFn != nil {
		return m.ListConsumerGroupsFn(ctx, cluster)
	}
	return nil, nil
}

func (m *MockKafkaManager) DescribeConsumerGroup(ctx context.Context, cluster, groupID string) (*ConsumerGroupDetail, error) {
	m.record("DescribeConsumerGroup", cluster, groupID)
	if m.DescribeConsumerGroupFn != nil {
		return m.DescribeConsumerGroupFn(ctx, cluster, groupID)
	}
	return nil, nil
}

func (m *MockKafkaManager) ResetOffsets(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error) {
	m.record("ResetOffsets", cluster, req)
	if m.ResetOffsetsFn != nil {
		return m.ResetOffsetsFn(ctx, cluster, req)
	}
	return nil, nil
}

func (m *MockKafkaManager) GetClusterMetadata(ctx context.Context, cluster string) (*ClusterMetadata, error) {
	m.record("GetClusterMetadata", cluster)
	if m.GetClusterMetadataFn != nil {
		return m.GetClusterMetadataFn(ctx, cluster)
	}
	return nil, nil
}

func (m *MockKafkaManager) Aggregate(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
	m.record("Aggregate", cluster, req)
	if m.AggregateFn != nil {
		return m.AggregateFn(ctx, cluster, req)
	}
	return nil, nil
}

func (m *MockKafkaManager) GetTopicGraph(ctx context.Context, cluster string) (*TopicGraph, error) {
	m.record("GetTopicGraph", cluster)
	if m.GetTopicGraphFn != nil {
		return m.GetTopicGraphFn(ctx, cluster)
	}
	return nil, nil
}

func (m *MockKafkaManager) JoinTopics(ctx context.Context, cluster string, req JoinRequest) (*JoinResult, error) {
	m.record("JoinTopics", cluster, req)
	if m.JoinTopicsFn != nil {
		return m.JoinTopicsFn(ctx, cluster, req)
	}
	return nil, nil
}

func (m *MockKafkaManager) NaturalLanguageQuery(ctx context.Context, cluster, query string, topics []TopicSummary) (*NaturalLanguageQueryResult, error) {
	m.record("NaturalLanguageQuery", cluster, query, topics)
	if m.NaturalLanguageQueryFn != nil {
		return m.NaturalLanguageQueryFn(ctx, cluster, query, topics)
	}
	return nil, nil
}

func (m *MockKafkaManager) AnalyzeDLQ(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
	m.record("AnalyzeDLQ", cluster, topic, limit)
	if m.AnalyzeDLQFn != nil {
		return m.AnalyzeDLQFn(ctx, cluster, topic, limit)
	}
	return nil, nil
}

func (m *MockKafkaManager) StartStream(ctx context.Context, cluster string, cfg StreamConfig) (*StreamResult, error) {
	m.record("StartStream", cluster, cfg)
	if m.StartStreamFn != nil {
		return m.StartStreamFn(ctx, cluster, cfg)
	}
	return nil, nil
}

func (m *MockKafkaManager) PollStream(sessionID string, timeoutMs int) (*StreamResult, error) {
	m.record("PollStream", sessionID, timeoutMs)
	if m.PollStreamFn != nil {
		return m.PollStreamFn(sessionID, timeoutMs)
	}
	return nil, nil
}

func (m *MockKafkaManager) ControlStream(sessionID, action string) (*StreamResult, error) {
	m.record("ControlStream", sessionID, action)
	if m.ControlStreamFn != nil {
		return m.ControlStreamFn(sessionID, action)
	}
	return nil, nil
}

func (m *MockKafkaManager) ListStreams() []StreamSession {
	m.record("ListStreams")
	if m.ListStreamsFn != nil {
		return m.ListStreamsFn()
	}
	return nil
}

func (m *MockKafkaManager) TimeTravelSnapshot(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
	m.record("TimeTravelSnapshot", cluster, topic, timestamp, maxKeys)
	if m.TimeTravelSnapshotFn != nil {
		return m.TimeTravelSnapshotFn(ctx, cluster, topic, timestamp, maxKeys)
	}
	return nil, nil
}

func (m *MockKafkaManager) TimeTravelDiff(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
	m.record("TimeTravelDiff", cluster, topic, tsFrom, tsTo, maxKeys)
	if m.TimeTravelDiffFn != nil {
		return m.TimeTravelDiffFn(ctx, cluster, topic, tsFrom, tsTo, maxKeys)
	}
	return nil, nil
}

func (m *MockKafkaManager) GetRecommendations(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
	m.record("GetRecommendations", cluster, topic)
	if m.GetRecommendationsFn != nil {
		return m.GetRecommendationsFn(ctx, cluster, topic)
	}
	return nil, nil
}

func (m *MockKafkaManager) TraceMessage(ctx context.Context, cluster, correlationID, headerName string, topics []string, maxScan int) (*TraceResult, error) {
	m.record("TraceMessage", cluster, correlationID, headerName, topics, maxScan)
	if m.TraceMessageFn != nil {
		return m.TraceMessageFn(ctx, cluster, correlationID, headerName, topics, maxScan)
	}
	return nil, nil
}

func (m *MockKafkaManager) ListACLs(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
	m.record("ListACLs", cluster, principal, resourceType, includeAnalysis)
	if m.ListACLsFn != nil {
		return m.ListACLsFn(ctx, cluster, principal, resourceType, includeAnalysis)
	}
	return nil, nil
}

func (m *MockKafkaManager) CompareConfigs(ctx context.Context, sourceCluster, targetCluster, topic string) (*ConfigCompareResult, error) {
	m.record("CompareConfigs", sourceCluster, targetCluster, topic)
	if m.CompareConfigsFn != nil {
		return m.CompareConfigsFn(ctx, sourceCluster, targetCluster, topic)
	}
	return nil, nil
}

func (m *MockKafkaManager) AuditConfigs(ctx context.Context, cluster, topic string, template map[string]string) (*ConfigAuditResult, error) {
	m.record("AuditConfigs", cluster, topic, template)
	if m.AuditConfigsFn != nil {
		return m.AuditConfigsFn(ctx, cluster, topic, template)
	}
	return nil, nil
}

func (m *MockKafkaManager) PlaygroundCreate(ctx context.Context, cluster, agentID, name string, partitions int32) (*PlaygroundTopicResult, error) {
	m.record("PlaygroundCreate", cluster, agentID, name, partitions)
	if m.PlaygroundCreateFn != nil {
		return m.PlaygroundCreateFn(ctx, cluster, agentID, name, partitions)
	}
	return nil, nil
}

func (m *MockKafkaManager) PlaygroundProduceSample(ctx context.Context, cluster, agentID, name, pattern string, count int) (map[string]any, error) {
	m.record("PlaygroundProduceSample", cluster, agentID, name, pattern, count)
	if m.PlaygroundProduceSampleFn != nil {
		return m.PlaygroundProduceSampleFn(ctx, cluster, agentID, name, pattern, count)
	}
	return nil, nil
}

func (m *MockKafkaManager) PlaygroundList(ctx context.Context, cluster, agentID string) ([]TopicSummary, error) {
	m.record("PlaygroundList", cluster, agentID)
	if m.PlaygroundListFn != nil {
		return m.PlaygroundListFn(ctx, cluster, agentID)
	}
	return nil, nil
}

func (m *MockKafkaManager) PlaygroundCleanup(ctx context.Context, cluster, agentID string) (int, error) {
	m.record("PlaygroundCleanup", cluster, agentID)
	if m.PlaygroundCleanupFn != nil {
		return m.PlaygroundCleanupFn(ctx, cluster, agentID)
	}
	return 0, nil
}
