package kafka

import (
	"context"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

// KafkaManager defines the interface for Kafka operations used by server handlers.
// This enables mock-based testing of handler logic without a live Kafka cluster.
type KafkaManager interface {
	// Cluster management
	DefaultClusterName() string
	ClusterNames() []string
	GetClusterConfig(clusterName string) (*config.ClusterConfig, error)
	Ping(ctx context.Context) error
	Close()

	// Topic operations
	ListTopics(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error)
	DescribeTopic(ctx context.Context, cluster, topicName string) (*TopicDetail, error)
	CreateTopic(ctx context.Context, cluster string, req CreateTopicRequest) error
	AlterTopic(ctx context.Context, cluster string, req AlterTopicRequest) error
	DeleteTopic(ctx context.Context, cluster, topicName string) error

	// Message operations
	Consume(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error)
	Produce(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error)
	Subscribe(ctx context.Context, cluster string, req SubscribeRequest) (*SubscribeResult, error)
	SearchMessages(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error)

	// Consumer group operations
	ListConsumerGroups(ctx context.Context, cluster string) ([]ConsumerGroupSummary, error)
	DescribeConsumerGroup(ctx context.Context, cluster, groupID string) (*ConsumerGroupDetail, error)
	ResetOffsets(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error)

	// Cluster metadata
	GetClusterMetadata(ctx context.Context, cluster string) (*ClusterMetadata, error)

	// Advanced operations
	Aggregate(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error)
	GetTopicGraph(ctx context.Context, cluster string) (*TopicGraph, error)
	JoinTopics(ctx context.Context, cluster string, req JoinRequest) (*JoinResult, error)
	NaturalLanguageQuery(ctx context.Context, cluster, query string, topics []TopicSummary) (*NaturalLanguageQueryResult, error)
	AnalyzeDLQ(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error)
}

// Verify Manager implements KafkaManager at compile time.
var _ KafkaManager = (*Manager)(nil)
