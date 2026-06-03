package kafka

import (
	"context"

	"github.com/josedab/kafkamcp/internal/config"
)

// ClusterManager defines cluster-level operations.
type ClusterManager interface {
	DefaultClusterName() string
	ClusterNames() []string
	GetClusterConfig(clusterName string) (*config.ClusterConfig, error)
	Ping(ctx context.Context) error
	Close()
}

// TopicManager defines topic CRUD and metadata operations.
type TopicManager interface {
	ListTopics(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error)
	DescribeTopic(ctx context.Context, cluster, topicName string) (*TopicDetail, error)
	CreateTopic(ctx context.Context, cluster string, req CreateTopicRequest) error
	AlterTopic(ctx context.Context, cluster string, req AlterTopicRequest) error
	DeleteTopic(ctx context.Context, cluster, topicName string) error
}

// GroupManager defines consumer group operations.
type GroupManager interface {
	ListConsumerGroups(ctx context.Context, cluster string) ([]ConsumerGroupSummary, error)
	DescribeConsumerGroup(ctx context.Context, cluster, groupID string) (*ConsumerGroupDetail, error)
	ResetOffsets(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error)
}

// KafkaManager defines the interface for Kafka operations used by server handlers.
// This enables mock-based testing of handler logic without a live Kafka cluster.
// It intentionally contains only operations reachable through the supported
// 29-tool / 9-resource product surface.
type KafkaManager interface {
	ClusterManager
	TopicManager
	GroupManager

	// Message operations
	Consume(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error)
	Produce(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error)
	SearchMessages(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error)
	AnalyzeDLQ(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error)

	// Cluster metadata
	GetClusterMetadata(ctx context.Context, cluster string) (*ClusterMetadata, error)

	// ACL operations
	ListACLs(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error)
	CreateACL(ctx context.Context, cluster string, rule ACLEntry) error
	DeleteACL(ctx context.Context, cluster string, rule ACLEntry) (int, error)
}

// Verify Manager implements KafkaManager at compile time.
var _ KafkaManager = (*Manager)(nil)
