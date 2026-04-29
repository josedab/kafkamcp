package gateway

import (
	"context"
	"time"
)

// Protocol identifies a streaming protocol backend.
type Protocol string

const (
	ProtocolKafka     Protocol = "kafka"
	ProtocolPulsar    Protocol = "pulsar"
	ProtocolRabbitMQ  Protocol = "rabbitmq"
	ProtocolNATS      Protocol = "nats"
	ProtocolKinesis   Protocol = "kinesis"
	ProtocolEventHubs Protocol = "eventhubs"
)

// Message represents a protocol-agnostic message.
type Message struct {
	ID        string            `json:"id"`
	Topic     string            `json:"topic"`
	Key       string            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Partition int32             `json:"partition,omitempty"`
	Offset    int64             `json:"offset,omitempty"`
	Protocol  Protocol          `json:"protocol"`
}

// ProduceRequest describes a message to be produced.
type ProduceRequest struct {
	Topic     string            `json:"topic"`
	Key       string            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Partition *int32            `json:"partition,omitempty"`
}

// ProduceResult is the outcome of a produce operation.
type ProduceResult struct {
	Topic     string   `json:"topic"`
	Partition int32    `json:"partition"`
	Offset    int64    `json:"offset"`
	Protocol  Protocol `json:"protocol"`
}

// ConsumeOptions configures message consumption.
type ConsumeOptions struct {
	Topic     string `json:"topic"`
	Group     string `json:"group,omitempty"`
	Offset    string `json:"offset,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	TimeoutMs int    `json:"timeout_ms,omitempty"`
}

// TopicInfo describes a topic/queue/subject in protocol-agnostic terms.
type TopicInfo struct {
	Name       string            `json:"name"`
	Protocol   Protocol          `json:"protocol"`
	Partitions int               `json:"partitions,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

// BackendConfig holds connection details for a streaming backend.
type BackendConfig struct {
	Protocol         Protocol          `json:"protocol" yaml:"protocol"`
	Name             string            `json:"name" yaml:"name"`
	Endpoints        []string          `json:"endpoints" yaml:"endpoints"`
	TLS              bool              `json:"tls,omitempty" yaml:"tls,omitempty"`
	AuthType         string            `json:"auth_type,omitempty" yaml:"auth_type,omitempty"`
	Username         string            `json:"username,omitempty" yaml:"username,omitempty"`
	Password         string            `json:"-" yaml:"password,omitempty"`
	Token            string            `json:"-" yaml:"token,omitempty"`
	Properties       map[string]string `json:"properties,omitempty" yaml:"properties,omitempty"`
}

// StreamBackend is the interface that each protocol adapter must implement.
type StreamBackend interface {
	// Protocol returns the backend's protocol type.
	Protocol() Protocol

	// Connect initializes the connection to the streaming backend.
	Connect(ctx context.Context) error

	// Close shuts down the backend connection.
	Close() error

	// Produce sends a message to the backend.
	Produce(ctx context.Context, req ProduceRequest) (*ProduceResult, error)

	// Consume reads messages from a topic/queue/subject.
	Consume(ctx context.Context, opts ConsumeOptions) ([]Message, error)

	// ListTopics returns available topics/queues/subjects.
	ListTopics(ctx context.Context) ([]TopicInfo, error)

	// CreateTopic creates a new topic/queue/subject.
	CreateTopic(ctx context.Context, name string, partitions int, config map[string]string) error

	// DeleteTopic removes a topic/queue/subject.
	DeleteTopic(ctx context.Context, name string) error

	// Healthy checks backend connectivity.
	Healthy(ctx context.Context) error
}
