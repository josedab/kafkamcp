package kafkamcpsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Client is a KafkaMCP SDK client.
type Client struct {
	transport Transport
	timeout   time.Duration
}

// Transport defines the interface for communicating with a KafkaMCP server.
type Transport interface {
	CallTool(ctx context.Context, name string, args map[string]any) (json.RawMessage, error)
	ReadResource(ctx context.Context, uri string) (json.RawMessage, error)
	Close() error
}

// Option configures the client.
type Option func(*Client)

// WithTimeout sets the default request timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

// New creates a new KafkaMCP SDK client.
func New(transport Transport, opts ...Option) *Client {
	c := &Client{
		transport: transport,
		timeout:   30 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// TopicSummary contains summarized topic information.
type TopicSummary struct {
	Name              string `json:"name"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	MessageCount      int64  `json:"message_count_estimate"`
}

// Message represents a consumed Kafka message.
type Message struct {
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Timestamp string            `json:"timestamp"`
	Key       any               `json:"key"`
	Value     any               `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// DeliveryResult is the result of producing a message.
type DeliveryResult struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Status    string `json:"status"`
}

// MutationOptions contains the approval and routing controls required by
// guarded write tools. Identity credentials belong to the Transport.
type MutationOptions struct {
	ApprovalToken  string
	IdempotencyKey string
	Cluster        string
}

// ResetOffsetsOptions contains mutation controls and reset-specific inputs.
type ResetOffsetsOptions struct {
	MutationOptions
	Value  string
	DryRun bool
}

func mutationArgs(opts MutationOptions, idempotent bool) (map[string]any, error) {
	if opts.ApprovalToken == "" {
		return nil, fmt.Errorf("approval token is required for guarded mutations")
	}
	args := map[string]any{"approval_token": opts.ApprovalToken}
	if opts.Cluster != "" {
		args["cluster"] = opts.Cluster
	}
	if idempotent && opts.IdempotencyKey != "" {
		args["idempotency_key"] = opts.IdempotencyKey
	}
	return args, nil
}

// ListTopics returns all topics matching the optional filter.
func (c *Client) ListTopics(ctx context.Context, filter string) ([]TopicSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.transport.CallTool(ctx, "kafka_list_topics", map[string]any{
		"filter": filter,
	})
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}

	var resp struct {
		Topics []TopicSummary `json:"topics"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing topics response: %w", err)
	}
	return resp.Topics, nil
}

// Consume reads messages from a topic.
func (c *Client) Consume(ctx context.Context, topic string, limit int, offset string) ([]Message, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	if limit <= 0 {
		limit = 10
	}
	if offset == "" {
		offset = "latest"
	}

	result, err := c.transport.CallTool(ctx, "kafka_consume", map[string]any{
		"topic": topic, "limit": float64(limit), "offset": offset,
	})
	if err != nil {
		return nil, fmt.Errorf("consuming messages: %w", err)
	}

	var resp struct {
		Messages []Message `json:"messages"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing consume response: %w", err)
	}
	return resp.Messages, nil
}

// Produce sends messages to a topic.
func (c *Client) Produce(ctx context.Context, topic string, messages []map[string]any, opts MutationOptions) ([]DeliveryResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	args, err := mutationArgs(opts, false)
	if err != nil {
		return nil, err
	}
	args["topic"] = topic
	args["messages"] = messages
	result, err := c.transport.CallTool(ctx, "kafka_produce", args)
	if err != nil {
		return nil, fmt.Errorf("producing messages: %w", err)
	}

	var resp struct {
		Deliveries []DeliveryResult `json:"deliveries"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing produce response: %w", err)
	}
	return resp.Deliveries, nil
}

// Search searches messages in a topic.
func (c *Client) Search(ctx context.Context, topic, valueContains string, limit int) ([]Message, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.transport.CallTool(ctx, "kafka_search", map[string]any{
		"topic": topic, "value_contains": valueContains, "limit": float64(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("searching messages: %w", err)
	}

	var resp struct {
		Messages []Message `json:"messages"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing search response: %w", err)
	}
	return resp.Messages, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	return c.transport.Close()
}

// --- Consumer Group Operations ---

// ConsumerGroupSummary contains summarized consumer group information.
type ConsumerGroupSummary struct {
	GroupID     string `json:"group_id"`
	State       string `json:"state"`
	MemberCount int    `json:"member_count"`
	TotalLag    int64  `json:"total_lag"`
}

// ListConsumerGroups returns all consumer groups.
func (c *Client) ListConsumerGroups(ctx context.Context, cluster string) ([]ConsumerGroupSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	args := map[string]any{}
	if cluster != "" {
		args["cluster"] = cluster
	}

	result, err := c.transport.CallTool(ctx, "kafka_list_consumer_groups", args)
	if err != nil {
		return nil, fmt.Errorf("listing consumer groups: %w", err)
	}

	var resp struct {
		Groups []ConsumerGroupSummary `json:"groups"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing groups response: %w", err)
	}
	return resp.Groups, nil
}

// DescribeConsumerGroup returns detailed info about a consumer group.
func (c *Client) DescribeConsumerGroup(ctx context.Context, groupID string) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.transport.CallTool(ctx, "kafka_describe_consumer_group", map[string]any{
		"group_id": groupID,
	})
	if err != nil {
		return nil, fmt.Errorf("describing consumer group: %w", err)
	}
	return result, nil
}

// --- Topic Management ---

// DescribeTopic returns detailed information about a topic.
func (c *Client) DescribeTopic(ctx context.Context, topic string) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.transport.CallTool(ctx, "kafka_describe_topic", map[string]any{
		"topic": topic,
	})
	if err != nil {
		return nil, fmt.Errorf("describing topic: %w", err)
	}
	return result, nil
}

// CreateTopic creates a new Kafka topic.
func (c *Client) CreateTopic(ctx context.Context, topic string, partitions int, replicationFactor int, configs map[string]string, opts MutationOptions) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	args, err := mutationArgs(opts, true)
	if err != nil {
		return err
	}
	args["name"] = topic
	args["partitions"] = float64(partitions)
	args["replication_factor"] = float64(replicationFactor)
	if len(configs) > 0 {
		args["configs"] = configs
	}

	_, err = c.transport.CallTool(ctx, "kafka_create_topic", args)
	if err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}
	return nil
}

// DeleteTopic deletes a Kafka topic.
func (c *Client) DeleteTopic(ctx context.Context, topic string, opts MutationOptions) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	args, err := mutationArgs(opts, true)
	if err != nil {
		return err
	}
	args["name"] = topic
	_, err = c.transport.CallTool(ctx, "kafka_delete_topic", args)
	if err != nil {
		return fmt.Errorf("deleting topic: %w", err)
	}
	return nil
}

// --- Schema Registry ---

// SchemaSubject contains summarized Schema Registry subject information.
type SchemaSubject struct {
	Subject            string `json:"subject_name"`
	Version            int    `json:"latest_version"`
	SchemaType         string `json:"schema_type"`
	CompatibilityLevel string `json:"compatibility_level,omitempty"`
}

// SchemaInfo contains a full schema definition.
type SchemaInfo struct {
	Subject            string `json:"subject"`
	Version            int    `json:"version"`
	SchemaID           int    `json:"schema_id"`
	SchemaType         string `json:"schema_type"`
	CompatibilityLevel string `json:"compatibility_level,omitempty"`
	Schema             any    `json:"schema"`
	FieldSummary       string `json:"field_summary"`
	References         []any  `json:"references"`
}

// ListSchemas returns all schema subjects.
func (c *Client) ListSchemas(ctx context.Context) ([]SchemaSubject, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.transport.CallTool(ctx, "kafka_list_schemas", map[string]any{})
	if err != nil {
		return nil, fmt.Errorf("listing schemas: %w", err)
	}

	var resp struct {
		Subjects []SchemaSubject `json:"subjects"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing schemas response: %w", err)
	}
	return resp.Subjects, nil
}

// GetSchema returns a schema by subject.
func (c *Client) GetSchema(ctx context.Context, subject string) (*SchemaInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.transport.CallTool(ctx, "kafka_get_schema", map[string]any{
		"subject": subject,
	})
	if err != nil {
		return nil, fmt.Errorf("getting schema: %w", err)
	}

	var info SchemaInfo
	if err := json.Unmarshal(result, &info); err != nil {
		return nil, fmt.Errorf("parsing schema response: %w", err)
	}
	return &info, nil
}

// --- Admin Operations ---

// ResetOffsets resets consumer group offsets.
func (c *Client) ResetOffsets(ctx context.Context, groupID, topic, strategy string, opts ResetOffsetsOptions) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	if strategy == "timestamp" && opts.Value == "" {
		return fmt.Errorf("timestamp value is required for timestamp offset resets")
	}

	args, err := mutationArgs(opts.MutationOptions, false)
	if err != nil {
		return err
	}
	args["group_id"] = groupID
	args["topic"] = topic
	args["strategy"] = strategy
	if opts.Value != "" {
		args["value"] = opts.Value
	}
	if opts.DryRun {
		args["dry_run"] = true
	}
	_, err = c.transport.CallTool(ctx, "kafka_reset_offsets", args)
	if err != nil {
		return fmt.Errorf("resetting offsets: %w", err)
	}
	return nil
}

// DiagnoseLag runs lag diagnosis for a consumer group.
func (c *Client) DiagnoseLag(ctx context.Context, groupID string) (json.RawMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.transport.CallTool(ctx, "kafka_diagnose_lag", map[string]any{
		"group_id": groupID,
	})
	if err != nil {
		return nil, fmt.Errorf("diagnosing lag: %w", err)
	}
	return result, nil
}
