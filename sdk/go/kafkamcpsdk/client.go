// Package kafkamcpsdk provides a Go client SDK for interacting with KafkaMCP
// servers programmatically from Go applications and AI agents.
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
	agentID   string
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

// WithAgentID sets the agent identity for authorization.
func WithAgentID(id string) Option {
	return func(c *Client) { c.agentID = id }
}

// WithTimeout sets the default request timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

// New creates a new KafkaMCP SDK client.
func New(transport Transport, opts ...Option) *Client {
	c := &Client{
		transport: transport,
		agentID:   "sdk-agent",
		timeout:   30 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// TopicSummary contains summarized topic information.
type TopicSummary struct {
	Name             string `json:"name"`
	Partitions       int    `json:"partitions"`
	ReplicationFactor int   `json:"replication_factor"`
	MessageCount     int64  `json:"message_count_estimate"`
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

	var msgs []Message
	if err := json.Unmarshal(result, &msgs); err != nil {
		return nil, fmt.Errorf("parsing consume response: %w", err)
	}
	return msgs, nil
}

// Produce sends messages to a topic.
func (c *Client) Produce(ctx context.Context, topic string, messages []map[string]any) ([]DeliveryResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.transport.CallTool(ctx, "kafka_produce", map[string]any{
		"topic": topic, "messages": messages,
	})
	if err != nil {
		return nil, fmt.Errorf("producing messages: %w", err)
	}

	var resp struct {
		Results []DeliveryResult `json:"results"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing produce response: %w", err)
	}
	return resp.Results, nil
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
		Matches []Message `json:"matches"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return nil, fmt.Errorf("parsing search response: %w", err)
	}
	return resp.Matches, nil
}

// Watch registers a subscription for event-driven notifications.
func (c *Client) Watch(ctx context.Context, topicPattern, keyFilter, webhookURL string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	result, err := c.transport.CallTool(ctx, "kafka_watch", map[string]any{
		"action": "subscribe", "topic_pattern": topicPattern,
		"key_filter": keyFilter, "webhook_url": webhookURL,
	})
	if err != nil {
		return "", fmt.Errorf("creating watch: %w", err)
	}

	var resp struct {
		SubscriptionID string `json:"subscription_id"`
	}
	if err := json.Unmarshal(result, &resp); err != nil {
		return "", fmt.Errorf("parsing watch response: %w", err)
	}
	return resp.SubscriptionID, nil
}

// Close closes the client connection.
func (c *Client) Close() error {
	return c.transport.Close()
}
