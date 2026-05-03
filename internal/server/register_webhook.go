package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerWebhookTools() {
	if s.webhookManager == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_webhook_subscribe",
		withCategory("ops"),
		mcp.WithDescription("Register a webhook subscription to receive Kafka events via HTTP POST. Supports topic patterns and event type filtering with optional HMAC-SHA256 signing."),
		mcp.WithString("id", mcp.Required(), mcp.Description("Unique subscription identifier")),
		mcp.WithString("url", mcp.Required(), mcp.Description("HTTP endpoint URL to receive webhook events")),
		mcp.WithObject("topics", mcp.Description("Topic patterns to match (supports trailing wildcard *). Empty matches all topics")),
		mcp.WithObject("events", mcp.Description("Event types to match: message, schema_change, consumer_lag_alert, topic_created, topic_deleted")),
		mcp.WithString("secret", mcp.Description("HMAC-SHA256 secret for signing payloads (sent in X-Webhook-Signature header)")),
		mcp.WithObject("metadata", mcp.Description("Arbitrary key-value metadata for the subscription")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_webhook_subscribe",
		resourceType: "cluster",
		action:       "write",
	}, s.handleWebhookSubscribe))

	s.mcpServer.AddTool(mcp.NewTool("kafka_webhook_unsubscribe",
		withCategory("ops"),
		mcp.WithDescription("Remove a webhook subscription by ID, stopping all future event deliveries."),
		mcp.WithString("id", mcp.Required(), mcp.Description("Subscription ID to remove")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_webhook_unsubscribe",
		resourceType: "cluster",
		action:       "write",
	}, s.handleWebhookUnsubscribe))

	s.mcpServer.AddTool(mcp.NewTool("kafka_webhook_list",
		withCategory("ops"),
		mcp.WithDescription("List all active webhook subscriptions with their configuration and status."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_webhook_list",
		resourceType: "cluster",
		action:       "read",
	}, s.handleWebhookList))

	s.mcpServer.AddTool(mcp.NewTool("kafka_webhook_deliver",
		withCategory("ops"),
		mcp.WithDescription("Dispatch a webhook event to all matching subscriptions. Delivery is asynchronous with automatic retries."),
		mcp.WithString("event_id", mcp.Required(), mcp.Description("Unique event identifier")),
		mcp.WithString("event_type", mcp.Required(), mcp.Description("Event type: message, schema_change, consumer_lag_alert, topic_created, topic_deleted")),
		mcp.WithString("topic", mcp.Description("Kafka topic the event relates to")),
		mcp.WithString("cluster", mcp.Description("Kafka cluster name")),
		mcp.WithObject("payload", mcp.Description("Event payload data")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_webhook_deliver",
		resourceType: "cluster",
		action:       "write",
	}, s.handleWebhookDeliver))

	s.mcpServer.AddTool(mcp.NewTool("kafka_webhook_history",
		withCategory("ops"),
		mcp.WithDescription("Get recent delivery attempts for a webhook subscription including status codes, latency, and errors."),
		mcp.WithString("subscription_id", mcp.Required(), mcp.Description("Subscription ID to get history for")),
		mcp.WithNumber("limit", mcp.Description("Maximum number of delivery attempts to return. Default: 20")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_webhook_history",
		resourceType: "cluster",
		action:       "read",
	}, s.handleWebhookHistory))
}
