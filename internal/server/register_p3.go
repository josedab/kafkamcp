package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerP3Tools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_watch",
		withCategory("streaming"),
		mcp.WithDescription("Register or manage event-driven watch subscriptions on topic patterns. When matching messages arrive, notifications are pushed via webhooks or MCP sampling."),
		mcp.WithString("action", mcp.Description("Action: subscribe, unsubscribe, list. Default: subscribe")),
		mcp.WithString("topic_pattern", mcp.Description("Glob pattern to match topics (e.g., 'orders.*', '*-dlq'). Required for subscribe.")),
		mcp.WithString("key_filter", mcp.Description("Filter messages where key contains this string")),
		mcp.WithString("value_filter", mcp.Description("Filter messages where value contains this string")),
		mcp.WithString("webhook_url", mcp.Description("URL to POST notifications to")),
		mcp.WithString("subscription_id", mcp.Description("Subscription ID (required for unsubscribe)")),
	), s.handleWatch)

	s.mcpServer.AddTool(mcp.NewTool("kafka_remediate_dlq",
		withCategory("streaming"),
		mcp.WithDescription("Auto-remediate DLQ messages: retry with backoff, route to correction topic, or generate fix suggestions using schema context."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("DLQ topic name")),
		mcp.WithString("action", mcp.Description("Remediation action: analyze, retry, route, suggest. Default: analyze")),
		mcp.WithString("target_topic", mcp.Description("Target topic for routing remediation")),
		mcp.WithNumber("limit", mcp.Description("Max messages to process. Default: 100")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleRemediateDLQ)

	if s.hasSchemaRegistry() {
		s.mcpServer.AddTool(mcp.NewTool("kafka_schema_diff",
		withCategory("streaming"),
			mcp.WithDescription("Compute a field-by-field diff between two schema versions showing additions, removals, and changes."),
			mcp.WithString("subject", mcp.Required(), mcp.Description("Schema subject name")),
			mcp.WithString("version_from", mcp.Required(), mcp.Description("Source version number")),
			mcp.WithString("version_to", mcp.Description("Target version number. Default: 'latest'")),
		), s.handleSchemaDiff)
	}
}
