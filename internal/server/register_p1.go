package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerP1Tools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_create_topic",
		mcp.WithDescription("Create a new Kafka topic with specified partitions, replication factor, and configuration."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithNumber("partitions", mcp.Description("Number of partitions. Default: 1")),
		mcp.WithNumber("replication_factor", mcp.Description("Replication factor. Default: 1")),
		mcp.WithObject("configs", mcp.Description("Topic configs (e.g., {\"retention.ms\": \"604800000\"})")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleCreateTopic)

	s.mcpServer.AddTool(mcp.NewTool("kafka_alter_topic",
		mcp.WithDescription("Modify configuration of an existing Kafka topic."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithObject("configs", mcp.Required(), mcp.Description("Configs to change (e.g., {\"retention.ms\": \"86400000\"})")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleAlterTopic)

	s.mcpServer.AddTool(mcp.NewTool("kafka_delete_topic",
		mcp.WithDescription("Delete a Kafka topic. This is a destructive operation."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Topic name to delete")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleDeleteTopic)

	s.mcpServer.AddTool(mcp.NewTool("kafka_search",
		mcp.WithDescription("Search/filter messages in a topic by key, header, value content, or time range. Bounded scan."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithString("key_filter", mcp.Description("Filter messages where key contains this string")),
		mcp.WithString("header_key", mcp.Description("Filter messages that have this header key")),
		mcp.WithString("header_value", mcp.Description("Filter messages where the header matches this value")),
		mcp.WithString("value_contains", mcp.Description("Filter messages where value contains this string")),
		mcp.WithString("start_time", mcp.Description("Start time (ISO 8601) for time range filter")),
		mcp.WithString("end_time", mcp.Description("End time (ISO 8601) for time range filter")),
		mcp.WithNumber("max_scan", mcp.Description("Max messages to scan. Default: 10000")),
		mcp.WithNumber("limit", mcp.Description("Max matching messages to return. Default: 10")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleSearch)

	s.mcpServer.AddTool(mcp.NewTool("kafka_reset_offsets",
		mcp.WithDescription("Reset consumer group offsets. Supports earliest, latest, or timestamp strategies with dry-run mode."),
		mcp.WithString("group_id", mcp.Required(), mcp.Description("Consumer group ID")),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithString("strategy", mcp.Required(), mcp.Description("Reset strategy: earliest, latest, timestamp")),
		mcp.WithString("value", mcp.Description("Timestamp value for 'timestamp' strategy (ISO 8601)")),
		mcp.WithBoolean("dry_run", mcp.Description("Preview without applying. Default: false")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleResetOffsets)

	s.mcpServer.AddTool(mcp.NewTool("kafka_analyze_dlq",
		mcp.WithDescription("Analyze a dead-letter queue topic: consume recent messages, parse error headers, categorize failure types, and return a statistical summary with samples."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("DLQ topic name")),
		mcp.WithNumber("limit", mcp.Description("Max messages to analyze. Default: 100")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleAnalyzeDLQ)

	// FR-11: Streaming Subscription
	s.mcpServer.AddTool(mcp.NewTool("kafka_subscribe",
		mcp.WithDescription("Subscribe to a Kafka topic using consumer group semantics. Polls for new messages for the specified duration and returns them as a batch. Supports auto-commit for offset management."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name to subscribe to")),
		mcp.WithString("group_id", mcp.Description("Consumer group ID. If omitted, an ephemeral group is created.")),
		mcp.WithBoolean("auto_commit", mcp.Description("Automatically commit offsets after consuming. Default: true")),
		mcp.WithNumber("max_messages", mcp.Description("Maximum messages to return per poll (1-1000). Default: 100")),
		mcp.WithNumber("timeout_ms", mcp.Description("How long to poll for messages in ms (max 30000). Default: 10000")),
		mcp.WithString("deserializer", mcp.Description("Deserialization format: auto, json, string, bytes. Default: auto")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleSubscribe)
}
