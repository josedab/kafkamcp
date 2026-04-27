package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerAdvancedTools() {
	// Kafka Connect management
	s.mcpServer.AddTool(mcp.NewTool("kafka_create_connector",
		withCategory("streaming"),
		mcp.WithDescription("Create a new Kafka Connect connector with the specified configuration."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
		mcp.WithObject("config", mcp.Required(), mcp.Description("Connector configuration map")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleCreateConnector)

	s.mcpServer.AddTool(mcp.NewTool("kafka_update_connector",
		withCategory("streaming"),
		mcp.WithDescription("Update an existing Kafka Connect connector configuration."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
		mcp.WithObject("config", mcp.Required(), mcp.Description("New connector configuration map")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleUpdateConnector)

	s.mcpServer.AddTool(mcp.NewTool("kafka_delete_connector",
		withCategory("streaming"),
		mcp.WithDescription("Delete a Kafka Connect connector."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleDeleteConnector)

	s.mcpServer.AddTool(mcp.NewTool("kafka_restart_connector",
		withCategory("streaming"),
		mcp.WithDescription("Restart a Kafka Connect connector or specific tasks. Supports bulk restart of failed tasks."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
		mcp.WithNumber("task_id", mcp.Description("Specific task ID to restart (-1 for all). Default: -1")),
		mcp.WithBoolean("failed_only", mcp.Description("Only restart failed tasks. Default: false")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleRestartConnector)

	s.mcpServer.AddTool(mcp.NewTool("kafka_connector_health",
		withCategory("streaming"),
		mcp.WithDescription("Get health score and diagnostics for a Kafka Connect connector including task states, error rates, and recommendations."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleConnectorHealth)

	// Time-travel query engine
	s.mcpServer.AddTool(mcp.NewTool("kafka_time_travel",
		withCategory("streaming"),
		mcp.WithDescription("Point-in-time topic queries: snapshot state at a timestamp, reconstruct compacted topic state, or diff state between two timestamps."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: snapshot, diff")),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithString("timestamp", mcp.Description("Point-in-time timestamp (ISO 8601). Required for snapshot.")),
		mcp.WithString("timestamp_from", mcp.Description("Start timestamp for diff (ISO 8601)")),
		mcp.WithString("timestamp_to", mcp.Description("End timestamp for diff (ISO 8601)")),
		mcp.WithNumber("max_keys", mcp.Description("Max unique keys to track. Default: 10000")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleTimeTravel)

	// Intelligent topic recommendations
	s.mcpServer.AddTool(mcp.NewTool("kafka_recommend",
		withCategory("streaming"),
		mcp.WithDescription("Get intelligent recommendations for topic optimization: detect under-replicated partitions, idle topics, consumer lag trends, partition hotspots, retention tuning, and compression analysis."),
		mcp.WithString("topic", mcp.Description("Specific topic to analyze (optional, analyzes all if empty)")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleRecommend)

	// End-to-end message tracing
	s.mcpServer.AddTool(mcp.NewTool("kafka_trace_message",
		withCategory("streaming"),
		mcp.WithDescription("Trace a message across topics using correlation IDs. Auto-detects correlation-id, trace-id, x-request-id headers and builds a message flow graph."),
		mcp.WithString("correlation_id", mcp.Required(), mcp.Description("Correlation ID to trace")),
		mcp.WithString("header_name", mcp.Description("Header name containing correlation ID. Default: auto-detect")),
		mcp.WithObject("topics", mcp.Description("List of topic names to search. Default: all topics.")),
		mcp.WithNumber("max_scan", mcp.Description("Max messages to scan per topic. Default: 10000")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleTraceMessage)

	// ACL & Security audit
	s.mcpServer.AddTool(mcp.NewTool("kafka_list_acls",
		withCategory("streaming"),
		mcp.WithDescription("List all Kafka ACLs with principal, resource, and operation breakdown. Includes security analysis for over-permissioned principals and wildcard exposure."),
		mcp.WithString("principal", mcp.Description("Filter by principal (e.g., 'User:alice')")),
		mcp.WithString("resource_type", mcp.Description("Filter by resource type: topic, group, cluster, transactional_id")),
		mcp.WithBoolean("include_analysis", mcp.Description("Include security analysis. Default: true")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleListACLs)

	// Configuration drift detection
	s.mcpServer.AddTool(mcp.NewTool("kafka_config_drift",
		withCategory("streaming"),
		mcp.WithDescription("Detect configuration drift across clusters. Compare topic configs between clusters, check against golden templates, and generate remediation commands."),
		mcp.WithString("action", mcp.Description("Action: compare, audit, template. Default: compare")),
		mcp.WithString("topic", mcp.Description("Specific topic to compare")),
		mcp.WithString("source_cluster", mcp.Description("Source cluster for comparison")),
		mcp.WithString("target_cluster", mcp.Description("Target cluster for comparison")),
		mcp.WithObject("template", mcp.Description("Golden template configs for audit")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleConfigDrift)

	// Interactive Kafka playground
	s.mcpServer.AddTool(mcp.NewTool("kafka_playground",
		withCategory("streaming"),
		mcp.WithDescription("Sandboxed Kafka environment for safe experimentation. Create temporary topics, generate sample data, and auto-cleanup. Actions: create, produce_sample, list, cleanup."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, produce_sample, list, cleanup")),
		mcp.WithString("name", mcp.Description("Playground topic name suffix (auto-prefixed with _playground_)")),
		mcp.WithNumber("partitions", mcp.Description("Partitions for playground topic. Default: 3")),
		mcp.WithString("data_pattern", mcp.Description("Sample data pattern: ecommerce, iot, user_activity, logs. Default: ecommerce")),
		mcp.WithNumber("count", mcp.Description("Number of sample messages to produce. Default: 10")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handlePlayground)
}
