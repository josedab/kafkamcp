package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerP2Tools() {
	s.addTool(mcp.NewTool("kafka_aggregate",
		withCategory("streaming"),
		mcp.WithDescription("Compute count, sum, avg, min, or max over a time window on a numeric field in topic messages. Lightweight stream processing."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithString("field", mcp.Description("Dot-notation path to the numeric field (e.g., 'amount', 'metrics.latency_ms'). Empty for the entire value.")),
		mcp.WithString("function", mcp.Required(), mcp.Description("Aggregation function: count, sum, avg, min, max")),
		mcp.WithNumber("window_ms", mcp.Description("Time window in milliseconds. Default: 300000 (5 min)")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleAggregate)

	s.addTool(mcp.NewTool("kafka_suggest_schema_evolution",
		withCategory("streaming"),
		mcp.WithDescription("Given a current schema subject and a desired change description, suggest compatible evolution strategies with step-by-step instructions."),
		mcp.WithString("subject", mcp.Required(), mcp.Description("Schema subject name")),
		mcp.WithString("change", mcp.Required(), mcp.Description("Description of desired change (e.g., 'add a new field for user email', 'rename field from name to full_name', 'remove deprecated field')")),
	), s.handleSuggestSchemaEvolution)

	s.addTool(mcp.NewTool("kafka_list_connectors",
		withCategory("streaming"),
		mcp.WithDescription("List all Kafka Connect connectors with their status, type, and worker assignment."),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleListConnectors)

	s.addTool(mcp.NewTool("kafka_describe_connector",
		withCategory("streaming"),
		mcp.WithDescription("Get full details for a Kafka Connect connector including config, task status, and errors."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Connector name")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleDescribeConnector)

	s.addTool(mcp.NewTool("kafka_join",
		withCategory("streaming"),
		mcp.WithDescription("Perform a time-windowed join across two topics on a key field. Returns matched messages from both topics."),
		mcp.WithString("left_topic", mcp.Required(), mcp.Description("Left topic name")),
		mcp.WithString("right_topic", mcp.Required(), mcp.Description("Right topic name")),
		mcp.WithString("key_field", mcp.Description("Dot-notation field to join on. Empty uses the message key.")),
		mcp.WithNumber("window_ms", mcp.Description("Time window in ms. Default: 60000 (1 min)")),
		mcp.WithNumber("limit", mcp.Description("Max joined results. Default: 100")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleJoin)

	s.addTool(mcp.NewTool("kafka_query",
		withCategory("streaming"),
		mcp.WithDescription("Accept a natural language description of desired data and translate it to a topic lookup, filter, and consume call. Example: 'last 10 failed payments'"),
		mcp.WithString("query", mcp.Required(), mcp.Description("Natural language description of the data you want")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleNaturalLanguageQuery)
}
