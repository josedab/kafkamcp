package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerCDCTools() {
	s.addTool(mcp.NewTool("kafka_cdc_create_bridge",
		withCategory("streaming"),
		mcp.WithDescription("Create a CDC bridge that streams changes from a Kafka topic to a target data store (postgres, mysql, mongodb, elasticsearch, s3, bigquery, snowflake, redis, http)."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Bridge name")),
		mcp.WithString("source_topic", mcp.Required(), mcp.Description("Source Kafka topic")),
		mcp.WithString("sink_type", mcp.Required(), mcp.Description("Target sink type: postgres, mysql, mongodb, elasticsearch, s3, bigquery, snowflake, redis, http")),
		mcp.WithString("table", mcp.Description("Target table/collection/index name")),
		mcp.WithString("key_field", mcp.Description("Primary key field for upserts")),
		mcp.WithString("cluster", mcp.Description("Source Kafka cluster name")),
	), s.handleCDCCreateBridge)

	s.addTool(mcp.NewTool("kafka_cdc_list_bridges",
		withCategory("streaming"),
		mcp.WithDescription("List all CDC bridges with their status and statistics."),
	), s.handleCDCListBridges)

	s.addTool(mcp.NewTool("kafka_cdc_manage_bridge",
		withCategory("streaming"),
		mcp.WithDescription("Manage a CDC bridge: get status, pause, resume, stop, or delete."),
		mcp.WithString("bridge_id", mcp.Required(), mcp.Description("Bridge ID")),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: status, pause, resume, stop, delete, events")),
	), s.handleCDCManageBridge)

	s.addTool(mcp.NewTool("kafka_cdc_process",
		withCategory("streaming"),
		mcp.WithDescription("Process a batch of messages through a CDC bridge, applying transformations and tracking change operations."),
		mcp.WithString("bridge_id", mcp.Required(), mcp.Description("Bridge ID to process through")),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Source topic")),
		mcp.WithString("cluster", mcp.Description("Kafka cluster name")),
	), s.handleCDCProcess)
}
