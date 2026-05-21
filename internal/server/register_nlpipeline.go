package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerNLPipelineTools() {
	s.addTool(mcp.NewTool("kafka_nl_pipeline_build",
		withCategory("streaming"),
		mcp.WithDescription("Build a data pipeline from a natural language description. Translates plain English into Kafka Streams/Connect configurations with filter, transform, aggregate, and routing steps."),
		mcp.WithString("description", mcp.Required(), mcp.Description("Natural language description of the desired pipeline, e.g., 'Filter orders from orders-topic where amount > 100 and send to premium-orders'")),
	), s.handleNLPipelineBuild)

	s.addTool(mcp.NewTool("kafka_nl_pipeline_manage",
		withCategory("streaming"),
		mcp.WithDescription("Manage NL-generated pipelines: list, get, validate, deploy, or delete."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, validate, deploy, delete, parse")),
		mcp.WithString("pipeline_id", mcp.Description("Pipeline ID (required for get/validate/deploy/delete)")),
		mcp.WithString("description", mcp.Description("NL description (for parse action)")),
	), s.handleNLPipelineManage)
}
