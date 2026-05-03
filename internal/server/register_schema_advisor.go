package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerSchemaAdvisorTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_schema_advisor_predict",
		withCategory("analytics"),
		mcp.WithDescription("Predict compatibility issues for a proposed schema change. Analyzes breaking changes, risk level, and provides mitigation suggestions."),
		mcp.WithString("subject", mcp.Required(), mcp.Description("Schema registry subject name")),
		mcp.WithString("schema", mcp.Required(), mcp.Description("Proposed new schema definition")),
		mcp.WithString("schema_type", mcp.Description("Schema type: AVRO, PROTOBUF, JSON. Default: AVRO")),
		mcp.WithString("compatibility", mcp.Description("Compatibility level: BACKWARD, FORWARD, FULL, NONE. Default: BACKWARD")),
	), s.handleSchemaAdvisorPredict)

	s.mcpServer.AddTool(mcp.NewTool("kafka_schema_advisor_migrate",
		withCategory("analytics"),
		mcp.WithDescription("Generate a step-by-step migration plan for evolving a schema between versions."),
		mcp.WithString("subject", mcp.Required(), mcp.Description("Schema registry subject name")),
		mcp.WithNumber("from_version", mcp.Description("Source version number")),
		mcp.WithNumber("to_version", mcp.Description("Target version number")),
	), s.handleSchemaAdvisorMigrate)

	s.mcpServer.AddTool(mcp.NewTool("kafka_schema_advisor_analyze",
		withCategory("analytics"),
		mcp.WithDescription("Analyze schema evolution history and provide health score, patterns, best practices, and antipattern detection."),
		mcp.WithString("subject", mcp.Description("Schema subject (omit to list all subjects)")),
	), s.handleSchemaAdvisorAnalyze)

	s.mcpServer.AddTool(mcp.NewTool("kafka_schema_advisor_intelligent",
		withCategory("analytics"),
		mcp.WithDescription("ML-powered schema intelligence: analyzes usage patterns, field stability, naming conventions, and predicts future evolution. Provides confidence-scored recommendations."),
		mcp.WithString("subject", mcp.Required(), mcp.Description("Schema registry subject name")),
	), s.handleSchemaAdvisorIntelligent)
}
