package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerSchemaPlaygroundTools() {
	s.addTool(mcp.NewTool("kafka_schema_playground",
		withCategory("schema"),
		mcp.WithDescription("Interactive schema playground for drafting, testing, and validating schemas. Supports draft lifecycle: create → test → validate → promote."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: create, update, test, validate, promote, get, list, evolve")),
		mcp.WithString("draft_id", mcp.Description("Draft ID (for update/test/validate/promote/get/evolve)")),
		mcp.WithString("subject", mcp.Description("Schema subject (for create)")),
		mcp.WithString("schema_type", mcp.Description("Schema type: AVRO, PROTOBUF, JSON (default: AVRO)")),
		mcp.WithString("schema", mcp.Description("Schema definition (for create/update)")),
		mcp.WithString("message", mcp.Description("Sample message JSON to test against schema (for test)")),
		mcp.WithString("against_schema", mcp.Description("Schema to validate compatibility against (for validate/evolve)")),
		mcp.WithString("compatibility", mcp.Description("Compatibility level: BACKWARD, FORWARD, FULL (for validate)")),
		mcp.WithString("status_filter", mcp.Description("Filter drafts by status (for list)")),
	), s.handleSchemaPlayground)
}
