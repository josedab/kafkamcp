package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerSchemaWorkflowTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_schema_propose",
		mcp.WithDescription("Propose a schema change with auto-generated migration plan, compatibility validation, and consumer impact analysis. Creates an approval workflow."),
		mcp.WithString("subject", mcp.Required(), mcp.Description("Schema Registry subject name")),
		mcp.WithString("schema", mcp.Required(), mcp.Description("New schema definition (JSON string)")),
		mcp.WithString("schema_type", mcp.Description("Schema type: AVRO, JSON, PROTOBUF. Default: AVRO")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleSchemaPropose)

	s.mcpServer.AddTool(mcp.NewTool("kafka_schema_apply",
		mcp.WithDescription("Apply, review, or rollback a schema proposal. Actions: review (approve/reject), apply (register schema), rollback (revert), list (show proposals), get (proposal details), timeline (version history)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: review, apply, rollback, list, get, timeline")),
		mcp.WithString("proposal_id", mcp.Description("Proposal ID (required for review/apply/rollback/get)")),
		mcp.WithString("review_action", mcp.Description("Review action: approve, reject (required for review)")),
		mcp.WithString("note", mcp.Description("Review note or comment")),
		mcp.WithString("status_filter", mcp.Description("Filter proposals by status: pending, approved, rejected, applied")),
		mcp.WithString("subject", mcp.Description("Subject for timeline action")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleSchemaApply)
}
