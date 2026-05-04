package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerAgenticRunbookTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_agentic_runbook",
		withCategory("ops"),
		mcp.WithDescription("Agentic runbook executor with YAML DSL, approval gates, dry-run mode, blast-radius limits, and automatic rollback for multi-step Kafka operational procedures."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, create, execute, status, approve, reject, rollback, resume")),
		mcp.WithString("id", mcp.Description("Runbook ID or execution ID")),
		mcp.WithString("definition", mcp.Description("Runbook definition as YAML (for create)")),
		mcp.WithObject("params", mcp.Description("Parameters for runbook execution")),
		mcp.WithBoolean("dry_run", mcp.Description("Run in dry-run mode (default: false)")),
		mcp.WithString("approver", mcp.Description("Approver ID (for approve/reject)")),
		mcp.WithString("reason", mcp.Description("Rejection reason (for reject)")),
		mcp.WithString("category", mcp.Description("Filter by category (for list)")),
	), s.handleAgenticRunbook)
}
