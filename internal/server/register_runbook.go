package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerRunbookTools() {
	s.addTool(mcp.NewTool("kafka_runbook",
		withCategory("ops"),
		mcp.WithDescription("Automated runbook executor for multi-step operational procedures (partition reassignment, rolling restart verification, topic migration). Defines and runs auditable workflows."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, create, execute, status, complete_step, executions, enable, disable, delete")),
		mcp.WithString("id", mcp.Description("Runbook ID or execution ID")),
		mcp.WithString("category", mcp.Description("Filter runbooks by category (for list)")),
		mcp.WithString("definition", mcp.Description("Runbook definition as JSON (for create)")),
		mcp.WithObject("params", mcp.Description("Parameters for runbook execution (for execute)")),
		mcp.WithNumber("step_index", mcp.Description("Step index to complete (for complete_step)")),
		mcp.WithString("step_status", mcp.Description("Step completion status: completed, failed, skipped (for complete_step)")),
		mcp.WithString("step_output", mcp.Description("Step output as JSON string (for complete_step)")),
		mcp.WithString("step_error", mcp.Description("Step error message (for complete_step)")),
	), s.handleRunbook)
}
