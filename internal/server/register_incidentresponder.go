package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerIncidentResponderTools() {
	s.addTool(mcp.NewTool("kafka_incident_respond",
		withCategory("ops"),
		mcp.WithDescription("AI incident responder: generate root cause hypotheses, create remediation runbooks, and execute fixes with human-in-the-loop approval gates."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: respond, hypothesize, generate_runbook, approve_runbook, execute_runbook, get_runbook")),
		mcp.WithString("incident_id", mcp.Description("Incident ID (required for respond/hypothesize/generate_runbook)")),
		mcp.WithString("runbook_id", mcp.Description("Runbook ID (for approve/execute/get)")),
	), s.handleIncidentRespond)
}
