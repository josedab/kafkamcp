package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerInteractiveOnboardingTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_onboard_interactive",
		withCategory("onboarding"),
		mcp.WithDescription("AI-guided interactive onboarding experience. Walks you through setup, configuration, and first successful Kafka interaction with contextual guidance. Actions: start, current_step, advance, skip, progress, help."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: start, current_step, advance, skip, progress, help")),
		mcp.WithString("session_id", mcp.Description("Session ID (for all actions except start)")),
		mcp.WithString("result", mcp.Description("Result of current step (for advance action)")),
	), s.handleInteractiveOnboarding)
}
