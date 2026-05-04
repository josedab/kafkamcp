package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerIntentVerifyTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_intent_verify",
		withCategory("governance"),
		mcp.WithDescription("Agent intent verification. Classify tools by risk level, verify agent intent matches operation semantics, and manage confirmation flows for destructive operations."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: classify, verify, confirm, resolve, pending, list_classifications, configure_agent, stats")),
		mcp.WithString("tool_name", mcp.Description("Tool name to classify or verify (for classify/verify)")),
		mcp.WithString("intent", mcp.Description("Agent's stated intent (for verify/confirm)")),
		mcp.WithString("confirm_id", mcp.Description("Confirmation ID (for resolve)")),
		mcp.WithBoolean("approved", mcp.Description("Approve or deny (for resolve)")),
		mcp.WithString("risk_level", mcp.Description("Filter by risk: read, write, destructive, admin")),
		mcp.WithString("target_agent", mcp.Description("Agent ID to configure (for configure_agent)")),
		mcp.WithString("max_risk", mcp.Description("Max risk level for agent (for configure_agent)")),
		mcp.WithBoolean("block_destructive", mcp.Description("Block destructive ops for agent (for configure_agent)")),
	), s.handleIntentVerify)
}
