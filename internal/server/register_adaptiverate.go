package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerAdaptiveRateTools() {
	if s.adaptiveRateEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_quota_status",
		mcp.WithDescription("Get adaptive rate limit status for an agent or all agents. Shows current rate, dynamic quota, burst credits, utilization, and recommendations."),
		mcp.WithString("agent_id", mcp.Description("Agent ID (omit for all agents)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_quota_status",
		resourceType: "topic",
		action:       "read",
	}, s.handleQuotaStatus))

	s.mcpServer.AddTool(mcp.NewTool("kafka_quota_request",
		mcp.WithDescription("Request a quota adjustment: increase, decrease, reset, or grant burst credits."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: increase, decrease, reset, grant_credits")),
		mcp.WithString("agent_id", mcp.Description("Target agent ID (default: self)")),
		mcp.WithNumber("amount", mcp.Description("Adjustment amount")),
		mcp.WithString("reason", mcp.Description("Reason for the adjustment")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_quota_request",
		resourceType: "topic",
		action:       "admin",
	}, s.handleQuotaRequest))
}
