package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerAdaptiveRateTools() {
	if s.adaptiveRateEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_quota_status",
		withCategory("ops"),
		mcp.WithDescription("Get adaptive rate limit status for an agent or all agents. Shows current rate, dynamic quota, burst credits, utilization, and recommendations."),
		mcp.WithString("agent_id", mcp.Description("Agent ID (omit for all agents)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_quota_status",
		resourceType: "topic",
		action:       "read",
	}, s.handleQuotaStatus))

	s.mcpServer.AddTool(mcp.NewTool("kafka_quota_request",
		withCategory("ops"),
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

	s.mcpServer.AddTool(mcp.NewTool("kafka_rate_intelligence",
		withCategory("ops"),
		mcp.WithDescription("Get intelligent rate analysis: agent behavior patterns (steady, bursty, growing, declining), global utilization, and actionable recommendations for quota tuning."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_rate_intelligence",
		resourceType: "cluster",
		action:       "read",
	}, s.handleRateIntelligence))

	s.mcpServer.AddTool(mcp.NewTool("kafka_cluster_health_update",
		withCategory("ops"),
		mcp.WithDescription("Report cluster health metrics to auto-adjust rate limits. When cluster is degraded, quotas are reduced; when healthy, quotas are restored."),
		mcp.WithString("status", mcp.Required(), mcp.Description("Cluster status: healthy, degraded, critical")),
		mcp.WithNumber("cpu_utilization", mcp.Description("CPU utilization percentage (0-100)")),
		mcp.WithNumber("error_rate", mcp.Description("Error rate percentage (0-100)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cluster_health_update",
		resourceType: "cluster",
		action:       "admin",
	}, s.handleClusterHealthUpdate))
}
