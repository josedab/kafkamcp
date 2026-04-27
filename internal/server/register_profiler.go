package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerProfilerTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_agent_profile",
		withCategory("ops"),
		mcp.WithDescription("Profile agent performance: latency distributions, tool call patterns, error rates, and efficiency scoring. Actions: profile (agent metrics), slow_queries (find slow calls), list_agents (all agents)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: profile, slow_queries, list_agents")),
		mcp.WithString("agent_id", mcp.Description("Agent ID to profile")),
		mcp.WithNumber("since_hours", mcp.Description("Look back period in hours. Default: 24")),
		mcp.WithNumber("threshold_ms", mcp.Description("Slow query threshold in ms. Default: 1000")),
	), s.handleAgentProfile)

	s.mcpServer.AddTool(mcp.NewTool("kafka_agent_optimize",
		withCategory("ops"),
		mcp.WithDescription("Get optimization suggestions for an agent based on its call patterns. Detects redundant calls, suboptimal tool sequences, and suggests improvements."),
		mcp.WithString("agent_id", mcp.Required(), mcp.Description("Agent ID to optimize")),
		mcp.WithNumber("since_hours", mcp.Description("Look back period in hours. Default: 24")),
	), s.handleAgentOptimize)

	s.mcpServer.AddTool(mcp.NewTool("kafka_agent_report",
		withCategory("ops"),
		mcp.WithDescription("Generate a comprehensive agent efficiency report with trends, comparisons, and scoring."),
		mcp.WithString("agent_id", mcp.Description("Agent ID (empty = all agents)")),
		mcp.WithNumber("since_hours", mcp.Description("Look back period in hours. Default: 24")),
	), s.handleAgentReport)
}
