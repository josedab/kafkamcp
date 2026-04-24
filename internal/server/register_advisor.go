package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerAdvisorTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_advisor_scan",
		mcp.WithDescription("Scan topics for best-practice violations, anti-patterns, and improvement opportunities. Checks replication factors, partition counts, retention policies, naming conventions, orphaned topics, and topic sprawl."),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
		mcp.WithString("severity", mcp.Description("Filter by severity: info, warning, critical")),
		mcp.WithString("category", mcp.Description("Filter by category: replication, partitioning, retention, naming, hygiene")),
	), s.handleAdvisorScan)

	s.mcpServer.AddTool(mcp.NewTool("kafka_advisor_report",
		mcp.WithDescription("Generate a detailed advisory report with health scores, category breakdowns, and prioritized recommendations."),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
	), s.handleAdvisorReport)
}
