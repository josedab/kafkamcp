package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerHealthScoreTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_health_score",
		withCategory("ops"),
		mcp.WithDescription("Get a unified health score for a Kafka cluster by aggregating broker status, under-replicated partitions, ISR shrink rate, and consumer lag into a single score (0-100)."),
		mcp.WithString("cluster", mcp.Description("Cluster name for multi-cluster setups.")),
		mcp.WithString("action", mcp.Description("Action: evaluate (default), history")),
		mcp.WithNumber("history_limit", mcp.Description("Number of historical scores to return (default: 20)")),
	), s.handleHealthScore)
}
