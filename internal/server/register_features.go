package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerFeatureTools() {
	// Consumer Lag Diagnosis (handler in tools_lagdiag.go).
	s.addTool(mcp.NewTool("kafka_diagnose_lag",
		withCategory("governance"),
		mcp.WithDescription("Deep diagnostic analysis of consumer group lag. Traces root cause: slow consumers, producer spikes, partition imbalance, or GC pressure."),
		mcp.WithString("group_id", mcp.Required(), mcp.Description("Consumer group ID")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleDiagnoseLag)
}
