package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerTelemetryDataLakeTools() {
	s.addTool(mcp.NewTool("kafka_telemetry_lake",
		withCategory("observability"),
		mcp.WithDescription("Agent telemetry data lake for observability and analytics. Actions: stats, agent_report, query, export."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: stats, agent_report, query, export")),
		mcp.WithString("agent_id", mcp.Description("Agent ID filter")),
		mcp.WithString("tool", mcp.Description("Tool name filter")),
		mcp.WithString("cluster", mcp.Description("Cluster filter")),
		mcp.WithString("status", mcp.Description("Status filter: success, error")),
		mcp.WithString("format", mcp.Description("Export format: json, jsonl, csv. Default: json")),
		mcp.WithNumber("limit", mcp.Description("Max records. Default: 100")),
	), s.handleTelemetryDataLake)
}
