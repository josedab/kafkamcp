package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerCapacityTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_capacity_plan",
		withCategory("ops"),
		mcp.WithDescription("Generate a comprehensive capacity plan with ML-based forecasting, storage growth analysis, and scaling recommendations for all tracked topics."),
		mcp.WithNumber("horizon_days", mcp.Description("Forecast horizon in days. Default: 30")),
		mcp.WithNumber("max_storage_gb", mcp.Description("Maximum cluster storage in GB. Default: 1000")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleCapacityPlan)

	s.mcpServer.AddTool(mcp.NewTool("kafka_capacity_forecast",
		withCategory("ops"),
		mcp.WithDescription("Forecast capacity for a specific topic using exponential smoothing and linear regression. Predicts throughput, storage, and lag trends."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithString("metric", mcp.Description("Metric to forecast: throughput, storage, lag. Default: throughput")),
		mcp.WithNumber("horizon_days", mcp.Description("Forecast horizon in days. Default: 7")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleCapacityForecast)
}
