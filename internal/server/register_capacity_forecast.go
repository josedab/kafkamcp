package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerPredictiveCapacityTools() {
	s.addTool(mcp.NewTool("kafka_capacity_forecast",
		withCategory("analytics"),
		mcp.WithDescription("ML-based Kafka capacity forecasting. Analyzes historical metrics to predict scaling needs and recommend optimal partition/broker configurations."),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
		mcp.WithNumber("horizon_days", mcp.Description("Forecast horizon in days. Default: 30")),
		mcp.WithNumber("current_brokers", mcp.Description("Current broker count. Default: 3")),
		mcp.WithNumber("max_storage_gb", mcp.Description("Max storage capacity in GB. Default: 1000")),
		mcp.WithNumber("cost_per_gb", mcp.Description("Cost per GB per month in USD. Default: 0.10")),
		mcp.WithNumber("cost_per_broker", mcp.Description("Cost per broker per month in USD. Default: 500")),
	), s.handlePredictiveCapacityForecast)
}
