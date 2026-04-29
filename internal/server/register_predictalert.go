package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerPredictAlertTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_predict_alert",
		withCategory("ops"),
		mcp.WithDescription("Predictive consumer group alerting. Analyzes lag trends, consumption rates, and partition assignment patterns to forecast potential issues before they occur."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: analyze, record, alerts, acknowledge")),
		mcp.WithString("group_id", mcp.Description("Consumer group ID (required for analyze/record)")),
		mcp.WithNumber("total_lag", mcp.Description("Current total lag (for record action)")),
		mcp.WithNumber("member_count", mcp.Description("Current member count (for record action)")),
		mcp.WithString("alert_id", mcp.Description("Alert ID (for acknowledge action)")),
		mcp.WithNumber("limit", mcp.Description("Max alerts to return (for alerts action, default: 50)")),
	), s.handlePredictAlert)
}
