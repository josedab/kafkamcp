package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerObservabilityTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_dashboard_create",
		mcp.WithDescription("Auto-provision Kafka monitoring dashboards for Grafana, Datadog, or custom providers. Creates pre-configured panels for cluster, topic, or consumer group metrics."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Dashboard name")),
		mcp.WithString("provider", mcp.Description("Dashboard provider: grafana, datadog, custom. Default: grafana")),
		mcp.WithString("type", mcp.Description("Dashboard type: cluster, topic, consumer_group. Default: cluster")),
		mcp.WithString("target", mcp.Description("Target resource (topic name or group ID)")),
	), s.handleDashboardCreate)

	s.mcpServer.AddTool(mcp.NewTool("kafka_alert_create",
		mcp.WithDescription("Create an alerting rule for Kafka metrics. Supports consumer lag, throughput, error rate, and partition metrics with configurable thresholds and notification channels."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Alert rule name")),
		mcp.WithString("metric", mcp.Required(), mcp.Description("Metric: consumer_lag, throughput, error_rate, partition_count")),
		mcp.WithString("condition", mcp.Description("Condition: gt, lt, eq, ne. Default: gt")),
		mcp.WithNumber("threshold", mcp.Required(), mcp.Description("Threshold value")),
		mcp.WithString("severity", mcp.Description("Severity: info, warning, critical. Default: warning")),
		mcp.WithString("duration", mcp.Description("Duration before firing: 5m, 15m, 1h. Default: 5m")),
		mcp.WithString("topic", mcp.Description("Topic to monitor")),
		mcp.WithString("group_id", mcp.Description("Consumer group to monitor")),
	), s.handleAlertCreate)

	s.mcpServer.AddTool(mcp.NewTool("kafka_alert_list",
		mcp.WithDescription("List all configured alerting rules."),
	), s.handleAlertList)

	s.mcpServer.AddTool(mcp.NewTool("kafka_alert_delete",
		mcp.WithDescription("Delete an alerting rule by ID."),
		mcp.WithString("alert_id", mcp.Required(), mcp.Description("Alert rule ID to delete")),
	), s.handleAlertDelete)
}
