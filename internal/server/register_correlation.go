package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerCorrelationTools() {
	s.addTool(mcp.NewTool("kafka_correlation_ingest",
		withCategory("analytics"),
		mcp.WithDescription("Ingest external observability signals (from Prometheus, Datadog, PagerDuty, or custom sources) for correlation with Kafka metrics."),
		mcp.WithString("source", mcp.Required(), mcp.Description("Signal source: prometheus, datadog, pagerduty, kafka, custom")),
		mcp.WithString("metric", mcp.Required(), mcp.Description("Metric name (e.g., cpu_usage, error_rate, response_time)")),
		mcp.WithString("value", mcp.Required(), mcp.Description("Metric value")),
		mcp.WithString("severity", mcp.Description("Signal severity: info, warning, critical")),
		mcp.WithString("message", mcp.Description("Optional message describing the signal")),
	), s.handleCorrelationIngest)

	s.addTool(mcp.NewTool("kafka_correlation_analyze",
		withCategory("analytics"),
		mcp.WithDescription("Analyze correlations between Kafka metrics and external signals within a time window. Produces temporal correlations, root cause ranking, and unified timeline."),
		mcp.WithString("window", mcp.Description("Time window: 1h, 6h, 24h. Default: 1h")),
		mcp.WithString("metric", mcp.Description("Filter by specific metric name")),
	), s.handleCorrelationAnalyze)

	s.addTool(mcp.NewTool("kafka_correlation_endpoints",
		withCategory("analytics"),
		mcp.WithDescription("Register or list external observability endpoints for signal ingestion."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: register, list")),
		mcp.WithString("source", mcp.Description("Source type: prometheus, datadog, pagerduty")),
		mcp.WithString("url", mcp.Description("Endpoint URL (for register)")),
	), s.handleCorrelationEndpoints)
}
