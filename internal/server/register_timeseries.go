package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerTimeSeriesTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_timeseries_analyze",
		withCategory("ops"),
		mcp.WithDescription("Advanced time-series analysis: Mann-Kendall trend detection, FFT seasonality analysis, outlier identification, and cross-metric correlation. Returns chart-ready JSON."),
		mcp.WithString("action", mcp.Description("Action: analyze, correlate, ingest. Default: analyze")),
		mcp.WithString("metric", mcp.Required(), mcp.Description("Metric type: consumer_lag, throughput, error_rate")),
		mcp.WithString("source", mcp.Required(), mcp.Description("Source name (topic or consumer group)")),
		mcp.WithString("metric2", mcp.Description("Second metric for correlation")),
		mcp.WithString("source2", mcp.Description("Second source for correlation")),
		mcp.WithNumber("since_hours", mcp.Description("Analysis window in hours. Default: 24")),
		mcp.WithNumber("value", mcp.Description("Metric value (for ingest action)")),
	), s.handleTimeSeriesAnalyze)
}
