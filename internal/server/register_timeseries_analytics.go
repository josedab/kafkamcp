package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerTimeSeriesAnalyticsTools() {
	s.addTool(mcp.NewTool("kafka_timeseries_query",
		withCategory("analytics"),
		mcp.WithDescription("Query time-series metrics for a topic or consumer group within a configurable window (1h, 24h, 7d, 30d). Returns statistical summary, trend analysis, and percentile distribution."),
		mcp.WithString("metric", mcp.Required(), mcp.Description("Metric type: throughput_in, throughput_out, latency_p50, latency_p95, latency_p99, consumer_lag, partition_skew, group_velocity")),
		mcp.WithString("source", mcp.Required(), mcp.Description("Source identifier (topic name or consumer group)")),
		mcp.WithString("window", mcp.Description("Time window: 1h, 24h, 7d, 30d. Default: 1h")),
	), s.handleTimeSeriesQuery)

	s.addTool(mcp.NewTool("kafka_timeseries_compare",
		withCategory("analytics"),
		mcp.WithDescription("Compare time-series metrics between two windows to identify changes, degradation, or improvements over time."),
		mcp.WithString("metric", mcp.Required(), mcp.Description("Metric type")),
		mcp.WithString("source", mcp.Required(), mcp.Description("Source identifier")),
		mcp.WithString("base_window", mcp.Required(), mcp.Description("Base window for comparison (e.g., 24h)")),
		mcp.WithString("compare_window", mcp.Required(), mcp.Description("Window to compare against (e.g., 1h)")),
	), s.handleTimeSeriesCompare)

	s.addTool(mcp.NewTool("kafka_timeseries_baseline",
		withCategory("analytics"),
		mcp.WithDescription("Compute a baseline from historical data and compare current metrics against it. Detects deviations and scores overall health."),
		mcp.WithString("metric", mcp.Required(), mcp.Description("Metric type")),
		mcp.WithString("source", mcp.Required(), mcp.Description("Source identifier")),
		mcp.WithString("baseline_window", mcp.Description("Window for baseline computation. Default: 7d")),
		mcp.WithString("current_window", mcp.Description("Window for current comparison. Default: 1h")),
	), s.handleTimeSeriesBaseline)

	s.addTool(mcp.NewTool("kafka_timeseries_list",
		withCategory("analytics"),
		mcp.WithDescription("List all tracked time-series and available analysis windows."),
	), s.handleTimeSeriesList)
}
