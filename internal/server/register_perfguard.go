package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerPerfGuardTools() {
	s.addTool(mcp.NewTool("kafka_perf_guard",
		withCategory("observability"),
		mcp.WithDescription("Automated performance regression guard. Continuously benchmark Kafka operations, detect regressions via CUSUM change-point detection, and auto-throttle misbehaving agents."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: record, baseline, regressions, throttle_status, unthrottle, reset_baseline, stats")),
		mcp.WithString("agent_id", mcp.Description("Agent ID to check or record metrics for")),
		mcp.WithString("metric", mcp.Description("Metric: produce_latency_ms, consume_latency_ms, throughput_msg_sec, error_rate")),
		mcp.WithNumber("value", mcp.Description("Metric value (for record)")),
		mcp.WithNumber("limit", mcp.Description("Max results to return (default: 50)")),
	), s.handlePerfGuard)
}
