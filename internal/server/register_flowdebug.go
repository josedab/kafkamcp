package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerFlowDebugTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_flow_debug",
		mcp.WithDescription("Live message flow debugging. Builds a real-time graph of producers, topics, and consumer groups with throughput/latency metrics. Actions: graph (get current flow graph), snapshots (historical snapshots), config (get/set bottleneck thresholds), collect (trigger metric collection from cluster)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: graph, snapshots, config, collect")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
		mcp.WithNumber("since_minutes", mcp.Description("Look back period for snapshots. Default: 60")),
		mcp.WithNumber("limit", mcp.Description("Max snapshots to return. Default: 30")),
		mcp.WithObject("thresholds", mcp.Description("Bottleneck thresholds for config action (lag_warning, lag_critical, error_rate_warning, etc.)")),
	), s.handleFlowDebug)

	s.mcpServer.AddTool(mcp.NewTool("kafka_flow_bottlenecks",
		mcp.WithDescription("Detect bottlenecks in the message flow: high consumer lag, low throughput, high error rates, and high latency. Returns chart-ready JSON with severity levels."),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
		mcp.WithString("severity", mcp.Description("Filter by severity: warning, critical")),
		mcp.WithString("type", mcp.Description("Filter by type: high_lag, low_throughput, high_error_rate, high_latency")),
	), s.handleFlowBottlenecks)
}
