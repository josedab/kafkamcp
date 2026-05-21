package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerSmartRouteTools() {
	s.addTool(mcp.NewTool("kafka_smart_route",
		withCategory("orchestration"),
		mcp.WithDescription("Analyze a high-level request and automatically compose a multi-tool execution plan. E.g., 'investigate why orders are delayed' triggers lag diagnosis → anomaly detection → lineage trace."),
		mcp.WithString("request", mcp.Required(), mcp.Description("High-level natural language request to analyze")),
	), s.handleSmartRoute)

	s.addTool(mcp.NewTool("kafka_smart_route_status",
		withCategory("orchestration"),
		mcp.WithDescription("Get status of a smart route execution plan, list plans, get next executable steps, or record step results."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: status, next_steps, complete_step, list, graph")),
		mcp.WithString("plan_id", mcp.Description("Plan ID (required for status/next_steps/complete_step)")),
		mcp.WithString("step_id", mcp.Description("Step ID (required for complete_step)")),
		mcp.WithString("step_status", mcp.Description("Step result status: completed, failed, skipped (for complete_step)")),
		mcp.WithString("error", mcp.Description("Error message if step failed")),
	), s.handleSmartRouteStatus)
}
