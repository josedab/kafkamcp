package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerEventBusTools() {
	s.addTool(mcp.NewTool("kafka_event_bus",
		withCategory("observability"),
		mcp.WithDescription("Real-time agent observability bus. Subscribe to agent interactions, tool invocations, and Kafka operations as structured events. Supports filtering, backpressure, and historical replay."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: subscribe, unsubscribe, publish, replay, stats, list_subscriptions")),
		mcp.WithString("subscription_id", mcp.Description("Subscription ID (for unsubscribe)")),
		mcp.WithString("event_type", mcp.Description("Filter by event type: tool_invocation, tool_result, kafka_produce, kafka_consume, kafka_admin, policy_violation, anomaly, custom")),
		mcp.WithString("agent_filter", mcp.Description("Filter by agent ID")),
		mcp.WithString("tool_filter", mcp.Description("Filter by tool name")),
		mcp.WithString("cluster_filter", mcp.Description("Filter by cluster")),
		mcp.WithString("source_pattern", mcp.Description("Filter by source pattern (substring match)")),
		mcp.WithNumber("limit", mcp.Description("Max events for replay (default: 100)")),
		mcp.WithString("event_data", mcp.Description("JSON event data (for publish)")),
	), s.handleEventBus)
}
