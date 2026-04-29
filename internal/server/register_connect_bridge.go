package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerConnectBridgeTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_connect_bridge",
		withCategory("streaming"),
		mcp.WithDescription("Unified Kafka Connect MCP bridge. Manage connectors through a single tool: list, describe, create, update, pause, resume, restart, delete, and health check."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, describe, create, update, pause, resume, restart, delete, health")),
		mcp.WithString("name", mcp.Description("Connector name (required for all actions except list)")),
		mcp.WithObject("config", mcp.Description("Connector configuration map (for create/update)")),
		mcp.WithNumber("task_id", mcp.Description("Specific task ID to restart (-1 for all, default: -1)")),
		mcp.WithBoolean("failed_only", mcp.Description("Only restart failed tasks (for restart action)")),
		mcp.WithString("cluster", mcp.Description("Cluster name for multi-cluster setups.")),
	), s.handleConnectBridge)
}
