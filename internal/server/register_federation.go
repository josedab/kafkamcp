package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerFederationTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_federation_join",
		withCategory("streaming"),
		mcp.WithDescription("Manage federation membership. Actions: join (register instance), leave (deregister), heartbeat (update status)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: join, leave, heartbeat")),
		mcp.WithString("instance_id", mcp.Description("Instance ID")),
		mcp.WithString("name", mcp.Description("Instance name (for join)")),
		mcp.WithString("endpoint", mcp.Description("Instance endpoint URL (for join)")),
		mcp.WithObject("capabilities", mcp.Description("Instance capabilities list")),
		mcp.WithObject("clusters", mcp.Description("Cluster names managed by this instance")),
	), s.handleFederationJoin)

	s.mcpServer.AddTool(mcp.NewTool("kafka_federation_query",
		withCategory("streaming"),
		mcp.WithDescription("Execute a query across all federated instances. Routes tool calls to matching instances and aggregates results."),
		mcp.WithString("tool", mcp.Required(), mcp.Description("Tool name to execute across federation")),
		mcp.WithObject("args", mcp.Description("Tool arguments")),
		mcp.WithObject("targets", mcp.Description("Target instance IDs (empty = all)")),
	), s.handleFederationQuery)

	s.mcpServer.AddTool(mcp.NewTool("kafka_federation_status",
		withCategory("streaming"),
		mcp.WithDescription("Get federation status: instances, policies, health. Actions: status (overview), instances (list), policies (list), audit (audit log)."),
		mcp.WithString("action", mcp.Description("Action: status, instances, policies, audit. Default: status")),
		mcp.WithNumber("limit", mcp.Description("Limit for audit log. Default: 50")),
	), s.handleFederationStatus)
}
