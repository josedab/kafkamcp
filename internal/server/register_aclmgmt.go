package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerACLMgmtTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_acl_manage",
		withCategory("governance"),
		mcp.WithDescription("Manage Kafka ACLs — list, audit, diff, and dry-run access control entries for topics, consumer groups, and cluster resources."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, audit, set_desired, diff, dry_run, principals")),
		mcp.WithString("principal", mcp.Description("Principal filter or target (e.g., User:app1)")),
		mcp.WithString("resource_type", mcp.Description("Resource type filter: topic, group, cluster")),
		mcp.WithString("cluster", mcp.Description("Cluster name for multi-cluster setups.")),
		mcp.WithObject("rules", mcp.Description("ACL rules array (for set_desired). Each rule: {resource_type, resource_name, operation, permission}")),
	), s.handleACLManage)
}
