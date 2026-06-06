package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerACLMgmtTools() {
	s.addTool(mcp.NewTool("kafka_acl_manage",
		withCategory("governance"),
		mcp.WithDescription("Inspect and reconcile live Kafka ACLs. Read-only actions (list, audit, diff, dry_run) read live broker ACLs against a caller-supplied desired state. Reconciling actions (apply, reconcile) create missing and delete obsolete exact ACLs on the broker and require an approval grant. Stateless: no desired state is stored server-side."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, audit, diff, dry_run, apply, reconcile")),
		mcp.WithString("principal", mcp.Description("Principal filter or default principal for supplied rules (e.g. User:app1)")),
		mcp.WithString("resource_type", mcp.Description("Resource type filter for list/audit: topic, group, cluster, transactional_id")),
		mcp.WithString("cluster", mcp.Description("Cluster name for multi-cluster setups.")),
		mcp.WithObject("rules", mcp.Description("Desired ACL rules array (for diff, dry_run, apply, reconcile). Each rule: {principal, host, resource_type, resource_name, pattern_type, operation, permission}")),
	), s.handleACLManage)
}
