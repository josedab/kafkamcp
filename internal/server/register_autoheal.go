package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerAutoHealTools() {
	s.addTool(mcp.NewTool("kafka_group_heal",
		withCategory("ops"),
		mcp.WithDescription("Auto-heal consumer groups: diagnose root cause (stuck partition, slow consumer, GC pressure, rebalance storm), generate remediation playbook, and optionally apply fixes. Combines health scoring with rule-based diagnosis."),
		mcp.WithString("group_id", mcp.Required(), mcp.Description("Consumer group ID")),
		mcp.WithBoolean("auto_execute", mcp.Description("Auto-execute safe remediation steps. Default: false")),
		mcp.WithString("cluster", mcp.Description("Cluster name.")),
	), s.handleGroupHeal)
}
