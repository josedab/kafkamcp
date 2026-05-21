package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerConfigVCSTools() {
	if s.configVCSEngine == nil {
		return
	}

	s.addTool(mcp.NewTool("kafka_config_history",
		withCategory("governance"),
		mcp.WithDescription("Get configuration change history for a resource with full diff details. Supports SOC2-ready audit reports."),
		mcp.WithString("resource", mcp.Required(), mcp.Description("Resource type: topic, acl, schema, policy, cluster")),
		mcp.WithString("resource_id", mcp.Required(), mcp.Description("Resource identifier (e.g., topic name)")),
		mcp.WithNumber("limit", mcp.Description("Max history entries (default: 50)")),
		mcp.WithBoolean("audit_report", mcp.Description("Generate a full audit report")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_config_history",
		resourceType: "topic",
		action:       "read",
	}, s.handleConfigHistory))

	s.addTool(mcp.NewTool("kafka_config_rollback",
		withCategory("governance"),
		mcp.WithDescription("Rollback a resource configuration to a previous version."),
		mcp.WithString("resource", mcp.Required(), mcp.Description("Resource type")),
		mcp.WithString("resource_id", mcp.Required(), mcp.Description("Resource identifier")),
		mcp.WithNumber("target_version", mcp.Required(), mcp.Description("Version number to rollback to")),
		mcp.WithString("reason", mcp.Description("Reason for rollback")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_config_rollback",
		resourceType: "topic",
		action:       "admin",
	}, s.handleConfigRollback))

	s.addTool(mcp.NewTool("kafka_config_compare",
		withCategory("governance"),
		mcp.WithDescription("Compare two versions of a resource configuration showing a unified diff."),
		mcp.WithString("resource", mcp.Required(), mcp.Description("Resource type")),
		mcp.WithString("resource_id", mcp.Required(), mcp.Description("Resource identifier")),
		mcp.WithNumber("version1", mcp.Required(), mcp.Description("First version number")),
		mcp.WithNumber("version2", mcp.Required(), mcp.Description("Second version number")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_config_compare",
		resourceType: "topic",
		action:       "read",
	}, s.handleConfigCompare))
}
