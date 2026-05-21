package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerSandboxTools() {
	if s.sandboxEngine == nil {
		return
	}

	s.addTool(mcp.NewTool("kafka_sandbox_create",
		withCategory("ops"),
		mcp.WithDescription("Create an isolated sandbox environment for safe experimentation with Kafka operations. Sandboxes have resource quotas and auto-expire."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Sandbox name")),
		mcp.WithString("description", mcp.Description("Sandbox description")),
		mcp.WithNumber("duration_sec", mcp.Description("Sandbox duration in seconds (default: 3600, max: 86400)")),
		mcp.WithObject("quotas", mcp.Description("Resource quotas: {max_topics, max_partitions, max_messages, max_storage_bytes}")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_sandbox_create",
		resourceType: "topic",
		action:       "write",
	}, s.handleSandboxCreate))

	s.addTool(mcp.NewTool("kafka_sandbox_exec",
		withCategory("ops"),
		mcp.WithDescription("Record a sandbox operation against the in-process resource ledger. NOTE: this tool does NOT actually invoke another Kafka tool in an isolated context — it only registers the intended call and returns a 'simulated' status so callers can build dry-run dashboards. Real per-tool isolation/quotas are not implemented."),
		mcp.WithString("sandbox_id", mcp.Required(), mcp.Description("Sandbox ID")),
		mcp.WithString("tool", mcp.Required(), mcp.Description("Tool to execute (e.g., kafka_create_topic, kafka_produce)")),
		mcp.WithObject("args", mcp.Description("Tool arguments")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_sandbox_exec",
		resourceType: "topic",
		action:       "write",
	}, s.handleSandboxExec))

	s.addTool(mcp.NewTool("kafka_sandbox_destroy",
		withCategory("ops"),
		mcp.WithDescription("Destroy a sandbox, list sandboxes, get details, or generate a promotion report."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: destroy, list, get, promotion_report")),
		mcp.WithString("sandbox_id", mcp.Description("Sandbox ID")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_sandbox_destroy",
		resourceType: "topic",
		action:       "write",
	}, s.handleSandboxManage))
}
