package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerSandboxTools() {
	if s.sandboxEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_sandbox_create",
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

	s.mcpServer.AddTool(mcp.NewTool("kafka_sandbox_exec",
		mcp.WithDescription("Execute a Kafka tool within a sandbox. All operations are tracked with resource accounting."),
		mcp.WithString("sandbox_id", mcp.Required(), mcp.Description("Sandbox ID")),
		mcp.WithString("tool", mcp.Required(), mcp.Description("Tool to execute (e.g., kafka_create_topic, kafka_produce)")),
		mcp.WithObject("args", mcp.Description("Tool arguments")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_sandbox_exec",
		resourceType: "topic",
		action:       "write",
	}, s.handleSandboxExec))

	s.mcpServer.AddTool(mcp.NewTool("kafka_sandbox_destroy",
		mcp.WithDescription("Destroy a sandbox, list sandboxes, get details, or generate a promotion report."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: destroy, list, get, promotion_report")),
		mcp.WithString("sandbox_id", mcp.Description("Sandbox ID")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_sandbox_destroy",
		resourceType: "topic",
		action:       "write",
	}, s.handleSandboxManage))
}
