package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerTopicTemplateTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_topic_template",
		withCategory("ops"),
		mcp.WithDescription("Manage declarative topic-as-code templates. Define topic settings (partitions, replication, retention, cleanup policy) as YAML/JSON templates with apply/diff/plan commands."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: parse, save, get, list, delete, diff, plan, export")),
		mcp.WithString("template", mcp.Description("Template content as YAML or JSON string (for parse/save)")),
		mcp.WithString("format", mcp.Description("Template format: yaml, json (default: json)")),
		mcp.WithString("name", mcp.Description("Template name (for get/delete/diff/plan/export)")),
		mcp.WithString("cluster", mcp.Description("Cluster name for diff/plan operations.")),
	), s.handleTopicTemplate)
}
