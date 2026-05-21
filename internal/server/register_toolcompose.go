package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerToolComposeTools() {
	s.addTool(mcp.NewTool("kafka_tool_compose",
		withCategory("platform"),
		mcp.WithDescription("MCP tool composition engine. Define composite tools that chain multiple atomic MCP tools into reusable workflows. Supports DAG validation, parallel branches, and a shareable registry."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: list, get, create, execute, status, delete, search")),
		mcp.WithString("id", mcp.Description("Composition ID or execution ID")),
		mcp.WithString("definition", mcp.Description("Composition definition as YAML (for create)")),
		mcp.WithObject("params", mcp.Description("Parameters for execution")),
		mcp.WithString("category", mcp.Description("Filter by category (for list)")),
		mcp.WithString("query", mcp.Description("Search query (for search)")),
	), s.handleToolCompose)
}
