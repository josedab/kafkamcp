package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerAgentStoreTools() {
	s.addTool(mcp.NewTool("kafka_agent_config_search",
		withCategory("ops"),
		mcp.WithDescription("Search the agent configuration marketplace for pre-built agent configs, tool bundles, and integration setups."),
		mcp.WithString("query", mcp.Description("Search query (matches name, description, tags)")),
		mcp.WithString("category", mcp.Description("Filter by category: monitoring, operations, analytics, pipeline, security")),
		mcp.WithString("type", mcp.Description("Type to search: configs, bundles, or all. Default: all")),
		mcp.WithBoolean("verified_only", mcp.Description("Only show verified entries. Default: false")),
	), s.handleAgentConfigSearch)

	s.addTool(mcp.NewTool("kafka_agent_config_get",
		withCategory("ops"),
		mcp.WithDescription("Get details of a specific agent configuration or tool bundle from the marketplace."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Name of the agent config or tool bundle")),
		mcp.WithString("type", mcp.Description("Type: config or bundle. Default: config")),
	), s.handleAgentConfigGet)

	s.addTool(mcp.NewTool("kafka_agent_config_publish",
		withCategory("ops"),
		mcp.WithDescription("Publish an agent configuration or tool bundle to the marketplace."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Name of the agent config or tool bundle")),
		mcp.WithString("version", mcp.Required(), mcp.Description("Version string")),
		mcp.WithString("description", mcp.Required(), mcp.Description("Description of the config/bundle")),
		mcp.WithString("category", mcp.Description("Category: monitoring, operations, analytics, pipeline, security")),
		mcp.WithString("type", mcp.Description("Type: config or bundle. Default: config")),
	), s.handleAgentConfigPublish)
}
