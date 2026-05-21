package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerMarketplaceTools() {
	s.addTool(mcp.NewTool("kafka_plugin_search",
		withCategory("ops"),
		mcp.WithDescription("Search the plugin marketplace for extensions. Find monitoring, security, analytics, integration, and utility plugins with ratings and verification status."),
		mcp.WithString("query", mcp.Description("Search query (matches name, description, tags)")),
		mcp.WithString("category", mcp.Description("Filter by category: monitoring, security, analytics, integration, utility")),
		mcp.WithBoolean("verified_only", mcp.Description("Only show verified plugins. Default: false")),
	), s.handlePluginSearch)

	s.addTool(mcp.NewTool("kafka_plugin_install",
		withCategory("ops"),
		withTier(TierExperimental),
		mcp.WithDescription("Install a plugin from the marketplace. Validates dependencies and security capabilities before installation."),
		mcp.WithString("name", mcp.Required(), mcp.Description("Plugin name to install")),
	), s.handlePluginInstall)

	s.addTool(mcp.NewTool("kafka_plugin_list",
		withCategory("ops"),
		mcp.WithDescription("List installed plugins with their status, version, provided tools, and resources."),
	), s.handlePluginList)
}
