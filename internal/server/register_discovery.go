package server

import (
	mcp "github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerDiscoveryTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_marketplace_discover",
		withCategory("marketplace"),
		mcp.WithDescription("Discover plugins, agent configs, and tool bundles across the marketplace. Unified search across all item types with category filtering."),
		mcp.WithString("query", mcp.Description("Search query")),
		mcp.WithString("category", mcp.Description("Filter by category: monitoring, security, analytics, integration, utility, operations, pipeline")),
	), s.handleMarketplaceDiscover)

	s.mcpServer.AddTool(mcp.NewTool("kafka_marketplace_stats",
		withCategory("marketplace"),
		mcp.WithDescription("Get marketplace statistics including total items, categories, top rated, and featured items."),
	), s.handleMarketplaceStats)

	s.mcpServer.AddTool(mcp.NewTool("kafka_marketplace_review",
		withCategory("marketplace"),
		mcp.WithDescription("Add or view reviews for marketplace items (plugins, agent configs, tool bundles)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: add, list")),
		mcp.WithString("item_name", mcp.Required(), mcp.Description("Item name to review")),
		mcp.WithString("item_type", mcp.Description("Item type: plugin, agent_config, tool_bundle. Default: plugin")),
		mcp.WithNumber("rating", mcp.Description("Rating 1.0-5.0 (required for add)")),
		mcp.WithString("title", mcp.Description("Review title")),
		mcp.WithString("body", mcp.Description("Review body text")),
	), s.handleMarketplaceReview)
}
