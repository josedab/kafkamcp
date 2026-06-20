package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerCapabilityTools() {
	s.addTool(mcp.NewTool("kafka_discover_tools",
		withCategory("discovery"),
		mcp.WithDescription("Discover the active provider, tool packs, and registered tools. Optionally filter legacy capability metadata by category."),
		mcp.WithString("category", mcp.Description("Optional tool category to discover")),
	), s.handleDiscoverTools)
}
