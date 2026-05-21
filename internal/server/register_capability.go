package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerCapabilityTools() {
	s.addTool(mcp.NewTool("kafka_negotiate_capabilities",
		withCategory("discovery"),
		mcp.WithDescription("Negotiate agent capabilities to receive a contextual subset of available tools based on role, permissions, and conversation context. Reduces the full tool surface to 10-20 relevant tools."),
		mcp.WithString("role", mcp.Description("Agent role: viewer, operator, admin, analyst, sre, security. Default: viewer")),
		mcp.WithString("context", mcp.Description("Comma-separated context hints (e.g., 'consumer lag,incident triage')")),
	), s.handleNegotiateCapabilities)

	s.addTool(mcp.NewTool("kafka_discover_tools",
		withCategory("discovery"),
		mcp.WithDescription("Progressive disclosure meta-tool: discover additional tools in a specific category. Categories: discovery, monitoring, admin, streaming, schema, analytics, governance, orchestration, platform."),
		mcp.WithString("category", mcp.Required(), mcp.Description("Tool category to discover")),
	), s.handleDiscoverTools)

	s.addTool(mcp.NewTool("kafka_capability_profile",
		withCategory("discovery"),
		mcp.WithDescription("View the current agent's capability profile including allowed tools, role, and context."),
	), s.handleCapabilityProfile)
}
