package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerCrossLineageTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_cross_lineage",
		withCategory("analytics"),
		mcp.WithDescription("Cross-topic data lineage tracking and visualization. Maps how data flows between topics through streams applications, connectors, and consumers."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: register_node, add_edge, graph, trace, remove_node, remove_edge")),
		mcp.WithString("name", mcp.Description("Node name (for register_node, trace)")),
		mcp.WithString("node_type", mcp.Description("Node type: topic, connector, stream_app, consumer_group, external (for register_node)")),
		mcp.WithString("source_id", mcp.Description("Source node ID (for add_edge)")),
		mcp.WithString("target_id", mcp.Description("Target node ID (for add_edge)")),
		mcp.WithString("flow_type", mcp.Description("Flow type: produces, consumes, transforms, replicates (for add_edge)")),
		mcp.WithString("label", mcp.Description("Edge label (for add_edge)")),
		mcp.WithString("id", mcp.Description("Node or edge ID (for remove_node, remove_edge)")),
		mcp.WithNumber("max_depth", mcp.Description("Max lineage trace depth (for trace, default: 5)")),
	), s.handleCrossLineage)
}
