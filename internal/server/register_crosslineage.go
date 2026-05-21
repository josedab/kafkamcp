package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerCrossLineageTools() {
	s.addTool(mcp.NewTool("kafka_cross_lineage",
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

	s.addTool(mcp.NewTool("kafka_lineage_impact",
		withCategory("analytics"),
		mcp.WithDescription("Analyze impact of changing or removing a node in the lineage graph. Shows direct downstream, transitive impact, affected clusters, and risk level."),
		mcp.WithString("node_id", mcp.Required(), mcp.Description("Node ID to analyze (e.g., 'topic:orders')")),
	), s.handleLineageImpact)

	s.addTool(mcp.NewTool("kafka_lineage_find_path",
		withCategory("analytics"),
		mcp.WithDescription("Find all data flow paths between two nodes in the lineage graph."),
		mcp.WithString("source_id", mcp.Required(), mcp.Description("Source node ID")),
		mcp.WithString("target_id", mcp.Required(), mcp.Description("Target node ID")),
		mcp.WithNumber("max_depth", mcp.Description("Max path depth. Default: 10")),
	), s.handleLineageFindPath)

	s.addTool(mcp.NewTool("kafka_lineage_cross_cluster",
		withCategory("analytics"),
		mcp.WithDescription("Identify data flows that cross cluster boundaries. Shows which data moves between clusters and through what paths."),
	), s.handleLineageCrossCluster)
}
