package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerReplicationTools() {
	s.addTool(mcp.NewTool("kafka_replication_status",
		withCategory("streaming"),
		mcp.WithDescription("Get real-time cross-cluster replication status for MirrorMaker2/Confluent Replicator links. Shows active links, topic-level sync state, and recent alerts."),
		mcp.WithString("source_cluster", mcp.Description("Filter by source cluster")),
		mcp.WithString("target_cluster", mcp.Description("Filter by target cluster")),
	), s.handleReplicationStatusTool)

	s.addTool(mcp.NewTool("kafka_replication_lag",
		withCategory("streaming"),
		mcp.WithDescription("Get detailed partition-level replication lag for a specific replication link."),
		mcp.WithString("source_cluster", mcp.Required(), mcp.Description("Source cluster name")),
		mcp.WithString("target_cluster", mcp.Required(), mcp.Description("Target cluster name")),
		mcp.WithString("topic", mcp.Description("Filter by specific topic")),
	), s.handleReplicationLag)

	s.addTool(mcp.NewTool("kafka_failover_readiness",
		withCategory("streaming"),
		mcp.WithDescription("Evaluate failover readiness for a replication link. Provides readiness score (0-100), RPO/RTO estimates, and actionable recommendations."),
		mcp.WithString("source_cluster", mcp.Required(), mcp.Description("Source cluster name")),
		mcp.WithString("target_cluster", mcp.Required(), mcp.Description("Target cluster name")),
	), s.handleFailoverReadiness)
}
