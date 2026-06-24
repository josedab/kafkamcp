package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/auth"
)

func (s *Server) registerCoreObservabilityTools() {
	s.addTool(mcp.NewTool("kafka_health",
		withCategory("observability"),
		mcp.WithDescription("Run a read-only Kafka health check using live cluster metadata, topic discovery, and consumer-group discovery."),
		mcp.WithString("cluster", mcp.Description("Cluster name. Defaults to the configured default cluster.")),
	), s.withToolAuth(toolAuthConfig{
		tool: "kafka_health", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, s.handleCoreHealth))

	s.addTool(mcp.NewTool("kafka_metrics",
		withCategory("observability"),
		mcp.WithDescription("Return aggregated local KafkaMCP Prometheus metrics without exposing raw agent, topic, group, or cluster labels."),
	), s.withToolAuth(toolAuthConfig{
		tool: "kafka_metrics", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, s.handleCoreMetrics))
}

func (s *Server) handleCoreHealth(ctx context.Context, _ mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	cluster, _ := args["cluster"].(string)
	cluster = s.resolveClusterName(cluster)
	metadata, err := s.kafkaManager.GetClusterMetadata(ctx, cluster)
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_health", args, cluster, "error", 0, err, start)
		return s.toolError("failed to read cluster metadata", err), nil
	}
	topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_health", args, cluster, "error", 0, err, start)
		return s.toolError("failed to list topics", err), nil
	}
	groups, err := s.kafkaManager.ListConsumerGroups(ctx, cluster)
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_health", args, cluster, "error", 0, err, start)
		return s.toolError("failed to list consumer groups", err), nil
	}
	s.logAudit(ctx, agentID, "kafka_health", args, cluster, "success", len(topics), nil, start)
	return marshalResult(map[string]any{
		"status":          "healthy",
		"cluster":         cluster,
		"cluster_id":      metadata.ClusterID,
		"controller_id":   metadata.Controller.ID,
		"brokers":         len(metadata.Brokers),
		"topics":          len(topics),
		"consumer_groups": len(groups),
		"checked_at":      time.Now().UTC(),
		"latency_ms":      time.Since(start).Milliseconds(),
	})
}

func (s *Server) handleCoreMetrics(ctx context.Context, _ mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	families, err := s.metrics.Registry.Gather()
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_metrics", args, "", "error", 0, err, start)
		return s.toolError("failed to gather metrics", err), nil
	}
	result := make(map[string]any, len(families))
	for _, family := range families {
		result[family.GetName()] = aggregateMetricFamily(family)
	}
	s.logAudit(ctx, agentID, "kafka_metrics", args, "", "success", len(result), nil, start)
	return marshalResult(map[string]any{
		"metrics":    result,
		"families":   len(result),
		"aggregated": true,
	})
}
