package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerOnboardingTools() {
	if s.onboardingEngine == nil {
		return
	}

	s.addTool(mcp.NewTool("kafka_discover_clusters",
		withCategory("ops"),
		mcp.WithDescription("Auto-discover Kafka clusters by scanning common ports, environment variables, and DNS records."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_discover_clusters",
		resourceType: "cluster",
		action:       "read",
	}, s.handleDiscoverClusters))
}

func (s *Server) handleDiscoverClusters(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	result := s.onboardingEngine.DiscoverClusters(ctx)
	s.logAudit(ctx, agentID, "kafka_discover_clusters", args, "", "success", len(result.Clusters), nil, start)
	return marshalResult(result)
}
