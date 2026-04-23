package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

// handleRecommend provides topic optimization recommendations.
func (s *Server) handleRecommend(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_recommend", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")

		result, err := s.kafkaManager.GetRecommendations(ctx, cluster, topic)
		if err != nil {
			s.logAudit(agentID, "kafka_recommend", args, cluster, "error", 0, err, start)
			return s.toolError("Failed", err), nil
		}

		s.logAudit(agentID, "kafka_recommend", args, cluster, "success", len(result.Recommendations), nil, start)
		return marshalResult(result)
	})(ctx, request)
}
