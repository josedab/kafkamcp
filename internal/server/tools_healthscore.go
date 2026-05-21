package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/healthscore"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleHealthScore(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_health_score", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")
		action := request.GetString("action", "evaluate")

		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		switch action {
		case "evaluate":
			input := healthscore.ClusterMetricsInput{}

			meta, err := s.kafkaManager.GetClusterMetadata(ctx, cluster)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_health_score", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to get cluster metadata", err), nil
			}

			input.TotalBrokers = len(meta.Brokers)
			input.OnlineBrokers = len(meta.Brokers)
			input.TotalPartitions = meta.PartitionCount
			input.ActiveControllerCount = 1
			if meta.Controller.ID < 0 {
				input.ActiveControllerCount = 0
			}

			report := s.healthScoreEngine.Evaluate(cluster, input)

			s.logAudit(ctx, agentID, "kafka_health_score", args, cluster, "success", 1, nil, start)
			return marshalResult(report)

		case "history":
			limit := int(clampInt64(int64(request.GetInt("history_limit", 20)), 1, 1000))
			history := s.healthScoreEngine.GetHistory(cluster, limit)

			s.logAudit(ctx, agentID, "kafka_health_score", args, cluster, "success", len(history), nil, start)
			return marshalResult(map[string]any{"cluster": cluster, "history": history, "total": len(history)})

		default:
			return mcp.NewToolResultError("action must be: evaluate, history"), nil
		}
	})(ctx, request)
}
