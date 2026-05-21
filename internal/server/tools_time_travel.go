package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

// handleTimeTravel handles point-in-time topic queries.
func (s *Server) handleTimeTravel(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_time_travel", resourceType: auth.ResourceTopic, action: auth.ActionRead,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("topic", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")
		action := request.GetString("action", "snapshot")

		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}

		if err := s.acquireExpensiveOp(ctx); err != nil {
			return s.toolError("operation capacity exceeded", err), nil
		}
		defer s.releaseExpensiveOp()

		maxKeys := request.GetInt("max_keys", 10000)
		if maxKeys > 100000 {
			maxKeys = 100000
		}

		switch action {
		case "snapshot":
			ts := request.GetString("timestamp", "")
			if ts == "" {
				return mcp.NewToolResultError("timestamp is required for snapshot action"), nil
			}
			result, err := s.kafkaManager.TimeTravelSnapshot(ctx, cluster, topic, ts, maxKeys)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_time_travel", args, cluster, "error", 0, err, start)
				return s.toolError("Failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_time_travel", args, cluster, "success", result.KeyCount, nil, start)
			return marshalResult(result)

		case "diff":
			tsFrom := request.GetString("timestamp_from", "")
			tsTo := request.GetString("timestamp_to", "")
			if tsFrom == "" || tsTo == "" {
				return mcp.NewToolResultError("timestamp_from and timestamp_to are required for diff action"), nil
			}
			result, err := s.kafkaManager.TimeTravelDiff(ctx, cluster, topic, tsFrom, tsTo, maxKeys)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_time_travel", args, cluster, "error", 0, err, start)
				return s.toolError("Failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_time_travel", args, cluster, "success", result.TotalChanges, nil, start)
			return marshalResult(result)

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: snapshot, diff", action)), nil
		}
	})(ctx, request)
}
