package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/perfguard"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handlePerfGuard(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_perf_guard", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		targetAgent := request.GetString("agent_id", agentID)

		switch action {
		case "record":
			metric := perfguard.MetricName(request.GetString("metric", ""))
			if metric == "" {
				return mcp.NewToolResultError("metric is required"), nil
			}
			value := request.GetFloat("value", 0)
			reg := s.perfGuard.RecordSample(perfguard.Sample{
				AgentID: targetAgent, Metric: metric, Value: value,
			})
			result := map[string]any{"status": "recorded", "agent_id": targetAgent, "metric": metric, "value": value}
			if reg != nil {
				result["regression"] = reg
			}
			return marshalResult(result)

		case "baseline":
			metric := perfguard.MetricName(request.GetString("metric", ""))
			if metric == "" {
				return mcp.NewToolResultError("metric is required"), nil
			}
			bl := s.perfGuard.GetBaseline(targetAgent, metric)
			if bl == nil {
				return marshalResult(map[string]any{"status": "no_baseline", "message": "Not enough samples to compute baseline"})
			}
			return marshalResult(bl)

		case "regressions":
			limit := int(request.GetInt("limit", 50))
			regs := s.perfGuard.ListRegressions(targetAgent, limit)
			return marshalResult(map[string]any{"regressions": regs, "total": len(regs)})

		case "throttle_status":
			ts := s.perfGuard.GetThrottleStatus(targetAgent)
			if ts == nil {
				return marshalResult(map[string]any{"throttled": false, "agent_id": targetAgent})
			}
			return marshalResult(ts)

		case "unthrottle":
			if err := s.perfGuard.UnthrottleAgent(targetAgent); err != nil {
				return s.toolError("Failed to unthrottle", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_perf_guard", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "unthrottled", "agent_id": targetAgent})

		case "reset_baseline":
			metric := perfguard.MetricName(request.GetString("metric", ""))
			if metric == "" {
				return mcp.NewToolResultError("metric is required"), nil
			}
			s.perfGuard.ResetBaseline(targetAgent, metric)
			return marshalResult(map[string]any{"status": "baseline_reset", "agent_id": targetAgent, "metric": metric})

		case "stats":
			return marshalResult(s.perfGuard.Stats())

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action: %s", action)), nil
		}
	})(ctx, request)
}
