package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/chaos"
)

func (s *Server) handleChaosInject(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_chaos_inject", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("action", action); deny != nil {
			return deny, nil
		}

		switch action {
		case "inject":
			expType := request.GetString("type", "")
			if expType == "" {
				return mcp.NewToolResultError("type is required for inject action"), nil
			}

			var topics []string
			if t, ok := args["topics"].([]any); ok {
				for _, v := range t {
					if s, ok := v.(string); ok {
						topics = append(topics, s)
					}
				}
			}

			var groups []string
			if g, ok := args["groups"].([]any); ok {
				for _, v := range g {
					if s, ok := v.(string); ok {
						groups = append(groups, s)
					}
				}
			}

			var params map[string]any
			if p, ok := args["parameters"].(map[string]any); ok {
				params = p
			}

			maxDur := 10 * time.Minute
			if m, ok := args["max_duration_minutes"].(float64); ok && m > 0 {
				maxDur = time.Duration(m) * time.Minute
			}
			if maxDur > 60*time.Minute {
				maxDur = 60 * time.Minute
			}

			guard := chaos.DefaultSafetyGuard()
			guard.MaxDuration = maxDur

			exp, err := s.chaosEngine.Inject(chaos.ExperimentType(expType), cluster, topics, groups, params, &guard)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_chaos_inject", args, cluster, "error", 0, err, start)
				return s.toolError("failed to inject chaos", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_chaos_inject", args, cluster, "success", 1, nil, start)
			return marshalResult(exp)

		case "scenario":
			scenarioName := request.GetString("scenario", "")
			if scenarioName == "" {
				return mcp.NewToolResultError("scenario name is required"), nil
			}

			sc, err := s.chaosEngine.GetScenario(scenarioName)
			if err != nil {
				return s.toolError("scenario not found", err), nil
			}

			var topics []string
			if t, ok := args["topics"].([]any); ok {
				for _, v := range t {
					if s, ok := v.(string); ok {
						topics = append(topics, s)
					}
				}
			}

			exp, err := s.chaosEngine.Inject(sc.Type, cluster, topics, nil, sc.Parameters, &sc.SafetyGuard)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_chaos_inject", args, cluster, "error", 0, err, start)
				return s.toolError("failed to run scenario", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_chaos_inject", args, cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{"scenario": sc.Name, "experiment": exp})

		case "list_scenarios":
			category := request.GetString("category", "")
			scenarios := s.chaosEngine.ListScenarios(category)
			s.logAudit(ctx, agentID, "kafka_chaos_inject", args, cluster, "success", len(scenarios), nil, start)
			return marshalResult(map[string]any{"scenarios": scenarios, "total": len(scenarios)})

		default:
			return mcp.NewToolResultError("action must be: inject, scenario, list_scenarios"), nil
		}
	})(ctx, request)
}

func (s *Server) handleChaosStatus(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_chaos_status", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		expID := request.GetString("experiment_id", "")

		if expID != "" {
			exp, err := s.chaosEngine.Status(expID)
			if err != nil {
				return s.toolError("experiment not found", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_chaos_status", args, "", "success", 1, nil, start)
			return marshalResult(exp)
		}

		statusFilter := request.GetString("status_filter", "")
		experiments := s.chaosEngine.ListExperiments(statusFilter)
		s.logAudit(ctx, agentID, "kafka_chaos_status", args, "", "success", len(experiments), nil, start)
		return marshalResult(map[string]any{"experiments": experiments, "total": len(experiments)})
	})(ctx, request)
}

func (s *Server) handleChaosStop(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_chaos_stop", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		expID := request.GetString("experiment_id", "")
		if expID == "" {
			return mcp.NewToolResultError("experiment_id is required"), nil
		}
		if deny := validateStringLength("experiment_id", expID); deny != nil {
			return deny, nil
		}

		exp, err := s.chaosEngine.Stop(expID)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_chaos_stop", args, "", "error", 0, err, start)
			return s.toolError("failed to stop experiment", err), nil
		}
		s.logAudit(ctx, agentID, "kafka_chaos_stop", args, "", "success", 1, nil, start)
		return marshalResult(exp)
	})(ctx, request)
}
