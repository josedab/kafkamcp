package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/digitaltwin"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleTwinPlayground(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_twin_playground", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		switch action {
		case "create":
			clusterName := request.GetString("cluster_name", "playground")
			brokerCount := 3
			if b, ok := args["brokers"].(float64); ok && b > 0 {
				brokerCount = int(b)
			}
			topicDefs := make(map[string]int)
			if topicsMap, ok := args["topics"].(map[string]any); ok {
				for name, val := range topicsMap {
					partitions := 3
					if p, ok := val.(float64); ok {
						partitions = int(p)
					}
					topicDefs[name] = partitions
				}
			}
			if len(topicDefs) == 0 {
				topicDefs["example-topic"] = 3
			}
			twin, err := s.digitalTwin.CreatePlayground(clusterName, topicDefs, brokerCount)
			if err != nil {
				return s.toolError("failed to create playground", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_twin_playground", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"twin": twin, "message": "Playground created. Use twin_id for simulation."})

		case "run_scenario":
			twinID := request.GetString("twin_id", "")
			scenarioID := request.GetString("scenario_id", "")
			if twinID == "" || scenarioID == "" {
				return mcp.NewToolResultError("twin_id and scenario_id are required"), nil
			}
			var params map[string]any
			if p, ok := args["params"].(map[string]any); ok {
				params = p
			}
			result, err := s.digitalTwin.RunScenario(twinID, scenarioID, params)
			if err != nil {
				return s.toolError("failed to run scenario", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_twin_playground", args, "", "success", 1, nil, start)
			return marshalResult(result)

		case "analyze_impact":
			twinID := request.GetString("twin_id", "")
			if twinID == "" {
				return mcp.NewToolResultError("twin_id is required"), nil
			}
			rawOps, ok := args["operations"].([]any)
			if !ok || len(rawOps) == 0 {
				return mcp.NewToolResultError("operations array is required"), nil
			}
			var ops []digitaltwin.SimulationOp
			for _, raw := range rawOps {
				m, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				op := digitaltwin.SimulationOp{
					Type: stringFromMap(m, "type"),
				}
				if p, ok := m["params"].(map[string]any); ok {
					op.Params = p
				}
				ops = append(ops, op)
			}
			analysis, err := s.digitalTwin.AnalyzeImpact(twinID, ops)
			if err != nil {
				return s.toolError("failed to analyze impact", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_twin_playground", args, "", "success", 1, nil, start)
			return marshalResult(analysis)

		case "list_scenarios":
			category := request.GetString("category", "")
			scenarios := s.digitalTwin.GetScenarios(category)
			s.logAudit(ctx, agentID, "kafka_twin_playground", args, "", "success", len(scenarios), nil, start)
			return marshalResult(map[string]any{"scenarios": scenarios, "total": len(scenarios)})

		case "reset":
			twinID := request.GetString("twin_id", "")
			if twinID == "" {
				return mcp.NewToolResultError("twin_id is required"), nil
			}
			twin, err := s.digitalTwin.ResetTwin(twinID)
			if err != nil {
				return s.toolError("failed to reset twin", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_twin_playground", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"twin": twin, "message": "Twin reset to snapshot state"})

		default:
			return mcp.NewToolResultError("action must be: create, run_scenario, analyze_impact, list_scenarios, reset"), nil
		}
	})(ctx, request)
}
