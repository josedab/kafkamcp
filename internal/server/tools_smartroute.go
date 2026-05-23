package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/smartroute"
)

func (s *Server) handleSmartRoute(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_smart_route",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		req := request.GetString("request", "")
		if req == "" {
			return mcp.NewToolResultError("request is required"), nil
		}
		if deny := validateStringLength("request", req); deny != nil {
			return deny, nil
		}

		plan, err := s.smartRouter.ClassifyAndPlan(req)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_smart_route", args, "", "error", 0, err, start)
			return s.toolError("Failed to create execution plan", err), nil
		}

		nextSteps, _ := s.smartRouter.GetNextSteps(plan.ID)

		s.logAudit(ctx, agentID, "kafka_smart_route", args, "", "success", len(plan.Steps), nil, start)
		return marshalResult(map[string]any{
			"plan":       plan,
			"next_steps": nextSteps,
			"message":    "Execution plan created. Use kafka_smart_route_status with action=next_steps to get executable steps, then complete_step to record results.",
		})
	})(ctx, request)
}

func (s *Server) handleSmartRouteStatus(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_smart_route_status",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		planID := request.GetString("plan_id", "")

		if deny := validateStringLength("plan_id", planID); deny != nil {
			return deny, nil
		}

		switch action {
		case "status":
			if planID == "" {
				return mcp.NewToolResultError("plan_id is required for status"), nil
			}
			plan, err := s.smartRouter.GetPlan(planID)
			if err != nil {
				return s.toolError("Plan not found", err), nil
			}
			result, _ := s.smartRouter.GetResult(planID)
			s.logAudit(ctx, agentID, "kafka_smart_route_status", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"plan": plan, "result": result})

		case "next_steps":
			if planID == "" {
				return mcp.NewToolResultError("plan_id is required for next_steps"), nil
			}
			steps, err := s.smartRouter.GetNextSteps(planID)
			if err != nil {
				return s.toolError("Failed to get next steps", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_smart_route_status", args, "", "success", len(steps), nil, start)
			return marshalResult(map[string]any{"next_steps": steps, "count": len(steps)})

		case "complete_step":
			if planID == "" {
				return mcp.NewToolResultError("plan_id is required for complete_step"), nil
			}
			stepID := request.GetString("step_id", "")
			stepStatus := request.GetString("step_status", "completed")
			errMsg := request.GetString("error", "")

			if stepID == "" {
				return mcp.NewToolResultError("step_id is required"), nil
			}
			if deny := validateStringLength("step_id", stepID); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("step_status", stepStatus); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("error", errMsg); deny != nil {
				return deny, nil
			}

			result := smartroute.StepResult{
				StepID: stepID,
				Status: stepStatus,
				Error:  errMsg,
			}
			if err := s.smartRouter.RecordStepResult(planID, result); err != nil {
				return s.toolError("Failed to record step result", err), nil
			}

			nextSteps, _ := s.smartRouter.GetNextSteps(planID)
			s.logAudit(ctx, agentID, "kafka_smart_route_status", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{
				"status":     "step_recorded",
				"next_steps": nextSteps,
				"next_count": len(nextSteps),
			})

		case "graph":
			graph := s.smartRouter.GetToolGraph()
			s.logAudit(ctx, agentID, "kafka_smart_route_status", args, "", "success", len(graph), nil, start)
			return marshalResult(map[string]any{"tool_graph": graph, "total": len(graph)})

		default:
			plans := s.smartRouter.ListPlans()
			s.logAudit(ctx, agentID, "kafka_smart_route_status", args, "", "success", len(plans), nil, start)
			return marshalResult(map[string]any{"plans": plans, "total": len(plans)})
		}
	})(ctx, request)
}
