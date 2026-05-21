package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/dagworkflow"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleMultiAgentOrchestrate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_multi_agent_orchestrate", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		runID := request.GetString("run_id", "")
		if runID == "" {
			return mcp.NewToolResultError("run_id is required"), nil
		}

		switch action {
		case "assign":
			rawAssignments, ok := args["assignments"].([]any)
			if !ok || len(rawAssignments) == 0 {
				return mcp.NewToolResultError("assignments array is required"), nil
			}
			var assignments []dagworkflow.AgentAssignment
			for _, raw := range rawAssignments {
				m, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				a := dagworkflow.AgentAssignment{
					StepID:    stringFromMap(m, "step_id"),
					AgentID:   stringFromMap(m, "agent_id"),
					AgentRole: stringFromMap(m, "agent_role"),
				}
				assignments = append(assignments, a)
			}
			mar, err := s.dagWorkflowEngine.AssignSteps(runID, assignments)
			if err != nil {
				return s.toolError("failed to assign steps", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_multi_agent_orchestrate", args, "", "success", len(assignments), nil, start)
			return marshalResult(mar)

		case "handoff":
			fromAgent := request.GetString("from_agent", "")
			toAgent := request.GetString("to_agent", "")
			stepID := request.GetString("step_id", "")
			data := args["data"]
			handoff, err := s.dagWorkflowEngine.RecordHandoff(runID, fromAgent, toAgent, stepID, data)
			if err != nil {
				return s.toolError("failed to record handoff", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_multi_agent_orchestrate", args, "", "success", 1, nil, start)
			return marshalResult(handoff)

		case "agent_steps":
			targetAgent := request.GetString("agent_id", agentID)
			steps, err := s.dagWorkflowEngine.GetAgentSteps(runID, targetAgent)
			if err != nil {
				return s.toolError("failed to get agent steps", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_multi_agent_orchestrate", args, "", "success", len(steps), nil, start)
			return marshalResult(map[string]any{"agent_id": targetAgent, "steps": steps, "total": len(steps)})

		case "agent_ready":
			targetAgent := request.GetString("agent_id", agentID)
			steps, err := s.dagWorkflowEngine.GetReadyAgentSteps(runID, targetAgent)
			if err != nil {
				return s.toolError("failed to get ready steps", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_multi_agent_orchestrate", args, "", "success", len(steps), nil, start)
			return marshalResult(map[string]any{"agent_id": targetAgent, "ready_steps": steps, "total": len(steps)})

		case "status":
			mar, err := s.dagWorkflowEngine.GetMultiAgentStatus(runID)
			if err != nil {
				return s.toolError("failed to get status", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_multi_agent_orchestrate", args, "", "success", 1, nil, start)
			return marshalResult(mar)

		default:
			return mcp.NewToolResultError("action must be: assign, handoff, agent_steps, agent_ready, status"), nil
		}
	})(ctx, request)
}

func stringFromMap(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}
