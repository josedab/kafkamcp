package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/orchestration"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleOrchestrationCreate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_orchestration_create",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		description := request.GetString("description", "")
		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}

		// Create a simple single-step orchestration via MCP
		steps := []orchestration.OrchestrationStep{
			{ID: "step_1", Name: name + " - initial", Tool: "kafka_cluster_info", AssignTo: agentID},
		}

		orch, err := s.orchestrationEngine.CreateOrchestration(name, description, agentID, steps)
		if err != nil {
			s.logAudit(agentID, "kafka_orchestration_create", args, "", "error", 0, err, start)
			return s.toolError("Failed to create orchestration", err), nil
		}

		s.logAudit(agentID, "kafka_orchestration_create", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status":        "created",
			"orchestration": orch,
			"message":       fmt.Sprintf("Orchestration %q created with %d steps", name, len(steps)),
		})
	})(ctx, request)
}

func (s *Server) handleOrchestrationStatus(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_orchestration_status",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		orchID := request.GetString("orchestration_id", "")
		action := request.GetString("action", "status")

		if orchID == "" {
			orchs := s.orchestrationEngine.ListOrchestrations()
			s.logAudit(agentID, "kafka_orchestration_status", args, "", "success", len(orchs), nil, start)
			return marshalResult(map[string]any{"orchestrations": orchs, "total": len(orchs)})
		}

		switch action {
		case "ready_steps":
			steps, err := s.orchestrationEngine.GetReadySteps(orchID)
			if err != nil {
				return s.toolError("Failed to get ready steps", err), nil
			}
			s.logAudit(agentID, "kafka_orchestration_status", args, "", "success", len(steps), nil, start)
			return marshalResult(map[string]any{"ready_steps": steps, "count": len(steps)})

		case "context":
			ctx, err := s.orchestrationEngine.GetSharedContext(orchID)
			if err != nil {
				return s.toolError("Failed to get context", err), nil
			}
			s.logAudit(agentID, "kafka_orchestration_status", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"shared_context": ctx})

		default:
			orch, err := s.orchestrationEngine.GetOrchestration(orchID)
			if err != nil {
				return s.toolError("Orchestration not found", err), nil
			}
			s.logAudit(agentID, "kafka_orchestration_status", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"orchestration": orch})
		}
	})(ctx, request)
}

func (s *Server) handleOrchestrationCompleteStep(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_orchestration_complete_step",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		orchID := request.GetString("orchestration_id", "")
		stepID := request.GetString("step_id", "")
		status := request.GetString("status", "")
		errMsg := request.GetString("error", "")

		if orchID == "" || stepID == "" || status == "" {
			return mcp.NewToolResultError("orchestration_id, step_id, and status are required"), nil
		}

		err := s.orchestrationEngine.CompleteStep(orchID, stepID, agentID, status, nil, errMsg)
		if err != nil {
			s.logAudit(agentID, "kafka_orchestration_complete_step", args, "", "error", 0, err, start)
			return s.toolError("Failed to complete step", err), nil
		}

		// Get next ready steps
		ready, _ := s.orchestrationEngine.GetReadySteps(orchID)

		s.logAudit(agentID, "kafka_orchestration_complete_step", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status":      "step_completed",
			"step_id":     stepID,
			"next_ready":  ready,
			"ready_count": len(ready),
		})
	})(ctx, request)
}

func (s *Server) handleOrchestrationHandoff(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_orchestration_handoff",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		fromAgent := request.GetString("from_agent", "")
		toAgent := request.GetString("to_agent", "")
		message := request.GetString("message", "")

		if fromAgent == "" || toAgent == "" || message == "" {
			return mcp.NewToolResultError("from_agent, to_agent, and message are required"), nil
		}

		handoff, err := s.orchestrationEngine.CreateHandoff(fromAgent, toAgent, message, nil)
		if err != nil {
			s.logAudit(agentID, "kafka_orchestration_handoff", args, "", "error", 0, err, start)
			return s.toolError("Failed to create handoff", err), nil
		}

		s.logAudit(agentID, "kafka_orchestration_handoff", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"status": "handoff_created", "handoff": handoff})
	})(ctx, request)
}

func (s *Server) handleOrchestrationAgents(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_orchestration_agents",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")

		switch action {
		case "register":
			id := request.GetString("agent_id", agentID)
			name := request.GetString("name", id)
			role := orchestration.AgentRole(request.GetString("role", "operator"))

			agent, err := s.orchestrationEngine.RegisterAgent(id, name, role, nil)
			if err != nil {
				return s.toolError("Failed to register agent", err), nil
			}
			s.logAudit(agentID, "kafka_orchestration_agents", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "registered", "agent": agent})

		case "heartbeat":
			id := request.GetString("agent_id", agentID)
			if err := s.orchestrationEngine.AgentHeartbeat(id); err != nil {
				return s.toolError("Heartbeat failed", err), nil
			}
			s.logAudit(agentID, "kafka_orchestration_agents", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "ok", "agent_id": id})

		default:
			agents := s.orchestrationEngine.ListAgents()
			s.logAudit(agentID, "kafka_orchestration_agents", args, "", "success", len(agents), nil, start)
			return marshalResult(map[string]any{"agents": agents, "total": len(agents)})
		}
	})(ctx, request)
}
