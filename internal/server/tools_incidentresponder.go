package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

func (s *Server) handleIncidentRespond(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_incident_respond", resourceType: auth.ResourceCluster, action: auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "respond")
		incidentID := request.GetString("incident_id", "")
		runbookID := request.GetString("runbook_id", "")

		switch action {
		case "respond":
			if incidentID == "" {
				return mcp.NewToolResultError("incident_id is required"), nil
			}
			result, err := s.incidentCorrelator.Respond(incidentID)
			if err != nil {
				return s.toolError("incident response failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_incident_respond", args, "", "success", 1, nil, start)
			return marshalResult(result)

		case "hypothesize":
			if incidentID == "" {
				return mcp.NewToolResultError("incident_id is required"), nil
			}
			hypotheses, err := s.incidentCorrelator.GenerateHypotheses(incidentID)
			if err != nil {
				return s.toolError("hypothesis generation failed", err), nil
			}
			return marshalResult(map[string]any{"hypotheses": hypotheses, "total": len(hypotheses)})

		case "generate_runbook":
			if incidentID == "" {
				return mcp.NewToolResultError("incident_id is required"), nil
			}
			runbook, err := s.incidentCorrelator.GenerateRunbook(incidentID)
			if err != nil {
				return s.toolError("runbook generation failed", err), nil
			}
			return marshalResult(map[string]any{
				"runbook": runbook,
				"message": "Runbook generated. Use action=approve_runbook to approve, then execute_runbook to run.",
			})

		case "approve_runbook":
			if runbookID == "" {
				return mcp.NewToolResultError("runbook_id is required"), nil
			}
			runbook, err := s.incidentCorrelator.ApproveRunbook(runbookID, agentID)
			if err != nil {
				return s.toolError("approval failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_incident_respond", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"runbook": runbook, "message": "Runbook approved. Use action=execute_runbook to run."})

		case "execute_runbook":
			if runbookID == "" {
				return mcp.NewToolResultError("runbook_id is required"), nil
			}
			logs, err := s.incidentCorrelator.ExecuteRunbook(runbookID)
			if err != nil {
				return s.toolError("execution failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_incident_respond", args, "", "success", len(logs), nil, start)
			return marshalResult(map[string]any{"execution_logs": logs, "total_actions": len(logs)})

		case "get_runbook":
			if runbookID == "" {
				return mcp.NewToolResultError("runbook_id is required"), nil
			}
			runbook, err := s.incidentCorrelator.GetRunbook(runbookID)
			if err != nil {
				return s.toolError("runbook not found", err), nil
			}
			return marshalResult(runbook)

		default:
			return mcp.NewToolResultError("action must be: respond, hypothesize, generate_runbook, approve_runbook, execute_runbook, get_runbook"), nil
		}
	})(ctx, request)
}
