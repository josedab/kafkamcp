package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleAgenticRunbook(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_agentic_runbook", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}
		id := request.GetString("id", "")
		if deny := validateStringLength("id", id); deny != nil {
			return deny, nil
		}

		switch action {
		case "list":
			category := request.GetString("category", "")
			runbooks := s.runbookEngine.ListAgenticRunbooks(category)
			s.logAudit(agentID, "kafka_agentic_runbook", args, "", "success", len(runbooks), nil, start)
			return marshalResult(map[string]any{"runbooks": runbooks, "total": len(runbooks)})

		case "get":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			rb, err := s.runbookEngine.GetAgenticRunbook(id)
			if err != nil {
				return s.toolError("Agentic runbook not found", err), nil
			}
			return marshalResult(rb)

		case "create":
			definition := request.GetString("definition", "")
			if definition == "" {
				return mcp.NewToolResultError("definition (YAML) is required"), nil
			}
			if deny := validateStringLength("definition", definition); deny != nil {
				return deny, nil
			}
			rb, err := s.runbookEngine.CreateAgenticRunbook(definition, agentID)
			if err != nil {
				return s.toolError("Failed to create agentic runbook", err), nil
			}
			s.logAudit(agentID, "kafka_agentic_runbook", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "created", "runbook": rb})

		case "execute":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			params := extractStringMapArg(request, "params")
			dryRun := request.GetBool("dry_run", false)
			exec, err := s.runbookEngine.ExecuteAgentic(id, agentID, params, dryRun)
			if err != nil {
				s.logAudit(agentID, "kafka_agentic_runbook", args, "", "error", 0, err, start)
				return s.toolError("Failed to execute agentic runbook", err), nil
			}
			s.logAudit(agentID, "kafka_agentic_runbook", args, "", "success", len(exec.Steps), nil, start)
			return marshalResult(map[string]any{"status": exec.Status, "execution": exec})

		case "status":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			exec, err := s.runbookEngine.GetAgenticExecution(id)
			if err != nil {
				return s.toolError("Execution not found", err), nil
			}
			return marshalResult(exec)

		case "approve":
			if id == "" {
				return mcp.NewToolResultError("id (execution ID) is required"), nil
			}
			approver := request.GetString("approver", agentID)
			if err := s.runbookEngine.ApproveStep(id, approver); err != nil {
				return s.toolError("Failed to approve step", err), nil
			}
			s.logAudit(agentID, "kafka_agentic_runbook", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "approved", "execution_id": id})

		case "reject":
			if id == "" {
				return mcp.NewToolResultError("id (execution ID) is required"), nil
			}
			rejector := request.GetString("approver", agentID)
			reason := request.GetString("reason", "no reason provided")
			if err := s.runbookEngine.RejectStep(id, rejector, reason); err != nil {
				return s.toolError("Failed to reject step", err), nil
			}
			s.logAudit(agentID, "kafka_agentic_runbook", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "rejected", "execution_id": id})

		case "rollback":
			if id == "" {
				return mcp.NewToolResultError("id (execution ID) is required"), nil
			}
			exec, err := s.runbookEngine.RollbackExecution(id)
			if err != nil {
				return s.toolError("Failed to rollback", err), nil
			}
			s.logAudit(agentID, "kafka_agentic_runbook", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "rolling_back", "execution": exec})

		case "resume":
			if id == "" {
				return mcp.NewToolResultError("id (execution ID) is required"), nil
			}
			if err := s.runbookEngine.ResumeExecution(id); err != nil {
				return s.toolError("Failed to resume execution", err), nil
			}
			return marshalResult(map[string]any{"status": "resumed", "execution_id": id})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action: %s", action)), nil
		}
	})(ctx, request)
}
