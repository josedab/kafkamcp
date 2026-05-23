package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

func (s *Server) handleRunbook(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_runbook", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		id := request.GetString("id", "")

		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}
		if deny := validateStringLength("id", id); deny != nil {
			return deny, nil
		}

		switch action {
		case "list":
			category := request.GetString("category", "")
			if deny := validateStringLength("category", category); deny != nil {
				return deny, nil
			}
			runbooks := s.runbookEngine.ListRunbooks(category)
			s.logAudit(ctx, agentID, "kafka_runbook", args, "", "success", len(runbooks), nil, start)
			return marshalResult(map[string]any{"runbooks": runbooks, "total": len(runbooks)})

		case "get":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			rb, err := s.runbookEngine.GetRunbook(id)
			if err != nil {
				return s.toolError("Runbook not found", err), nil
			}
			return marshalResult(rb)

		case "create":
			definition := request.GetString("definition", "")
			if definition == "" {
				return mcp.NewToolResultError("definition is required"), nil
			}
			if deny := validateStringLength("definition", definition); deny != nil {
				return deny, nil
			}
			rb, err := s.runbookEngine.CreateRunbook(definition, agentID)
			if err != nil {
				return s.toolError("Failed to create runbook", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_runbook", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "created", "runbook": rb})

		case "execute":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			params := extractStringMapArg(request, "params")
			exec, err := s.runbookEngine.Execute(id, agentID, params)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_runbook", args, "", "error", 0, err, start)
				return s.toolError("Failed to execute runbook", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_runbook", args, "", "success", len(exec.Steps), nil, start)
			return marshalResult(map[string]any{"status": "executing", "execution": exec})

		case "status":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			exec, err := s.runbookEngine.GetExecution(id)
			if err != nil {
				return s.toolError("Execution not found", err), nil
			}
			return marshalResult(exec)

		case "complete_step":
			if id == "" {
				return mcp.NewToolResultError("id is required (execution ID)"), nil
			}
			stepIndex := request.GetInt("step_index", -1)
			if stepIndex < 0 {
				return mcp.NewToolResultError("step_index is required"), nil
			}
			stepIndex = int(clampInt64(int64(stepIndex), 0, 10000))
			stepStatus := request.GetString("step_status", "completed")
			stepError := request.GetString("step_error", "")
			stepOutput := request.GetString("step_output", "")

			if deny := validateStringLength("step_status", stepStatus); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("step_error", stepError); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("step_output", stepOutput); deny != nil {
				return deny, nil
			}

			var output any
			if stepOutput != "" {
				output = stepOutput
			}

			if err := s.runbookEngine.CompleteStep(id, stepIndex, stepStatus, output, stepError); err != nil {
				return s.toolError("Failed to complete step", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_runbook", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "step_completed", "execution_id": id, "step_index": stepIndex})

		case "executions":
			runbookID := id
			execs := s.runbookEngine.ListExecutions(runbookID, 50)
			return marshalResult(map[string]any{"executions": execs, "total": len(execs)})

		case "enable":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			if err := s.runbookEngine.EnableRunbook(id, true); err != nil {
				return s.toolError("Failed to enable runbook", err), nil
			}
			return marshalResult(map[string]any{"status": "enabled", "id": id})

		case "disable":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			if err := s.runbookEngine.EnableRunbook(id, false); err != nil {
				return s.toolError("Failed to disable runbook", err), nil
			}
			return marshalResult(map[string]any{"status": "disabled", "id": id})

		case "delete":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			if err := s.runbookEngine.DeleteRunbook(id); err != nil {
				return s.toolError("Failed to delete runbook", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_runbook", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "deleted", "id": id})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q", action)), nil
		}
	})(ctx, request)
}
