package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/dagworkflow"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleDAGWorkflowCreate(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	name, _ := args["name"].(string)
	description, _ := args["description"].(string)

	if name == "" {
		return mcp.NewToolResultError("name is required"), nil
	}

	var steps []dagworkflow.DAGStep
	if stepsRaw, ok := args["steps"]; ok {
		stepsJSON, err := json.Marshal(stepsRaw)
		if err != nil {
			return mcp.NewToolResultError("invalid steps format"), nil
		}
		if err := json.Unmarshal(stepsJSON, &steps); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid steps: %s", err)), nil
		}
	}

	var params []dagworkflow.Parameter
	if paramsRaw, ok := args["parameters"]; ok {
		paramsJSON, err := json.Marshal(paramsRaw)
		if err != nil {
			return mcp.NewToolResultError("invalid parameters format"), nil
		}
		if err := json.Unmarshal(paramsJSON, &params); err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("invalid parameters: %s", err)), nil
		}
	}

	var tags []string
	if tagsRaw, ok := args["tags"]; ok {
		tagsJSON, err := json.Marshal(tagsRaw)
		if err == nil {
			json.Unmarshal(tagsJSON, &tags)
		}
	}

	wf, err := s.dagWorkflowEngine.CreateWorkflow(name, description, agentID, params, steps, tags)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	s.logAudit(agentID, "kafka_workflow_create", args, "", "success", 1, nil, start)
	return marshalResult(wf)
}

func (s *Server) handleDAGWorkflowRun(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	templateID, _ := args["template_id"].(string)
	if templateID == "" {
		return mcp.NewToolResultError("template_id is required"), nil
	}

	var params map[string]any
	if p, ok := args["parameters"].(map[string]any); ok {
		params = p
	}

	run, err := s.dagWorkflowEngine.StartRun(templateID, agentID, params)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}

	readySteps, _ := s.dagWorkflowEngine.GetReadySteps(run.ID)

	var readyInfo []map[string]any
	for _, step := range readySteps {
		resolvedArgs := s.dagWorkflowEngine.ResolveStepArgs(step, run.Parameters)
		readyInfo = append(readyInfo, map[string]any{
			"step_id": step.ID,
			"name":    step.Name,
			"tool":    step.Tool,
			"args":    resolvedArgs,
		})
	}

	s.logAudit(agentID, "kafka_workflow_run", args, "", "success", 1, nil, start)
	return marshalResult(map[string]any{
		"run":         run,
		"ready_steps": readyInfo,
	})
}

func (s *Server) handleDAGWorkflowStatus(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)

	switch action {
	case "status":
		runID, _ := args["run_id"].(string)
		if runID == "" {
			return mcp.NewToolResultError("run_id is required"), nil
		}
		run, err := s.dagWorkflowEngine.GetRun(runID)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		readySteps, _ := s.dagWorkflowEngine.GetReadySteps(runID)
		var readyIDs []string
		for _, s := range readySteps {
			readyIDs = append(readyIDs, s.ID)
		}
		s.logAudit(agentID, "kafka_workflow_status", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{
			"run":         run,
			"ready_steps": readyIDs,
		})

	case "list":
		templateID, _ := args["template_id"].(string)
		workflows := s.dagWorkflowEngine.ListWorkflows()
		runs := s.dagWorkflowEngine.ListRuns(templateID, 50)
		s.logAudit(agentID, "kafka_workflow_status", args, "", "success", len(workflows), nil, start)
		return marshalResult(map[string]any{
			"workflows": workflows,
			"runs":      runs,
		})

	case "versions":
		templateID, _ := args["template_id"].(string)
		if templateID == "" {
			return mcp.NewToolResultError("template_id is required for versions"), nil
		}
		// Look up template to get name
		tmpl, err := s.dagWorkflowEngine.GetWorkflow(templateID)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		versions, err := s.dagWorkflowEngine.GetVersions(tmpl.Name)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		s.logAudit(agentID, "kafka_workflow_status", args, "", "success", len(versions), nil, start)
		return marshalResult(map[string]any{"versions": versions})

	case "cancel":
		runID, _ := args["run_id"].(string)
		if runID == "" {
			return mcp.NewToolResultError("run_id is required"), nil
		}
		if err := s.dagWorkflowEngine.CancelRun(runID); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		s.logAudit(agentID, "kafka_workflow_status", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "cancelled"})

	case "complete_step":
		runID, _ := args["run_id"].(string)
		stepID, _ := args["step_id"].(string)
		stepStatus, _ := args["step_status"].(string)
		stepOutput := args["step_output"]
		stepError, _ := args["step_error"].(string)

		if runID == "" || stepID == "" || stepStatus == "" {
			return mcp.NewToolResultError("run_id, step_id, and step_status are required"), nil
		}

		if err := s.dagWorkflowEngine.CompleteStep(runID, stepID, stepStatus, stepOutput, stepError); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}

		readySteps, _ := s.dagWorkflowEngine.GetReadySteps(runID)
		run, _ := s.dagWorkflowEngine.GetRun(runID)

		var readyInfo []map[string]any
		if run != nil {
			for _, step := range readySteps {
				resolvedArgs := s.dagWorkflowEngine.ResolveStepArgs(step, run.Parameters)
				readyInfo = append(readyInfo, map[string]any{
					"step_id": step.ID,
					"name":    step.Name,
					"tool":    step.Tool,
					"args":    resolvedArgs,
				})
			}
		}

		s.logAudit(agentID, "kafka_workflow_status", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{
			"run":         run,
			"ready_steps": readyInfo,
		})

	case "ready_steps":
		runID, _ := args["run_id"].(string)
		if runID == "" {
			return mcp.NewToolResultError("run_id is required"), nil
		}
		run, err := s.dagWorkflowEngine.GetRun(runID)
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		readySteps, _ := s.dagWorkflowEngine.GetReadySteps(runID)
		var readyInfo []map[string]any
		for _, step := range readySteps {
			resolvedArgs := s.dagWorkflowEngine.ResolveStepArgs(step, run.Parameters)
			readyInfo = append(readyInfo, map[string]any{
				"step_id": step.ID,
				"name":    step.Name,
				"tool":    step.Tool,
				"args":    resolvedArgs,
			})
		}
		s.logAudit(agentID, "kafka_workflow_status", args, "", "success", len(readySteps), nil, start)
		return marshalResult(map[string]any{"ready_steps": readyInfo})

	default:
		return mcp.NewToolResultError("action must be: status, list, versions, cancel, complete_step, or ready_steps"), nil
	}
}
