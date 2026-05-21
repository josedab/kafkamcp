package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

// handleWorkflows manages workflow template operations.
func (s *Server) handleWorkflows(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_workflows",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		category := request.GetString("category", "")
		templateName := request.GetString("template", "")

		if deny := validateStringLength("action", action); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("category", category); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("template", templateName); deny != nil {
			return deny, nil
		}

		switch action {
		case "list":
			templates := s.workflowEngine.ListTemplates(category)
			s.logAudit(ctx, agentID, "kafka_workflows", args, "", "success", len(templates), nil, start)
			return marshalResult(map[string]any{"templates": templates, "total_count": len(templates)})

		case "describe":
			if templateName == "" {
				return mcp.NewToolResultError("template name is required for describe"), nil
			}
			tmpl, err := s.workflowEngine.GetTemplate(templateName)
			if err != nil {
				return s.toolError("workflow template not found", err), nil
			}
			return marshalResult(tmpl)

		case "execute":
			if templateName == "" {
				return mcp.NewToolResultError("template name is required for execute"), nil
			}
			params := make(map[string]any)
			if p, ok := args["params"].(map[string]any); ok {
				params = p
			}
			// Also check for common params passed directly
			for _, key := range []string{"topic", "group_id", "subject", "change", "correlation_id", "source_cluster", "target_cluster"} {
				if v := request.GetString(key, ""); v != "" {
					if deny := validateStringLength(key, v); deny != nil {
						return deny, nil
					}
					params[key] = v
				}
			}

			exec, err := s.workflowEngine.StartExecution(templateName, params)
			if err != nil {
				return s.toolError("workflow execution failed", err), nil
			}

			tmpl, err := s.workflowEngine.GetTemplate(templateName)
			if err != nil {
				s.logger.Warn("failed to get workflow template for response", "template", templateName, "error", err)
			}
			s.logAudit(ctx, agentID, "kafka_workflows", args, "", "success", exec.TotalSteps, nil, start)
			return marshalResult(map[string]any{
				"execution":   exec,
				"next_steps":  tmpl.Steps,
				"instruction": "Execute each step using the specified tool with the resolved arguments. Report results back via kafka_workflow_step.",
			})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; use: list, describe, execute", action)), nil
		}
	})(ctx, request)
}
