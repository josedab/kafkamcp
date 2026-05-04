package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleToolCompose(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_tool_compose", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}
		id := request.GetString("id", "")

		switch action {
		case "list":
			category := request.GetString("category", "")
			comps := s.toolComposeEngine.List(category)
			return marshalResult(map[string]any{"compositions": comps, "total": len(comps)})

		case "get":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			comp, err := s.toolComposeEngine.Get(id)
			if err != nil {
				return s.toolError("Composition not found", err), nil
			}
			return marshalResult(comp)

		case "create":
			def := request.GetString("definition", "")
			if def == "" {
				return mcp.NewToolResultError("definition (YAML) is required"), nil
			}
			comp, err := s.toolComposeEngine.Create(def, agentID)
			if err != nil {
				return s.toolError("Failed to create composition", err), nil
			}
			s.logAudit(agentID, "kafka_tool_compose", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "created", "composition": comp})

		case "execute":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			params := extractAnyMapArg(request, "params")
			exec, err := s.toolComposeEngine.Execute(id, agentID, params)
			if err != nil {
				return s.toolError("Failed to execute composition", err), nil
			}
			s.logAudit(agentID, "kafka_tool_compose", args, "", "success", len(exec.Steps), nil, start)
			return marshalResult(map[string]any{"status": exec.Status, "execution": exec})

		case "status":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			exec, err := s.toolComposeEngine.GetExecution(id)
			if err != nil {
				return s.toolError("Execution not found", err), nil
			}
			return marshalResult(exec)

		case "delete":
			if id == "" {
				return mcp.NewToolResultError("id is required"), nil
			}
			if err := s.toolComposeEngine.Delete(id); err != nil {
				return s.toolError("Failed to delete composition", err), nil
			}
			return marshalResult(map[string]any{"status": "deleted"})

		case "search":
			query := request.GetString("query", "")
			if query == "" {
				return mcp.NewToolResultError("query is required"), nil
			}
			results := s.toolComposeEngine.Search(query)
			return marshalResult(map[string]any{"results": results, "total": len(results)})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action: %s", action)), nil
		}
	})(ctx, request)
}

func extractAnyMapArg(request mcp.CallToolRequest, key string) map[string]any {
	rawArgs := request.GetRawArguments()
	m, ok := rawArgs.(map[string]any)
	if !ok {
		return nil
	}
	cfgRaw, ok := m[key]
	if !ok {
		return nil
	}
	if v, ok := cfgRaw.(map[string]any); ok {
		return v
	}
	return nil
}
