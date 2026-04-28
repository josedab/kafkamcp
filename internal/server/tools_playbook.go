package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/playbook"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handlePlaybookList(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_playbook_list",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		playbooks := s.playbookEngine.ListPlaybooks()

		triggerType := request.GetString("trigger_type", "")
		if triggerType != "" {
			var filtered []playbook.Playbook
			for _, pb := range playbooks {
				if string(pb.Trigger.Type) == triggerType {
					filtered = append(filtered, pb)
				}
			}
			playbooks = filtered
		}

		s.logAudit(agentID, "kafka_playbook_list", args, "", "success", len(playbooks), nil, start)
		return marshalResult(map[string]any{"playbooks": playbooks, "total": len(playbooks)})
	})(ctx, request)
}

func (s *Server) handlePlaybookExecute(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_playbook_execute",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		playbookID := request.GetString("playbook_id", "")
		if playbookID == "" {
			return mcp.NewToolResultError("playbook_id is required"), nil
		}
		if deny := validateStringLength("playbook_id", playbookID); deny != nil {
			return deny, nil
		}

		exec, err := s.playbookEngine.Execute(playbookID, agentID, map[string]any{"triggered_by": agentID})
		if err != nil {
			s.logAudit(agentID, "kafka_playbook_execute", args, "", "error", 0, err, start)
			return s.toolError("Failed to execute playbook", err), nil
		}

		s.logAudit(agentID, "kafka_playbook_execute", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status":    "executing",
			"execution": exec,
			"message":   fmt.Sprintf("Playbook execution %s started with %d steps", exec.ID, len(exec.Steps)),
		})
	})(ctx, request)
}

func (s *Server) handlePlaybookMatch(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_playbook_match",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		triggerType := request.GetString("trigger_type", "")
		value := request.GetFloat("value", 0)

		matches := s.playbookEngine.MatchTrigger(playbook.TriggerType(triggerType), value)

		s.logAudit(agentID, "kafka_playbook_match", args, "", "success", len(matches), nil, start)
		return marshalResult(map[string]any{
			"matches":      matches,
			"total":        len(matches),
			"trigger_type": triggerType,
			"value":        value,
		})
	})(ctx, request)
}

func (s *Server) handlePlaybookManage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_playbook_manage",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		id := request.GetString("id", "")
		if action == "" || id == "" {
			return mcp.NewToolResultError("action and id are required"), nil
		}

		switch action {
		case "get":
			pb, err := s.playbookEngine.GetPlaybook(id)
			if err != nil {
				return s.toolError("Playbook not found", err), nil
			}
			return marshalResult(map[string]any{"playbook": pb})

		case "enable":
			if err := s.playbookEngine.EnablePlaybook(id, true); err != nil {
				return s.toolError("Failed to enable", err), nil
			}
			return marshalResult(map[string]any{"status": "enabled", "id": id})

		case "disable":
			if err := s.playbookEngine.EnablePlaybook(id, false); err != nil {
				return s.toolError("Failed to disable", err), nil
			}
			return marshalResult(map[string]any{"status": "disabled", "id": id})

		case "delete":
			if err := s.playbookEngine.DeletePlaybook(id); err != nil {
				return s.toolError("Failed to delete", err), nil
			}
			return marshalResult(map[string]any{"status": "deleted", "id": id})

		case "executions":
			execs := s.playbookEngine.ListExecutions(id, 50)
			return marshalResult(map[string]any{"executions": execs, "total": len(execs)})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q", action)), nil
		}
	})(ctx, request)
}
