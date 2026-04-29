package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleSessionReplay(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_session_replay", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		switch action {
		case "start":
			targetAgent := request.GetString("agent_id", agentID)
			session := s.sessionReplayEngine.StartSession(targetAgent, map[string]any{"started_by": agentID})
			s.logAudit(agentID, "kafka_session_replay", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "recording", "session": session})

		case "record":
			sessionID := request.GetString("session_id", "")
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			tool := request.GetString("tool", "")
			if tool == "" {
				return mcp.NewToolResultError("tool is required for record action"), nil
			}

			var callArgs map[string]any
			if rawArgs := request.GetRawArguments(); rawArgs != nil {
				if m, ok := rawArgs.(map[string]any); ok {
					if ca, ok := m["call_args"].(map[string]any); ok {
						callArgs = ca
					}
				}
			}
			var callResponse any
			if rawArgs := request.GetRawArguments(); rawArgs != nil {
				if m, ok := rawArgs.(map[string]any); ok {
					callResponse = m["call_response"]
				}
			}

			isError := request.GetBool("is_error", false)
			errorMsg := request.GetString("error_message", "")
			durationMs := request.GetFloat("duration_ms", 0)

			if err := s.sessionReplayEngine.RecordCall(sessionID, tool, callArgs, callResponse, isError, errorMsg, durationMs); err != nil {
				return s.toolError("Failed to record call", err), nil
			}
			return marshalResult(map[string]any{"status": "recorded", "session_id": sessionID, "tool": tool})

		case "end":
			sessionID := request.GetString("session_id", "")
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			if err := s.sessionReplayEngine.EndSession(sessionID); err != nil {
				return s.toolError("Failed to end session", err), nil
			}
			s.logAudit(agentID, "kafka_session_replay", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "completed", "session_id": sessionID})

		case "get":
			sessionID := request.GetString("session_id", "")
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			session, err := s.sessionReplayEngine.GetSession(sessionID)
			if err != nil {
				return s.toolError("Session not found", err), nil
			}
			return marshalResult(session)

		case "list":
			filterAgent := request.GetString("agent_id", "")
			limit := request.GetInt("limit", 50)
			sessions := s.sessionReplayEngine.ListSessions(filterAgent, limit)
			s.logAudit(agentID, "kafka_session_replay", args, "", "success", len(sessions), nil, start)
			return marshalResult(map[string]any{"sessions": sessions, "total": len(sessions)})

		case "replay":
			sessionID := request.GetString("session_id", "")
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			result, err := s.sessionReplayEngine.Replay(sessionID)
			if err != nil {
				return s.toolError("Failed to replay session", err), nil
			}
			s.logAudit(agentID, "kafka_session_replay", args, "", "success", result.TotalCalls, nil, start)
			return marshalResult(result)

		case "delete":
			sessionID := request.GetString("session_id", "")
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required"), nil
			}
			if err := s.sessionReplayEngine.DeleteSession(sessionID); err != nil {
				return s.toolError("Failed to delete session", err), nil
			}
			s.logAudit(agentID, "kafka_session_replay", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "deleted", "session_id": sessionID})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q, use: start, record, end, get, list, replay, delete", action)), nil
		}
	})(ctx, request)
}
