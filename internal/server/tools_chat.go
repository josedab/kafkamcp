package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleChatSession(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_chat_session",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		sessionID := request.GetString("session_id", "")

		switch action {
		case "create":
			session := s.chatEngine.CreateSession(agentID)
			s.logAudit(agentID, "kafka_chat_session", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{
				"status":     "created",
				"session_id": session.ID,
				"agent_id":   session.AgentID,
			})

		case "get":
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required for get action"), nil
			}
			session, err := s.chatEngine.GetSession(sessionID)
			if err != nil {
				return s.toolError("Session not found", err), nil
			}
			s.logAudit(agentID, "kafka_chat_session", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{
				"session":    session,
				"turn_count": len(session.Turns),
			})

		case "close":
			if sessionID == "" {
				return mcp.NewToolResultError("session_id is required for close action"), nil
			}
			if err := s.chatEngine.CloseSession(sessionID); err != nil {
				return s.toolError("Failed to close session", err), nil
			}
			s.logAudit(agentID, "kafka_chat_session", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "closed", "session_id": sessionID})

		default:
			sessions := s.chatEngine.ListSessions(agentID)
			s.logAudit(agentID, "kafka_chat_session", args, "", "success", len(sessions), nil, start)
			return marshalResult(map[string]any{"sessions": sessions, "total": len(sessions)})
		}
	})(ctx, request)
}

func (s *Server) handleChatMessage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_chat_message",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		sessionID := request.GetString("session_id", "")
		message := request.GetString("message", "")

		if sessionID == "" || message == "" {
			return mcp.NewToolResultError("session_id and message are required"), nil
		}

		// Classify intent with context awareness
		intent := s.chatEngine.ClassifyIntent(sessionID, message)

		// Resolve references from session context
		resolved, _ := s.chatEngine.ResolveReference(sessionID, message)

		// Map intent to suggested tools
		suggestedTools := mapIntentToTools(intent.Type)

		// Merge resolved entities into intent entities
		for k, v := range resolved {
			intent.Entities[k] = v
		}

		// Record the turn
		entities := make(map[string]any)
		for k, v := range intent.Entities {
			entities[k] = v
		}

		turn, err := s.chatEngine.AddTurn(sessionID, message, intent, nil, "", entities)
		if err != nil {
			return s.toolError("Failed to add turn", err), nil
		}

		s.logAudit(agentID, "kafka_chat_message", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"turn":            turn,
			"intent":          intent,
			"resolved_refs":   resolved,
			"suggested_tools": suggestedTools,
		})
	})(ctx, request)
}

func (s *Server) handleChatContext(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_chat_context",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		sessionID := request.GetString("session_id", "")
		if sessionID == "" {
			return mcp.NewToolResultError("session_id is required"), nil
		}

		sessionCtx, err := s.chatEngine.GetSessionContext(sessionID)
		if err != nil {
			return s.toolError("Failed to get context", err), nil
		}

		s.logAudit(agentID, "kafka_chat_context", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"context": sessionCtx, "session_id": sessionID})
	})(ctx, request)
}

func (s *Server) handleChatTemplate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_chat_template",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "list")
		templateID := request.GetString("template_id", "")
		category := request.GetString("category", "")

		switch action {
		case "get":
			if templateID == "" {
				return mcp.NewToolResultError("template_id is required for get action"), nil
			}
			tmpl, err := s.chatEngine.GetTemplate(templateID)
			if err != nil {
				return s.toolError("Template not found", err), nil
			}
			s.logAudit(agentID, "kafka_chat_template", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"template": tmpl})

		default:
			templates := s.chatEngine.ListTemplates(category)
			s.logAudit(agentID, "kafka_chat_template", args, "", "success", len(templates), nil, start)
			return marshalResult(map[string]any{"templates": templates, "total": len(templates)})
		}
	})(ctx, request)
}

func mapIntentToTools(intentType string) []string {
	toolMap := map[string][]string{
		"lag_diagnosis":    {"kafka_consumer_groups", "kafka_lag_diagnosis", "kafka_selfheal_status"},
		"anomaly_detection": {"kafka_anomaly_detect", "kafka_anomaly_status"},
		"schema_inspection": {"kafka_list_schemas", "kafka_schema_versions", "kafka_schema_compatibility"},
		"topic_inspection":  {"kafka_list_topics", "kafka_topic_describe", "kafka_topic_config"},
		"group_inspection":  {"kafka_consumer_groups", "kafka_consumer_group_describe"},
		"produce_message":   {"kafka_produce"},
		"consume_message":   {"kafka_consume"},
		"lineage_tracking":  {"kafka_lineage_trace", "kafka_lineage_graph"},
		"capacity_planning": {"kafka_forecast_topic", "kafka_capacity_plan"},
		"incident_triage":   {"kafka_cluster_info", "kafka_anomaly_detect", "kafka_consumer_groups", "kafka_lag_diagnosis"},
		"cost_analysis":     {"kafka_cost_analyze", "kafka_cost_attribution"},
	}
	if tools, ok := toolMap[intentType]; ok {
		return tools
	}
	return []string{"kafka_cluster_info", "kafka_list_topics"}
}
