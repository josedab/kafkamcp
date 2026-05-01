package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/eventsourcing"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerEventSourcingTools() {
	if s.eventSourcingEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("agent_action_replay",
		withCategory("observability"),
		mcp.WithDescription("Replay an agent session showing all tool calls, parameters, results, and timing."),
		mcp.WithString("session_id", mcp.Required(), mcp.Description("Agent session ID to replay")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_action_replay",
		resourceType: "cluster",
		action:       "read",
	}, s.handleActionReplay))

	s.mcpServer.AddTool(mcp.NewTool("agent_action_analytics",
		withCategory("observability"),
		mcp.WithDescription("Get tool usage analytics including call counts, error rates, latency percentiles, and unique agent counts."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_action_analytics",
		resourceType: "cluster",
		action:       "read",
	}, s.handleActionAnalytics))

	s.mcpServer.AddTool(mcp.NewTool("agent_action_export",
		withCategory("observability"),
		mcp.WithDescription("Export agent actions as JSONL for LLM fine-tuning on domain-specific Kafka operations."),
		mcp.WithString("agent_id", mcp.Description("Filter by agent ID")),
		mcp.WithString("tool", mcp.Description("Filter by tool name")),
		mcp.WithString("status", mcp.Description("Filter by status: success or error")),
		mcp.WithNumber("limit", mcp.Description("Max events. Default: 10000")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_action_export",
		resourceType: "cluster",
		action:       "read",
	}, s.handleActionExport))

	s.mcpServer.AddTool(mcp.NewTool("agent_action_stats",
		withCategory("observability"),
		mcp.WithDescription("Get event sourcing engine statistics."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_action_stats",
		resourceType: "cluster",
		action:       "read",
	}, s.handleActionStats))
}

func (s *Server) handleActionReplay(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	sessionID, _ := args["session_id"].(string)
	if sessionID == "" {
		return mcp.NewToolResultError("session_id is required"), nil
	}

	session, ok := s.eventSourcingEngine.GetSession(sessionID)
	if !ok {
		return marshalResult(map[string]any{
			"session_id": sessionID,
			"status":     "not_found",
		})
	}

	s.logAudit(agentID, "agent_action_replay", args, "", "success", session.TotalActions, nil, start)
	return marshalResult(session)
}

func (s *Server) handleActionAnalytics(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	usage := s.eventSourcingEngine.GetToolUsage()
	s.logAudit(agentID, "agent_action_analytics", args, "", "success", len(usage), nil, start)
	return marshalResult(map[string]any{
		"tool_usage": usage,
		"total_tools": len(usage),
	})
}

func (s *Server) handleActionExport(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	filter := eventsourcing.ExportFilter{
		Limit: 10000,
	}
	if v, ok := args["agent_id"].(string); ok {
		filter.AgentID = v
	}
	if v, ok := args["tool"].(string); ok {
		filter.Tool = v
	}
	if v, ok := args["status"].(string); ok {
		filter.Status = v
	}
	if v, ok := args["limit"].(float64); ok && v > 0 {
		filter.Limit = int(v)
	}
	if filter.Limit > MaxDLQMessages {
		filter.Limit = MaxDLQMessages
	}

	lines, err := s.eventSourcingEngine.ExportJSONL(filter)
	if err != nil {
		s.logAudit(agentID, "agent_action_export", args, "", "error", 0, err, start)
		return s.toolError("failed to export actions", err), nil
	}

	s.logAudit(agentID, "agent_action_export", args, "", "success", len(lines), nil, start)
	return marshalResult(map[string]any{
		"lines":  len(lines),
		"format": "jsonl",
	})
}

func (s *Server) handleActionStats(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	stats := s.eventSourcingEngine.Stats()
	s.logAudit(agentID, "agent_action_stats", args, "", "success", 1, nil, start)
	return marshalResult(stats)
}
