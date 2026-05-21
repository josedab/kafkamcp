package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/agentprofile"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerThreatDetectionTools() {
	if s.threatDetector == nil {
		return
	}

	s.addTool(mcp.NewTool("agent_threat_alerts",
		withCategory("security"),
		mcp.WithDescription("List recent behavioral threat alerts detected for agents (data exfiltration, privilege escalation, bulk reads)."),
		mcp.WithString("agent_id", mcp.Description("Filter by agent ID")),
		mcp.WithNumber("limit", mcp.Description("Max alerts to return. Default: 50")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_threat_alerts",
		resourceType: "cluster",
		action:       "read",
	}, s.handleThreatAlerts))

	s.addTool(mcp.NewTool("agent_threat_profile",
		withCategory("security"),
		mcp.WithDescription("Get the behavioral profile for an agent, including access patterns, topic usage, and baseline status."),
		mcp.WithString("agent_id", mcp.Required(), mcp.Description("Agent ID to profile")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_threat_profile",
		resourceType: "cluster",
		action:       "read",
	}, s.handleThreatProfile))

	s.addTool(mcp.NewTool("agent_threat_stats",
		withCategory("security"),
		mcp.WithDescription("Get threat detection system statistics including profile count and alert totals."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "agent_threat_stats",
		resourceType: "cluster",
		action:       "read",
	}, s.handleThreatStats))
}

func (s *Server) handleThreatAlerts(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	filterAgent, _ := args["agent_id"].(string)
	limit := 50
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	if limit > MaxDLQMessages {
		limit = MaxDLQMessages
	}

	alerts := s.threatDetector.GetAlerts(filterAgent, limit)
	s.logAudit(agentID, "agent_threat_alerts", args, "", "success", len(alerts), nil, start)
	return marshalResult(map[string]any{
		"alerts": alerts,
		"total":  len(alerts),
	})
}

func (s *Server) handleThreatProfile(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	targetAgent, _ := args["agent_id"].(string)
	if targetAgent == "" {
		return mcp.NewToolResultError("agent_id is required"), nil
	}

	profile, ok := s.threatDetector.GetProfile(targetAgent)
	if !ok {
		return marshalResult(map[string]any{
			"agent_id": targetAgent,
			"status":   "no_profile",
			"message":  "No behavioral profile exists for this agent yet",
		})
	}

	s.logAudit(agentID, "agent_threat_profile", args, "", "success", 1, nil, start)
	return marshalResult(profile)
}

func (s *Server) handleThreatStats(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	stats := s.threatDetector.Stats()
	s.logAudit(agentID, "agent_threat_stats", args, "", "success", 1, nil, start)
	return marshalResult(stats)
}

// recordThreatAccess is called by the audit logging system to feed the threat detector.
func (s *Server) recordThreatAccess(agentID, tool, topic, action string, count int) {
	if s.threatDetector == nil {
		return
	}
	s.threatDetector.RecordAccess(agentprofile.AccessEvent{
		AgentID:   agentID,
		Tool:      tool,
		Topic:     topic,
		Action:    action,
		Count:     count,
		Timestamp: time.Now(),
	})
}
