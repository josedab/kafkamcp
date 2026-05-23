package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

func (s *Server) handleAgentProfile(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_agent_profile", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		targetAgent := request.GetString("agent_id", agentID)

		if deny := validateStringLength("agent_id", targetAgent); deny != nil {
			return deny, nil
		}

		sinceHours := 24.0
		if h, ok := args["since_hours"].(float64); ok && h > 0 {
			sinceHours = h
		}

		switch action {
		case "profile":
			profile := s.agentProfiler.ProfileAgent(targetAgent, sinceHours)
			s.logAudit(ctx, agentID, "kafka_agent_profile", args, "", "success", 1, nil, start)
			return marshalResult(profile)

		case "slow_queries":
			thresholdMs := 1000.0
			if t, ok := args["threshold_ms"].(float64); ok && t > 0 {
				thresholdMs = t
			}
			slow := s.agentProfiler.GetSlowQueries(targetAgent, thresholdMs)
			s.logAudit(ctx, agentID, "kafka_agent_profile", args, "", "success", len(slow), nil, start)
			return marshalResult(map[string]any{"slow_queries": slow, "total": len(slow), "threshold_ms": thresholdMs})

		case "list_agents":
			agents := s.agentProfiler.ListAgents()
			s.logAudit(ctx, agentID, "kafka_agent_profile", args, "", "success", len(agents), nil, start)
			return marshalResult(map[string]any{"agents": agents, "total": len(agents), "total_calls": s.agentProfiler.GetCallCount()})

		default:
			return mcp.NewToolResultError("action must be: profile, slow_queries, list_agents"), nil
		}
	})(ctx, request)
}

func (s *Server) handleAgentOptimize(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_agent_optimize", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		targetAgent := request.GetString("agent_id", agentID)
		if deny := validateStringLength("agent_id", targetAgent); deny != nil {
			return deny, nil
		}
		sinceHours := 24.0
		if h, ok := args["since_hours"].(float64); ok && h > 0 {
			sinceHours = h
		}

		profile := s.agentProfiler.ProfileAgent(targetAgent, sinceHours)
		s.logAudit(ctx, agentID, "kafka_agent_optimize", args, "", "success", len(profile.Optimizations), nil, start)
		return marshalResult(map[string]any{
			"agent_id":         targetAgent,
			"efficiency_score": profile.EfficiencyScore,
			"optimizations":    profile.Optimizations,
			"redundant_calls":  profile.RedundantCalls,
			"total_calls":      profile.TotalCalls,
			"error_rate":       profile.ErrorRate,
		})
	})(ctx, request)
}

func (s *Server) handleAgentReport(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_agent_report", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		targetAgent := request.GetString("agent_id", "")
		if deny := validateStringLength("agent_id", targetAgent); deny != nil {
			return deny, nil
		}
		sinceHours := 24.0
		if h, ok := args["since_hours"].(float64); ok && h > 0 {
			sinceHours = h
		}

		if targetAgent != "" {
			profile := s.agentProfiler.ProfileAgent(targetAgent, sinceHours)
			s.logAudit(ctx, agentID, "kafka_agent_report", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{
				"agent_id":     targetAgent,
				"profile":      profile,
				"period_hours": sinceHours,
			})
		}

		// Report across all agents
		agents := s.agentProfiler.ListAgents()
		var profiles []map[string]any
		for _, a := range agents {
			p := s.agentProfiler.ProfileAgent(a, sinceHours)
			profiles = append(profiles, map[string]any{
				"agent_id":         a,
				"total_calls":      p.TotalCalls,
				"avg_latency_ms":   p.AvgLatencyMs,
				"error_rate":       p.ErrorRate,
				"efficiency_score": p.EfficiencyScore,
			})
		}

		s.logAudit(ctx, agentID, "kafka_agent_report", args, "", "success", len(profiles), nil, start)
		return marshalResult(map[string]any{
			"agents":       profiles,
			"total_agents": len(profiles),
			"total_calls":  s.agentProfiler.GetCallCount(),
			"period_hours": sinceHours,
		})
	})(ctx, request)
}
