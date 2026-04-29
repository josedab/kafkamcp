package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/retention"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerRetentionTools() {
	if s.retentionEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_retention_policy",
		withCategory("governance"),
		mcp.WithDescription("Get or set the intelligent retention policy for a topic, including compliance constraints (GDPR, HIPAA, SOX)."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithNumber("retention_ms", mcp.Description("Current retention in milliseconds (for setting policy)")),
		mcp.WithString("compliance", mcp.Description("Compliance framework: GDPR, HIPAA, SOX, PCI_DSS, none")),
		mcp.WithString("criticality", mcp.Description("Topic criticality: low, medium, high, critical")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_retention_policy",
		resourceType: "topic",
		action:       "write",
	}, s.handleRetentionPolicy))

	s.mcpServer.AddTool(mcp.NewTool("kafka_retention_recommendations",
		withCategory("governance"),
		mcp.WithDescription("Get AI-powered retention recommendations based on usage patterns, compliance, and cost analysis."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_retention_recommendations",
		resourceType: "cluster",
		action:       "read",
	}, s.handleRetentionRecommendations))

	s.mcpServer.AddTool(mcp.NewTool("kafka_retention_change_request",
		withCategory("governance"),
		mcp.WithDescription("Request a retention change with approval workflow. Reducing retention requires approval and grace period."),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic name")),
		mcp.WithNumber("target_retention_ms", mcp.Required(), mcp.Description("Target retention in milliseconds")),
		mcp.WithString("reason", mcp.Required(), mcp.Description("Justification for the change")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_retention_change_request",
		resourceType: "topic",
		action:       "write",
	}, s.handleRetentionChangeRequest))

	s.mcpServer.AddTool(mcp.NewTool("kafka_retention_approve",
		withCategory("governance"),
		mcp.WithDescription("Approve a pending retention change request."),
		mcp.WithString("request_id", mcp.Required(), mcp.Description("Change request ID")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_retention_approve",
		resourceType: "cluster",
		action:       "write",
	}, s.handleRetentionApprove))
}

func (s *Server) handleRetentionPolicy(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic, _ := args["topic"].(string)
	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}

	// If retention_ms provided, set policy
	if retMs, ok := args["retention_ms"].(float64); ok {
		compliance := retention.ComplianceNone
		if c, ok := args["compliance"].(string); ok && c != "" {
			compliance = retention.ComplianceFramework(c)
		}
		criticality, _ := args["criticality"].(string)

		err := s.retentionEngine.SetPolicy(retention.RetentionPolicy{
			Topic:              topic,
			CurrentRetentionMs: int64(retMs),
			Compliance:         compliance,
			CriticalityTag:     criticality,
		})
		if err != nil {
			s.logAudit(agentID, "kafka_retention_policy", args, "", "error", 0, err, start)
			return s.toolError("failed to set retention policy", err), nil
		}
		s.logAudit(agentID, "kafka_retention_policy", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{"status": "policy_set", "topic": topic})
	}

	// Get policy
	policy, ok := s.retentionEngine.GetPolicy(topic)
	if !ok {
		return marshalResult(map[string]any{"topic": topic, "status": "no_policy"})
	}
	s.logAudit(agentID, "kafka_retention_policy", args, "", "success", 1, nil, start)
	return marshalResult(policy)
}

func (s *Server) handleRetentionRecommendations(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	recs := s.retentionEngine.GetRecommendations()
	s.logAudit(agentID, "kafka_retention_recommendations", args, "", "success", len(recs), nil, start)
	return marshalResult(map[string]any{
		"recommendations": recs,
		"total":           len(recs),
	})
}

func (s *Server) handleRetentionChangeRequest(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topic, _ := args["topic"].(string)
	targetMs, _ := args["target_retention_ms"].(float64)
	reason, _ := args["reason"].(string)

	if topic == "" || targetMs <= 0 || reason == "" {
		return mcp.NewToolResultError("topic, target_retention_ms, and reason are required"), nil
	}

	req, err := s.retentionEngine.RequestChange(topic, int64(targetMs), reason)
	if err != nil {
		s.logAudit(agentID, "kafka_retention_change_request", args, "", "error", 0, err, start)
		return s.toolError("failed to create change request", err), nil
	}

	s.logAudit(agentID, "kafka_retention_change_request", args, "", "success", 1, nil, start)
	return marshalResult(req)
}

func (s *Server) handleRetentionApprove(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	requestID, _ := args["request_id"].(string)
	if requestID == "" {
		return mcp.NewToolResultError("request_id is required"), nil
	}

	err := s.retentionEngine.ApproveChange(requestID, agentID)
	if err != nil {
		s.logAudit(agentID, "kafka_retention_approve", args, "", "error", 0, err, start)
		return s.toolError("failed to approve change", err), nil
	}

	s.logAudit(agentID, "kafka_retention_approve", args, "", "success", 1, nil, start)
	return marshalResult(map[string]any{"status": "approved", "request_id": requestID})
}
