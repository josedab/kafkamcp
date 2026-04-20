package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== P3 TOOL HANDLERS =====================

// handleWatch registers or manages a watch subscription for event-driven agent workflows.
func (s *Server) handleWatch(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()
	action := request.GetString("action", "subscribe")

	if deny := s.checkRateLimit(agentID, "kafka_watch"); deny != nil {
		return deny, nil
	}

	switch action {
	case "subscribe":
		return s.handleWatchSubscribe(ctx, request, agentID, args, start)
	case "unsubscribe":
		return s.handleWatchUnsubscribe(ctx, request, agentID, args, start)
	case "list":
		return s.handleWatchList(ctx, agentID, args, start)
	default:
		return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: subscribe, unsubscribe, list", action)), nil
	}
}

func (s *Server) handleWatchSubscribe(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	topicPattern := request.GetString("topic_pattern", "")
	if topicPattern == "" {
		return mcp.NewToolResultError("topic_pattern is required for subscribe action"), nil
	}
	if deny := validateStringLength("topic_pattern", topicPattern); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_watch", auth.ResourceTopic, topicPattern, auth.ActionRead); deny != nil {
		return deny, nil
	}

	if s.watchManager == nil {
		return mcp.NewToolResultError("watch manager not initialized"), nil
	}

	keyFilter := request.GetString("key_filter", "")
	valueFilter := request.GetString("value_filter", "")
	webhookURL := request.GetString("webhook_url", "")

	sub, err := s.watchManager.Subscribe(agentID, topicPattern, keyFilter, valueFilter, webhookURL, nil)
	if err != nil {
		s.logAudit(agentID, "kafka_watch", args, "", "error", 0, err, start)
		return s.toolError("Failed to create watch", err), nil
	}

	s.logAudit(agentID, "kafka_watch", args, "", "success", 0, nil, start)
	return marshalResult(map[string]any{
		"status":          "subscribed",
		"subscription_id": sub.ID,
		"topic_pattern":   sub.TopicPattern,
		"key_filter":      sub.KeyFilter,
		"value_filter":    sub.ValueFilter,
		"webhook_url":     redactURL(sub.WebhookURL),
	})
}

func (s *Server) handleWatchUnsubscribe(_ context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	subscriptionID := request.GetString("subscription_id", "")
	if subscriptionID == "" {
		return mcp.NewToolResultError("subscription_id is required for unsubscribe action"), nil
	}

	if s.watchManager == nil {
		return mcp.NewToolResultError("watch manager not initialized"), nil
	}

	err := s.watchManager.Unsubscribe(subscriptionID, agentID)
	if err != nil {
		s.logAudit(agentID, "kafka_watch", args, "", "error", 0, err, start)
		return s.toolError("Failed to unsubscribe", err), nil
	}

	s.logAudit(agentID, "kafka_watch", args, "", "success", 0, nil, start)
	return marshalResult(map[string]any{
		"status":          "unsubscribed",
		"subscription_id": subscriptionID,
	})
}

func (s *Server) handleWatchList(_ context.Context, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	if s.watchManager == nil {
		return mcp.NewToolResultError("watch manager not initialized"), nil
	}

	subs := s.watchManager.ListSubscriptions(agentID)
	s.logAudit(agentID, "kafka_watch", args, "", "success", len(subs), nil, start)
	return marshalResult(map[string]any{
		"subscriptions": subs,
		"total_count":   len(subs),
	})
}

// handleRemediateDLQ performs auto-remediation actions on DLQ messages.
func (s *Server) handleRemediateDLQ(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	topic := request.GetString("topic", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}
	if deny := validateStringLength("topic", topic); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_remediate_dlq"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_remediate_dlq", auth.ResourceTopic, topic, auth.ActionAdmin); deny != nil {
		return deny, nil
	}

	action := request.GetString("action", "analyze")
	limit := int(clampInt64(int64(request.GetInt("limit", 100)), 1, MaxDLQMessages))
	targetTopic := request.GetString("target_topic", "")

	if s.dlqRemediator == nil {
		return mcp.NewToolResultError("DLQ remediator not initialized"), nil
	}

	result, err := s.dlqRemediator.Remediate(ctx, s.kafkaManager, cluster, topic, action, targetTopic, limit)
	if err != nil {
		s.logAudit(agentID, "kafka_remediate_dlq", args, cluster, "error", 0, err, start)
		return s.toolError("DLQ remediation failed", err), nil
	}

	s.logAudit(agentID, "kafka_remediate_dlq", args, cluster, "success", result.ProcessedCount, nil, start)
	return marshalResult(result)
}

// handleSchemaDiff computes field-by-field diff between two schema versions.
func (s *Server) handleSchemaDiff(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()
	subject := request.GetString("subject", "")

	if subject == "" {
		return mcp.NewToolResultError("subject is required"), nil
	}
	if deny := validateStringLength("subject", subject); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_schema_diff"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_schema_diff", auth.ResourceSchema, subject, auth.ActionRead); deny != nil {
		return deny, nil
	}

	schemaClient := s.getSchemaClient("")
	if schemaClient == nil {
		return mcp.NewToolResultError("No Schema Registry configured"), nil
	}

	versionFrom := request.GetString("version_from", "")
	versionTo := request.GetString("version_to", "latest")

	if versionFrom == "" {
		return mcp.NewToolResultError("version_from is required"), nil
	}

	schemaFrom, err := schemaClient.GetSchema(ctx, subject, versionFrom)
	if err != nil {
		s.logAudit(agentID, "kafka_schema_diff", args, "", "error", 0, err, start)
		return s.toolError(fmt.Sprintf("Failed to get schema version %s", versionFrom), err), nil
	}

	schemaTo, err := schemaClient.GetSchema(ctx, subject, versionTo)
	if err != nil {
		s.logAudit(agentID, "kafka_schema_diff", args, "", "error", 0, err, start)
		return s.toolError(fmt.Sprintf("Failed to get schema version %s", versionTo), err), nil
	}

	diff := computeSchemaDiff(schemaFrom.Schema, schemaTo.Schema)
	s.logAudit(agentID, "kafka_schema_diff", args, "", "success", 0, nil, start)
	return marshalResult(map[string]any{
		"subject":      subject,
		"version_from": versionFrom,
		"version_to":   versionTo,
		"diff":         diff,
	})
}
