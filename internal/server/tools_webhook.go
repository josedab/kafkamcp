package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/webhook"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleWebhookSubscribe(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	id, _ := args["id"].(string)
	url, _ := args["url"].(string)
	if id == "" || url == "" {
		return mcp.NewToolResultError("id and url are required"), nil
	}

	secret, _ := args["secret"].(string)

	var topics []string
	if t, ok := args["topics"].([]any); ok {
		for _, v := range t {
			if s, ok := v.(string); ok {
				topics = append(topics, s)
			}
		}
	}

	var events []string
	if e, ok := args["events"].([]any); ok {
		for _, v := range e {
			if s, ok := v.(string); ok {
				events = append(events, s)
			}
		}
	}

	var metadata map[string]string
	if m, ok := args["metadata"].(map[string]any); ok {
		metadata = make(map[string]string, len(m))
		for k, v := range m {
			if s, ok := v.(string); ok {
				metadata[k] = s
			}
		}
	}

	sub, err := s.webhookManager.Subscribe(webhook.Subscription{
		ID:       id,
		URL:      url,
		Topics:   topics,
		Events:   events,
		Secret:   secret,
		Metadata: metadata,
	})
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_webhook_subscribe", args, "", "error", 0, err, start)
		return s.toolError("failed to subscribe webhook", err), nil
	}

	s.logAudit(ctx, agentID, "kafka_webhook_subscribe", args, "", "success", 1, nil, start)
	return marshalResult(sub)
}

func (s *Server) handleWebhookUnsubscribe(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	id, _ := args["id"].(string)
	if id == "" {
		return mcp.NewToolResultError("id is required"), nil
	}

	err := s.webhookManager.Unsubscribe(id)
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_webhook_unsubscribe", args, "", "error", 0, err, start)
		return s.toolError("failed to unsubscribe webhook", err), nil
	}

	s.logAudit(ctx, agentID, "kafka_webhook_unsubscribe", args, "", "success", 1, nil, start)
	return marshalResult(map[string]string{"status": "removed", "id": id})
}

func (s *Server) handleWebhookList(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	subs := s.webhookManager.ListSubscriptions()
	s.logAudit(ctx, agentID, "kafka_webhook_list", args, "", "success", len(subs), nil, start)
	return marshalResult(map[string]any{"subscriptions": subs, "total": len(subs)})
}

func (s *Server) handleWebhookDeliver(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	eventID, _ := args["event_id"].(string)
	eventType, _ := args["event_type"].(string)
	if eventID == "" || eventType == "" {
		return mcp.NewToolResultError("event_id and event_type are required"), nil
	}

	topic, _ := args["topic"].(string)
	cluster, _ := args["cluster"].(string)
	payload := args["payload"]

	event := webhook.WebhookEvent{
		ID:        eventID,
		Type:      eventType,
		Topic:     topic,
		Cluster:   cluster,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	matched := s.webhookManager.Deliver(event)
	s.logAudit(ctx, agentID, "kafka_webhook_deliver", args, "", "success", matched, nil, start)
	return marshalResult(map[string]any{
		"status":              "dispatched",
		"event_id":            eventID,
		"matched_subscribers": matched,
	})
}

func (s *Server) handleWebhookHistory(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	subID, _ := args["subscription_id"].(string)
	if subID == "" {
		return mcp.NewToolResultError("subscription_id is required"), nil
	}

	limit := 20
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	history := s.webhookManager.DeliveryHistory(subID, limit)
	s.logAudit(ctx, agentID, "kafka_webhook_history", args, "", "success", len(history), nil, start)
	return marshalResult(map[string]any{"deliveries": history, "total": len(history)})
}
