package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/eventbus"
)

func (s *Server) handleEventBus(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_event_bus", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		switch action {
		case "subscribe":
			filter := buildEventFilter(request)
			sub, err := s.eventBus.Subscribe(filter)
			if err != nil {
				return s.toolError("Failed to subscribe", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_event_bus", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{
				"status":          "subscribed",
				"subscription_id": sub.ID,
				"filter":          filter,
			})

		case "unsubscribe":
			subID := request.GetString("subscription_id", "")
			if subID == "" {
				return mcp.NewToolResultError("subscription_id is required"), nil
			}
			if err := s.eventBus.Unsubscribe(subID); err != nil {
				return s.toolError("Failed to unsubscribe", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_event_bus", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "unsubscribed", "subscription_id": subID})

		case "publish":
			dataStr := request.GetString("event_data", "")
			if dataStr == "" {
				return mcp.NewToolResultError("event_data is required"), nil
			}
			var data map[string]any
			if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
				return mcp.NewToolResultError("invalid JSON for event_data"), nil
			}
			evtType := eventbus.EventType(request.GetString("event_type", string(eventbus.EventCustom)))
			s.eventBus.Publish(eventbus.Event{
				Type:    evtType,
				Source:  "mcp_tool",
				AgentID: agentID,
				Data:    data,
			})
			return marshalResult(map[string]any{"status": "published"})

		case "replay":
			filter := buildEventFilter(request)
			limit := request.GetInt("limit", 100)
			events := s.eventBus.Replay(filter, limit)
			s.logAudit(ctx, agentID, "kafka_event_bus", args, "", "success", len(events), nil, start)
			return marshalResult(map[string]any{
				"events": events,
				"total":  len(events),
			})

		case "stats":
			return marshalResult(s.eventBus.Stats())

		case "list_subscriptions":
			subs := s.eventBus.ListSubscriptions()
			return marshalResult(map[string]any{"subscriptions": subs, "total": len(subs)})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action: %s", action)), nil
		}
	})(ctx, request)
}

func buildEventFilter(request mcp.CallToolRequest) eventbus.SubscriptionFilter {
	filter := eventbus.SubscriptionFilter{}
	if et := request.GetString("event_type", ""); et != "" {
		filter.EventTypes = []eventbus.EventType{eventbus.EventType(et)}
	}
	if agent := request.GetString("agent_filter", ""); agent != "" {
		filter.AgentIDs = []string{agent}
	}
	if tool := request.GetString("tool_filter", ""); tool != "" {
		filter.Tools = []string{tool}
	}
	if cluster := request.GetString("cluster_filter", ""); cluster != "" {
		filter.Clusters = []string{cluster}
	}
	if pattern := request.GetString("source_pattern", ""); pattern != "" {
		filter.SourcePattern = pattern
	}
	return filter
}
