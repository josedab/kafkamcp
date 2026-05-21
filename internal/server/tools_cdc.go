package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/cdc"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleCDCCreateBridge(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cdc_create_bridge",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		sourceTopic := request.GetString("source_topic", "")
		sinkType := request.GetString("sink_type", "")

		if name == "" || sourceTopic == "" || sinkType == "" {
			return mcp.NewToolResultError("name, source_topic, and sink_type are required"), nil
		}
		for _, field := range []string{"name", "source_topic", "sink_type"} {
			if deny := validateStringLength(field, request.GetString(field, "")); deny != nil {
				return deny, nil
			}
		}

		sink := cdc.SinkConfig{
			Type:     cdc.SinkType(sinkType),
			Table:    request.GetString("table", ""),
			KeyField: request.GetString("key_field", ""),
		}

		bridge, err := s.cdcEngine.CreateBridge(name, sourceTopic, agentID, sink, nil)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_cdc_create_bridge", args, "", "error", 0, err, start)
			return s.toolError("Failed to create CDC bridge", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_cdc_create_bridge", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status":  "created",
			"bridge":  bridge,
			"message": fmt.Sprintf("CDC bridge %q created: %s → %s", bridge.Name, sourceTopic, sinkType),
		})
	})(ctx, request)
}

func (s *Server) handleCDCListBridges(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cdc_list_bridges",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		bridges := s.cdcEngine.ListBridges()

		s.logAudit(ctx, agentID, "kafka_cdc_list_bridges", args, "", "success", len(bridges), nil, start)
		return marshalResult(map[string]any{
			"bridges":     bridges,
			"total_count": len(bridges),
		})
	})(ctx, request)
}

func (s *Server) handleCDCManageBridge(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cdc_manage_bridge",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		bridgeID := request.GetString("bridge_id", "")
		action := request.GetString("action", "")
		if bridgeID == "" || action == "" {
			return mcp.NewToolResultError("bridge_id and action are required"), nil
		}
		if deny := validateStringLength("bridge_id", bridgeID); deny != nil {
			return deny, nil
		}

		switch action {
		case "status":
			bridge, err := s.cdcEngine.GetBridge(bridgeID)
			if err != nil {
				return s.toolError("Bridge not found", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_cdc_manage_bridge", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"bridge": bridge})

		case "pause":
			if err := s.cdcEngine.UpdateBridgeStatus(bridgeID, "paused"); err != nil {
				return s.toolError("Failed to pause bridge", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_cdc_manage_bridge", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "paused", "bridge_id": bridgeID})

		case "resume":
			if err := s.cdcEngine.UpdateBridgeStatus(bridgeID, "active"); err != nil {
				return s.toolError("Failed to resume bridge", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_cdc_manage_bridge", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "active", "bridge_id": bridgeID})

		case "stop":
			if err := s.cdcEngine.UpdateBridgeStatus(bridgeID, "stopped"); err != nil {
				return s.toolError("Failed to stop bridge", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_cdc_manage_bridge", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "stopped", "bridge_id": bridgeID})

		case "delete":
			if err := s.cdcEngine.DeleteBridge(bridgeID); err != nil {
				return s.toolError("Failed to delete bridge", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_cdc_manage_bridge", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "deleted", "bridge_id": bridgeID})

		case "events":
			events, err := s.cdcEngine.GetRecentEvents(bridgeID, 50)
			if err != nil {
				return s.toolError("Failed to get events", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_cdc_manage_bridge", args, "", "success", len(events), nil, start)
			return marshalResult(map[string]any{"events": events, "count": len(events)})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; use: status, pause, resume, stop, delete, events", action)), nil
		}
	})(ctx, request)
}

func (s *Server) handleCDCProcess(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cdc_process",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		bridgeID := request.GetString("bridge_id", "")
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")
		if bridgeID == "" || topic == "" {
			return mcp.NewToolResultError("bridge_id and topic are required"), nil
		}
		if deny := validateStringLength("bridge_id", bridgeID); deny != nil {
			return deny, nil
		}

		messages, err := s.kafkaManager.Consume(ctx, cluster, topic, -1, "latest", 100, 5000, "")
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_cdc_process", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to consume messages", err), nil
		}

		var processed int
		var events []cdc.CDCEvent
		for _, m := range messages {
			ev, err := s.cdcEngine.ProcessEvent(bridgeID, fmt.Sprintf("%v", m.Key), fmt.Sprintf("%v", m.Value), m.Partition, m.Offset, topic)
			if err != nil {
				continue
			}
			events = append(events, *ev)
			processed++
		}

		s.logAudit(ctx, agentID, "kafka_cdc_process", args, cluster, "success", processed, nil, start)
		return marshalResult(map[string]any{
			"processed": processed,
			"total":     len(messages),
			"events":    events,
		})
	})(ctx, request)
}
