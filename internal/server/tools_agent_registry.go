package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

// handleAgentRegistry manages multi-agent collaboration primitives.
func (s *Server) handleAgentRegistry(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_agent_registry", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		if s.agentRegistry == nil {
			return mcp.NewToolResultError("agent registry not initialized"), nil
		}

		switch action {
		case "register":
			name := request.GetString("agent_name", agentID)
			if deny := validateStringLength("agent_name", name); deny != nil {
				return deny, nil
			}
			agent := s.agentRegistry.RegisterAgent(agentID, name)
			s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "registered", "agent": agent})

		case "heartbeat":
			if err := s.agentRegistry.Heartbeat(agentID); err != nil {
				s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "error", 0, err, start)
				return s.toolError("agent heartbeat failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "heartbeat_updated"})

		case "claim":
			topic := request.GetString("topic", "")
			partition := int32(request.GetFloat("partition", -1))
			if topic == "" || partition < 0 {
				return mcp.NewToolResultError("topic and partition are required for claim"), nil
			}
			if deny := validateStringLength("topic", topic); deny != nil {
				return deny, nil
			}
			if err := s.agentRegistry.ClaimPartition(agentID, topic, partition); err != nil {
				s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "error", 0, err, start)
				return s.toolError("partition claim failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "claimed", "topic": topic, "partition": partition})

		case "release":
			topic := request.GetString("topic", "")
			partition := int32(request.GetFloat("partition", -1))
			if topic == "" || partition < 0 {
				return mcp.NewToolResultError("topic and partition are required for release"), nil
			}
			if deny := validateStringLength("topic", topic); deny != nil {
				return deny, nil
			}
			if err := s.agentRegistry.ReleasePartition(agentID, topic, partition); err != nil {
				s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "error", 0, err, start)
				return s.toolError("partition release failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "released", "topic": topic, "partition": partition})

		case "share_cursor":
			topic := request.GetString("topic", "")
			partition := int32(request.GetFloat("partition", -1))
			offset := int64(request.GetFloat("offset", -1))
			if topic == "" || partition < 0 || offset < 0 {
				return mcp.NewToolResultError("topic, partition, and offset are required for share_cursor"), nil
			}
			if deny := validateStringLength("topic", topic); deny != nil {
				return deny, nil
			}
			cursor := s.agentRegistry.ShareCursor(agentID, topic, partition, offset)
			s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "cursor_shared", "cursor": cursor})

		case "handoff":
			targetAgent := request.GetString("target_agent", "")
			msgData := request.GetString("message_data", "")
			if targetAgent == "" {
				return mcp.NewToolResultError("target_agent is required for handoff"), nil
			}
			if deny := validateStringLength("target_agent", targetAgent); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("message_data", msgData); deny != nil {
				return deny, nil
			}
			msg, err := s.agentRegistry.QueueHandoff(agentID, targetAgent, msgData)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "error", 0, err, start)
				return s.toolError("agent handoff failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "handoff_queued", "handoff": msg})

		case "list_agents":
			agents := s.agentRegistry.ListAgents()
			cursors := s.agentRegistry.GetCursors()
			pending := s.agentRegistry.PendingHandoffs(agentID)
			s.logAudit(ctx, agentID, "kafka_agent_registry", args, "", "success", len(agents), nil, start)
			return marshalResult(map[string]any{
				"agents":           agents,
				"shared_cursors":   cursors,
				"pending_handoffs": pending,
				"total_agents":     len(agents),
			})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: register, heartbeat, claim, release, share_cursor, handoff, list_agents", action)), nil
		}
	})(ctx, request)
}
