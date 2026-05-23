package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/gateway"
)

func (s *Server) handleEventTranslate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_event_translate", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		switch action {
		case "create_rule":
			name := request.GetString("name", "")
			srcBackend := request.GetString("source_backend", "")
			srcTopic := request.GetString("source_topic", "")
			tgtBackend := request.GetString("target_backend", "")
			tgtTopic := request.GetString("target_topic", "")

			var transform gateway.TransformConfig
			if t, ok := args["transform"].(map[string]any); ok {
				if rh, ok := t["rename_headers"].(map[string]any); ok {
					transform.RenameHeaders = make(map[string]string)
					for k, v := range rh {
						if s, ok := v.(string); ok {
							transform.RenameHeaders[k] = s
						}
					}
				}
				if ah, ok := t["add_headers"].(map[string]any); ok {
					transform.AddHeaders = make(map[string]string)
					for k, v := range ah {
						if s, ok := v.(string); ok {
							transform.AddHeaders[k] = s
						}
					}
				}
				if kt, ok := t["key_transform"].(string); ok {
					transform.KeyTransform = kt
				}
				if kp, ok := t["key_prefix"].(string); ok {
					transform.KeyPrefix = kp
				}
			}

			rule, err := s.eventTranslator.CreateRule(name, srcBackend, srcTopic, tgtBackend, tgtTopic, transform)
			if err != nil {
				return s.toolError("failed to create rule", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_event_translate", args, "", "success", 1, nil, start)
			return marshalResult(rule)

		case "list_rules":
			rules := s.eventTranslator.ListRules("")
			s.logAudit(ctx, agentID, "kafka_event_translate", args, "", "success", len(rules), nil, start)
			return marshalResult(map[string]any{"rules": rules, "total": len(rules)})

		case "execute":
			ruleID := request.GetString("rule_id", "")
			if ruleID == "" {
				return mcp.NewToolResultError("rule_id is required"), nil
			}
			limit := 100
			if l, ok := args["limit"].(float64); ok && l > 0 {
				limit = int(l)
			}
			result, err := s.eventTranslator.ExecuteRule(ctx, ruleID, limit)
			if err != nil {
				return s.toolError("failed to execute rule", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_event_translate", args, "", "success", result.MessagesWritten, nil, start)
			return marshalResult(result)

		case "pause":
			ruleID := request.GetString("rule_id", "")
			if err := s.eventTranslator.PauseRule(ruleID); err != nil {
				return s.toolError("failed to pause rule", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_event_translate", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "paused", "rule_id": ruleID})

		case "resume":
			ruleID := request.GetString("rule_id", "")
			if err := s.eventTranslator.ResumeRule(ruleID); err != nil {
				return s.toolError("failed to resume rule", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_event_translate", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "active", "rule_id": ruleID})

		case "delete":
			ruleID := request.GetString("rule_id", "")
			if err := s.eventTranslator.DeleteRule(ruleID); err != nil {
				return s.toolError("failed to delete rule", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_event_translate", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "deleted", "rule_id": ruleID})

		case "stats":
			stats := s.eventTranslator.GetStats()
			s.logAudit(ctx, agentID, "kafka_event_translate", args, "", "success", 1, nil, start)
			return marshalResult(stats)

		case "protocol_map":
			srcProto := gateway.Protocol(request.GetString("source_protocol", "kafka"))
			tgtProto := gateway.Protocol(request.GetString("target_protocol", "pulsar"))
			mapping := gateway.DescribeProtocolMapping(srcProto, tgtProto)
			s.logAudit(ctx, agentID, "kafka_event_translate", args, "", "success", 1, nil, start)
			return marshalResult(mapping)

		default:
			return mcp.NewToolResultError("action must be: create_rule, list_rules, execute, pause, resume, delete, stats, dry_run, protocol_map"), nil
		}
	})(ctx, request)
}
