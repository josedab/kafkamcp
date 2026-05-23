package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/intentverify"
)

func (s *Server) handleIntentVerify(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_intent_verify", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		switch action {
		case "classify":
			toolName := request.GetString("tool_name", "")
			if toolName == "" {
				return mcp.NewToolResultError("tool_name is required"), nil
			}
			classification := s.intentVerifier.ClassifyTool(toolName)
			return marshalResult(classification)

		case "verify":
			toolName := request.GetString("tool_name", "")
			intent := request.GetString("intent", "")
			if toolName == "" {
				return mcp.NewToolResultError("tool_name is required"), nil
			}
			match := s.intentVerifier.VerifyIntent(agentID, toolName, intent)
			return marshalResult(match)

		case "confirm":
			toolName := request.GetString("tool_name", "")
			intent := request.GetString("intent", "")
			if toolName == "" {
				return mcp.NewToolResultError("tool_name is required"), nil
			}
			req := s.intentVerifier.RequestConfirmation(agentID, toolName, intent)
			s.logAudit(ctx, agentID, "kafka_intent_verify", args, "", "success", 1, nil, start)
			return marshalResult(req)

		case "resolve":
			confirmID := request.GetString("confirm_id", "")
			if confirmID == "" {
				return mcp.NewToolResultError("confirm_id is required"), nil
			}
			approved := request.GetBool("approved", false)
			if err := s.intentVerifier.ResolveConfirmation(confirmID, agentID, approved); err != nil {
				return s.toolError("Failed to resolve confirmation", err), nil
			}
			status := "denied"
			if approved {
				status = "confirmed"
			}
			s.logAudit(ctx, agentID, "kafka_intent_verify", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": status, "confirm_id": confirmID})

		case "pending":
			pending := s.intentVerifier.ListPendingConfirmations(agentID)
			return marshalResult(map[string]any{"pending": pending, "total": len(pending)})

		case "list_classifications":
			riskLevel := intentverify.RiskLevel(request.GetString("risk_level", ""))
			classifications := s.intentVerifier.ListClassifications(riskLevel)
			return marshalResult(map[string]any{"classifications": classifications, "total": len(classifications)})

		case "configure_agent":
			targetAgent := request.GetString("target_agent", "")
			if targetAgent == "" {
				return mcp.NewToolResultError("target_agent is required"), nil
			}
			maxRisk := intentverify.RiskLevel(request.GetString("max_risk", string(intentverify.RiskAdmin)))
			blockDestructive := request.GetBool("block_destructive", false)
			s.intentVerifier.SetAgentConfig(intentverify.AgentRiskConfig{
				AgentID:          targetAgent,
				MaxRiskLevel:     maxRisk,
				RequireConfirmAt: intentverify.RiskDestructive,
				BlockDestructive: blockDestructive,
			})
			s.logAudit(ctx, agentID, "kafka_intent_verify", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "configured", "agent_id": targetAgent})

		case "stats":
			return marshalResult(s.intentVerifier.Stats())

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action: %s", action)), nil
		}
	})(ctx, request)
}
