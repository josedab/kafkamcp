package server

import (
	"context"
	"encoding/json"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/contracts"
)

func (s *Server) handleContractViolations(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_contract_violations",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		topic := request.GetString("topic", "")

		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}

		switch action {
		case "violations":
			summary, err := s.contractEngine.GetViolationSummary(topic)
			if err != nil {
				return s.toolError("Failed to get violation summary", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_contract_violations", args, "", "success", 0, nil, start)
			return marshalResult(summary)

		case "enforce":
			message, _ := args["message"].(map[string]any)
			if message == nil {
				return mcp.NewToolResultError("message is required for enforce action"), nil
			}
			result := s.contractEngine.EnforceContract(topic, message)
			if result == nil {
				return marshalResult(map[string]any{"message": "no contract registered for this topic"})
			}
			s.logAudit(ctx, agentID, "kafka_contract_violations", args, "", "success", 0, nil, start)
			return marshalResult(result)

		case "set_mode":
			mode := request.GetString("mode", "")
			if mode == "" {
				return mcp.NewToolResultError("mode is required (warn, reject, quarantine)"), nil
			}
			enfMode := contracts.EnforcementMode(mode)
			if enfMode != contracts.ModeWarn && enfMode != contracts.ModeReject && enfMode != contracts.ModeQuarantine {
				return mcp.NewToolResultError("invalid mode: must be warn, reject, or quarantine"), nil
			}
			if err := s.contractEngine.SetEnforcementMode(topic, enfMode); err != nil {
				return s.toolError("Failed to set enforcement mode", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_contract_violations", args, "", "success", 0, nil, start)
			return marshalResult(map[string]any{"status": "updated", "topic": topic, "mode": mode})

		case "infer":
			samplesRaw, _ := args["samples"].([]any)
			if len(samplesRaw) == 0 {
				return mcp.NewToolResultError("samples are required for infer action"), nil
			}
			var samples []map[string]any
			for _, s := range samplesRaw {
				if m, ok := s.(map[string]any); ok {
					samples = append(samples, m)
				} else {
					if str, ok := s.(string); ok {
						var m map[string]any
						if err := json.Unmarshal([]byte(str), &m); err == nil {
							samples = append(samples, m)
						}
					}
				}
			}
			if len(samples) == 0 {
				return mcp.NewToolResultError("no valid message samples provided"), nil
			}
			inferred := s.contractEngine.InferContract(topic, samples)
			s.logAudit(ctx, agentID, "kafka_contract_violations", args, "", "success", 0, nil, start)
			return marshalResult(inferred)

		default:
			return mcp.NewToolResultError("invalid action: must be violations, enforce, set_mode, or infer"), nil
		}
	})(ctx, request)
}
