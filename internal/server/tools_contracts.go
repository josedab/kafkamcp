package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/contracts"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleDataContract(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_validate_contract", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("action", action); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		switch action {
		case "register":
			contractData, ok := args["contract"]
			if !ok {
				return mcp.NewToolResultError("contract is required for register action"), nil
			}
			data, err := json.Marshal(contractData)
			if err != nil {
				return s.toolError("invalid contract", err), nil
			}
			var c contracts.Contract
			if err := json.Unmarshal(data, &c); err != nil {
				return s.toolError("invalid contract format", err), nil
			}
			if err := s.contractEngine.RegisterContract(c); err != nil {
				return s.toolError("failed to register contract", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_validate_contract", args, cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "registered", "contract": c.Name, "topic": c.Topic})

		case "validate":
			if topic == "" {
				return mcp.NewToolResultError("topic is required for validate action"), nil
			}
			msgData, ok := args["message"]
			if !ok {
				return mcp.NewToolResultError("message is required for validate action"), nil
			}
			msgMap, ok := msgData.(map[string]any)
			if !ok {
				return mcp.NewToolResultError("message must be a JSON object"), nil
			}
			result := s.contractEngine.ValidateMessage(topic, msgMap)
			s.logAudit(ctx, agentID, "kafka_validate_contract", args, cluster, "success", 1, nil, start)
			return marshalResult(result)

		case "get":
			if topic == "" {
				return mcp.NewToolResultError("topic is required for get action"), nil
			}
			c, err := s.contractEngine.GetContract(topic)
			if err != nil {
				return s.toolError("contract not found", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_validate_contract", args, cluster, "success", 1, nil, start)
			return marshalResult(c)

		case "list":
			result := s.contractEngine.ListContracts()
			s.logAudit(ctx, agentID, "kafka_validate_contract", args, cluster, "success", len(result), nil, start)
			return marshalResult(map[string]any{"contracts": result, "total": len(result)})

		case "compliance":
			if topic == "" {
				return mcp.NewToolResultError("topic is required for compliance action"), nil
			}
			report, err := s.contractEngine.GetComplianceReport(topic)
			if err != nil {
				return s.toolError("compliance report failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_validate_contract", args, cluster, "success", 1, nil, start)
			return marshalResult(report)

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: validate, register, get, list, compliance", action)), nil
		}
	})(ctx, request)
}
