package server

import (
	"context"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
)

func (s *Server) handlePredictAlert(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_predict_alert", resourceType: auth.ResourceConsumerGroup, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		switch action {
		case "record":
			groupID := request.GetString("group_id", "")
			if groupID == "" {
				return mcp.NewToolResultError("group_id is required"), nil
			}
			if deny := validateStringLength("group_id", groupID); deny != nil {
				return deny, nil
			}
			if deny := validateKafkaIdentifier("group_id", groupID); deny != nil {
				return deny, nil
			}
			totalLag := int64(request.GetFloat("total_lag", 0))
			memberCount := int(clampInt64(int64(request.GetInt("member_count", 0)), 0, 100000))

			s.predictAlertEngine.RecordSample(groupID, totalLag, memberCount)
			s.logAudit(ctx, agentID, "kafka_predict_alert", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "recorded", "group_id": groupID})

		case "analyze":
			groupID := request.GetString("group_id", "")
			if groupID == "" {
				return mcp.NewToolResultError("group_id is required"), nil
			}
			if deny := validateStringLength("group_id", groupID); deny != nil {
				return deny, nil
			}
			if deny := validateKafkaIdentifier("group_id", groupID); deny != nil {
				return deny, nil
			}
			analysis := s.predictAlertEngine.Analyze(groupID)
			s.logAudit(ctx, agentID, "kafka_predict_alert", args, "", "success", 1, nil, start)
			return marshalResult(analysis)

		case "alerts":
			groupID := request.GetString("group_id", "")
			if deny := validateStringLength("group_id", groupID); deny != nil {
				return deny, nil
			}
			limit := request.GetInt("limit", 50)
			if limit > MaxDLQMessages {
				limit = MaxDLQMessages
			}
			alerts := s.predictAlertEngine.ListAlerts(groupID, limit)
			s.logAudit(ctx, agentID, "kafka_predict_alert", args, "", "success", len(alerts), nil, start)
			return marshalResult(map[string]any{"alerts": alerts, "total": len(alerts)})

		case "acknowledge":
			alertID := request.GetString("alert_id", "")
			if alertID == "" {
				return mcp.NewToolResultError("alert_id is required"), nil
			}
			if deny := validateStringLength("alert_id", alertID); deny != nil {
				return deny, nil
			}
			if err := s.predictAlertEngine.AcknowledgeAlert(alertID); err != nil {
				return s.toolError("Failed to acknowledge alert", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_predict_alert", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"status": "acknowledged", "alert_id": alertID})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q, use: analyze, record, alerts, acknowledge", action)), nil
		}
	})(ctx, request)
}
