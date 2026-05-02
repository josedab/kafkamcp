package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleDashboardCreate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_dashboard_create",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		provider := request.GetString("provider", "grafana")
		dashType := request.GetString("type", "cluster")
		target := request.GetString("target", "")

		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("provider", provider); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("type", dashType); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("target", target); deny != nil {
			return deny, nil
		}

		dash, err := s.observabilityMgr.CreateDashboard(name, provider, dashType, target, agentID)
		if err != nil {
			s.logAudit(agentID, "kafka_dashboard_create", args, "", "error", 0, err, start)
			return s.toolError("Failed to create dashboard", err), nil
		}

		s.logAudit(agentID, "kafka_dashboard_create", args, "", "success", 0, nil, start)
		return marshalResult(dash)
	})(ctx, request)
}

func (s *Server) handleAlertCreate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_alert_create",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		metric := request.GetString("metric", "")
		condition := request.GetString("condition", "gt")
		threshold := 0.0
		if t, ok := args["threshold"].(float64); ok {
			threshold = t
		}
		severity := request.GetString("severity", "warning")
		duration := request.GetString("duration", "5m")
		topic := request.GetString("topic", "")
		groupID := request.GetString("group_id", "")

		if name == "" || metric == "" {
			return mcp.NewToolResultError("name and metric are required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("metric", metric); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("duration", duration); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("group_id", groupID); deny != nil {
			return deny, nil
		}
		switch condition {
		case "gt", "lt", "eq", "ne":
			// valid
		default:
			return mcp.NewToolResultError("invalid condition: must be gt, lt, eq, or ne"), nil
		}
		switch severity {
		case "info", "warning", "critical":
			// valid
		default:
			return mcp.NewToolResultError("invalid severity: must be info, warning, or critical"), nil
		}

		rule, err := s.observabilityMgr.CreateAlertRule(name, metric, condition, threshold, duration, severity, agentID, nil, topic, groupID)
		if err != nil {
			s.logAudit(agentID, "kafka_alert_create", args, "", "error", 0, err, start)
			return s.toolError("Failed to create alert rule", err), nil
		}

		s.logAudit(agentID, "kafka_alert_create", args, "", "success", 0, nil, start)
		return marshalResult(rule)
	})(ctx, request)
}

func (s *Server) handleAlertList(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_alert_list",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		rules := s.observabilityMgr.ListAlertRules()
		s.logAudit(agentID, "kafka_alert_list", args, "", "success", len(rules), nil, start)
		return marshalResult(map[string]any{"alert_rules": rules, "total_count": len(rules)})
	})(ctx, request)
}

func (s *Server) handleAlertDelete(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_alert_delete",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionWrite,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		alertID := request.GetString("alert_id", "")
		if alertID == "" {
			return mcp.NewToolResultError("alert_id is required"), nil
		}
		if deny := validateStringLength("alert_id", alertID); deny != nil {
			return deny, nil
		}

		if err := s.observabilityMgr.DeleteAlertRule(alertID); err != nil {
			s.logAudit(agentID, "kafka_alert_delete", args, "", "error", 0, err, start)
			return s.toolError("Failed to delete alert rule", err), nil
		}

		s.logAudit(agentID, "kafka_alert_delete", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"status": "deleted", "alert_id": alertID})
	})(ctx, request)
}
