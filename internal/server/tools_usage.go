package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleUsageReport(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_usage_report",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		tenantID := request.GetString("tenant_id", "")
		period := request.GetString("period", "daily")
		daysBack := 30
		if db, ok := args["days_back"].(float64); ok && db > 0 {
			daysBack = int(db)
		}
		if daysBack > 365 {
			daysBack = 365
		}

		if tenantID == "" {
			return mcp.NewToolResultError("tenant_id is required"), nil
		}
		if deny := validateStringLength("tenant_id", tenantID); deny != nil {
			return deny, nil
		}

		end := time.Now()
		startTime := end.AddDate(0, 0, -daysBack)

		switch action {
		case "rollup":
			rollup := s.usageTracker.GenerateRollup(tenantID, period, startTime, end)
			s.logAudit(agentID, "kafka_usage_report", args, "", "success", 0, nil, start)
			return marshalResult(rollup)

		case "chargeback":
			tenant, err := s.tenantManager.GetTenant(tenantID)
			if err != nil {
				s.logAudit(agentID, "kafka_usage_report", args, "", "error", 0, err, start)
				return s.toolError("Tenant not found", err), nil
			}
			report := s.usageTracker.GenerateChargebackReport(tenant, period, startTime, end)
			s.logAudit(agentID, "kafka_usage_report", args, "", "success", 0, nil, start)
			return marshalResult(report)

		case "export_csv":
			csv := s.usageTracker.ExportCSV(tenantID, startTime, end)
			s.logAudit(agentID, "kafka_usage_report", args, "", "success", 0, nil, start)
			return mcp.NewToolResultText(csv), nil

		case "export_json":
			jsonStr, err := s.usageTracker.ExportJSON(tenantID, startTime, end)
			if err != nil {
				return s.toolError("Failed to export JSON", err), nil
			}
			s.logAudit(agentID, "kafka_usage_report", args, "", "success", 0, nil, start)
			return mcp.NewToolResultText(jsonStr), nil

		default:
			return mcp.NewToolResultError("invalid action: must be rollup, chargeback, export_csv, or export_json"), nil
		}
	})(ctx, request)
}
