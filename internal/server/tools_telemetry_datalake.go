package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/telemetry"
)

func (s *Server) handleTelemetryDataLake(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_telemetry_lake", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")

		switch action {
		case "stats":
			stats := s.telemetryDataLake.GetStats()
			s.logAudit(ctx, agentID, "kafka_telemetry_lake", args, "", "success", 1, nil, start)
			return marshalResult(stats)

		case "agent_report":
			targetAgent := request.GetString("agent_id", "")
			if targetAgent == "" {
				targetAgent = agentID
			}
			report := s.telemetryDataLake.AgentReport(targetAgent)
			s.logAudit(ctx, agentID, "kafka_telemetry_lake", args, "", "success", 1, nil, start)
			return marshalResult(report)

		case "query":
			filterAgent := request.GetString("agent_id", "")
			filterTool := request.GetString("tool", "")
			filterCluster := request.GetString("cluster", "")
			filterStatus := request.GetString("status", "")
			limit := 100
			if l, ok := args["limit"].(float64); ok && l > 0 {
				limit = int(l)
			}
			records := s.telemetryDataLake.Query(filterAgent, filterTool, filterCluster, filterStatus, limit)
			s.logAudit(ctx, agentID, "kafka_telemetry_lake", args, "", "success", len(records), nil, start)
			return marshalResult(map[string]any{"records": records, "total": len(records)})

		case "export":
			format := request.GetString("format", "json")
			opts := telemetry.ExportOptions{
				Format:  telemetry.ExportFormat(format),
				AgentID: request.GetString("agent_id", ""),
				Tool:    request.GetString("tool", ""),
				Cluster: request.GetString("cluster", ""),
				Limit:   10000,
			}
			if l, ok := args["limit"].(float64); ok && l > 0 {
				opts.Limit = int(l)
			}
			result, err := s.telemetryDataLake.Export(opts)
			if err != nil {
				return s.toolError("export failed", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_telemetry_lake", args, "", "success", result.RecordCount, nil, start)
			return marshalResult(result)

		default:
			return mcp.NewToolResultError("action must be: stats, agent_report, query, export"), nil
		}
	})(ctx, request)
}
