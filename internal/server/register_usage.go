package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerUsageTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_usage_report",
		mcp.WithDescription("Generate usage analytics and chargeback reports. Actions: rollup (usage summary), chargeback (cost attribution), export_csv (CSV export), export_json (JSON export)."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: rollup, chargeback, export_csv, export_json")),
		mcp.WithString("tenant_id", mcp.Required(), mcp.Description("Tenant ID")),
		mcp.WithString("period", mcp.Description("Period: hourly, daily, monthly. Default: daily")),
		mcp.WithNumber("days_back", mcp.Description("Look-back period in days. Default: 30")),
	), s.handleUsageReport)
}
