package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerComplianceTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_compliance_scan",
		withCategory("governance"),
		mcp.WithDescription("Scan topics for compliance with GDPR, HIPAA, SOC2, or PCI-DSS. Detects PII exposure, retention policy violations, and missing data masking."),
		mcp.WithString("regulation", mcp.Description("Regulation to check: GDPR, HIPAA, SOC2, PCI-DSS. Empty = all")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
		mcp.WithString("topic", mcp.Description("Specific topic to scan (empty = all)")),
	), s.handleComplianceScan)

	s.mcpServer.AddTool(mcp.NewTool("kafka_compliance_report",
		withCategory("governance"),
		mcp.WithDescription("Generate a comprehensive compliance report with scores, violations, retention checks, and recommendations."),
		mcp.WithString("regulation", mcp.Required(), mcp.Description("Regulation: GDPR, HIPAA, SOC2, PCI-DSS")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
	), s.handleComplianceReport)

	s.mcpServer.AddTool(mcp.NewTool("kafka_compliance_violations",
		withCategory("governance"),
		mcp.WithDescription("List detected compliance violations with optional filters."),
		mcp.WithString("regulation", mcp.Description("Filter by regulation")),
		mcp.WithString("severity", mcp.Description("Filter by severity: critical, high, medium, low")),
	), s.handleComplianceViolations)

	s.mcpServer.AddTool(mcp.NewTool("kafka_compliance_audit_report",
		withCategory("governance"),
		mcp.WithDescription("Generate an exportable SOC2/HIPAA/GDPR/PCI-DSS audit report combining compliance scans, audit logs, data lineage, and access patterns into HTML, JSON, or text format."),
		mcp.WithString("regulation", mcp.Required(), mcp.Description("Regulation: GDPR, HIPAA, SOC2, PCI-DSS")),
		mcp.WithString("cluster", mcp.Description("Cluster name")),
		mcp.WithString("format", mcp.Description("Output format: html, json, text. Default: json")),
	), s.handleComplianceAuditReport)
}
