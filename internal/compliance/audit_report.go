package compliance

import (
	"fmt"
	"html"
	"strings"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// AuditReportFormat defines supported export formats.
type AuditReportFormat string

const (
	FormatHTML AuditReportFormat = "html"
	FormatJSON AuditReportFormat = "json"
	FormatText AuditReportFormat = "text"
)

// AuditEntry represents a single audit log entry for inclusion in reports.
type AuditEntry struct {
	Timestamp string `json:"timestamp"`
	AgentID   string `json:"agent_id"`
	Tool      string `json:"tool"`
	Cluster   string `json:"cluster,omitempty"`
	Action    string `json:"action"`
	Status    string `json:"status"`
	Details   string `json:"details,omitempty"`
}

// AccessPattern summarizes data access patterns for a topic.
type AccessPattern struct {
	Topic          string   `json:"topic"`
	AgentCount     int      `json:"agent_count"`
	Agents         []string `json:"agents"`
	ReadCount      int      `json:"read_count"`
	WriteCount     int      `json:"write_count"`
	LastAccessedAt string   `json:"last_accessed_at"`
}

// LineageSummary captures data lineage for the report.
type LineageSummary struct {
	Topic       string   `json:"topic"`
	Producers   []string `json:"producers"`
	Consumers   []string `json:"consumers"`
	Upstream    []string `json:"upstream_topics"`
	Downstream  []string `json:"downstream_topics"`
}

// AuditReportInput holds all data needed to generate an audit report.
type AuditReportInput struct {
	ClusterName     string           `json:"cluster_name"`
	Regulation      Regulation       `json:"regulation"`
	Scan            *ScanResult      `json:"scan,omitempty"`
	Report          *ComplianceReport `json:"report,omitempty"`
	AuditEntries    []AuditEntry     `json:"audit_entries,omitempty"`
	AccessPatterns  []AccessPattern  `json:"access_patterns,omitempty"`
	LineageSummaries []LineageSummary `json:"lineage_summaries,omitempty"`
}

// AuditReport is the generated exportable compliance audit report.
type AuditReport struct {
	ID               string             `json:"id"`
	Title            string             `json:"title"`
	ClusterName      string             `json:"cluster_name"`
	Regulation       Regulation         `json:"regulation"`
	Format           AuditReportFormat  `json:"format"`
	OverallScore     int                `json:"overall_score"`
	Status           string             `json:"status"`
	ExecutiveSummary string             `json:"executive_summary"`
	Sections         []ReportSection    `json:"sections"`
	GeneratedAt      string             `json:"generated_at"`
	Content          string             `json:"content,omitempty"`
}

// ReportSection is a section in the audit report.
type ReportSection struct {
	Title   string `json:"title"`
	Content string `json:"content"`
	Order   int    `json:"order"`
}

// GenerateAuditReport creates a comprehensive exportable compliance audit report.
func (e *Engine) GenerateAuditReport(input AuditReportInput, format AuditReportFormat) (*AuditReport, error) {
	if input.Regulation == "" {
		return nil, fmt.Errorf("regulation is required")
	}
	if format == "" {
		format = FormatJSON
	}

	// Run a compliance scan if none provided
	report := input.Report
	if report == nil && input.Scan != nil {
		report = &ComplianceReport{
			ClusterName:  input.ClusterName,
			Regulation:   input.Regulation,
			OverallScore: input.Scan.ComplianceScore,
			Violations:   input.Scan.Violations,
			PIIExposures: input.Scan.PIIExposures,
			GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		}
	}

	score := 0
	status := "unknown"
	if report != nil {
		score = report.OverallScore
		status = report.Status
	}

	auditReport := &AuditReport{
		ID:           randid.New("audit-report"),
		Title:        fmt.Sprintf("%s Compliance Audit Report - %s", input.Regulation, input.ClusterName),
		ClusterName:  input.ClusterName,
		Regulation:   input.Regulation,
		Format:       format,
		OverallScore: score,
		Status:       status,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	// Build executive summary
	auditReport.ExecutiveSummary = e.buildExecutiveSummary(input, report)

	// Build sections
	order := 1
	auditReport.Sections = append(auditReport.Sections, ReportSection{
		Title:   "Executive Summary",
		Content: auditReport.ExecutiveSummary,
		Order:   order,
	})
	order++

	if report != nil {
		auditReport.Sections = append(auditReport.Sections, ReportSection{
			Title:   "Compliance Score & Status",
			Content: e.buildScoreSection(report),
			Order:   order,
		})
		order++

		if len(report.Violations) > 0 {
			auditReport.Sections = append(auditReport.Sections, ReportSection{
				Title:   "Violations",
				Content: e.buildViolationsSection(report.Violations),
				Order:   order,
			})
			order++
		}

		if len(report.PIIExposures) > 0 {
			auditReport.Sections = append(auditReport.Sections, ReportSection{
				Title:   "PII Exposure Analysis",
				Content: e.buildPIISection(report.PIIExposures),
				Order:   order,
			})
			order++
		}

		if len(report.RetentionChecks) > 0 {
			auditReport.Sections = append(auditReport.Sections, ReportSection{
				Title:   "Data Retention Compliance",
				Content: e.buildRetentionSection(report.RetentionChecks),
				Order:   order,
			})
			order++
		}
	}

	if len(input.AuditEntries) > 0 {
		auditReport.Sections = append(auditReport.Sections, ReportSection{
			Title:   "Audit Log Summary",
			Content: e.buildAuditLogSection(input.AuditEntries),
			Order:   order,
		})
		order++
	}

	if len(input.AccessPatterns) > 0 {
		auditReport.Sections = append(auditReport.Sections, ReportSection{
			Title:   "Data Access Patterns",
			Content: e.buildAccessSection(input.AccessPatterns),
			Order:   order,
		})
		order++
	}

	if len(input.LineageSummaries) > 0 {
		auditReport.Sections = append(auditReport.Sections, ReportSection{
			Title:   "Data Lineage",
			Content: e.buildLineageSection(input.LineageSummaries),
			Order:   order,
		})
		order++
	}

	if report != nil && len(report.Recommendations) > 0 {
		auditReport.Sections = append(auditReport.Sections, ReportSection{
			Title:   "Recommendations",
			Content: e.buildRecommendationsSection(report.Recommendations),
			Order:   order,
		})
	}

	// Render content based on format
	switch format {
	case FormatHTML:
		auditReport.Content = e.renderHTML(auditReport)
	case FormatText:
		auditReport.Content = e.renderText(auditReport)
	}

	return auditReport, nil
}

func (e *Engine) buildExecutiveSummary(input AuditReportInput, report *ComplianceReport) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("This report presents the %s compliance audit findings for Kafka cluster '%s'. ", input.Regulation, input.ClusterName))

	if report != nil {
		sb.WriteString(fmt.Sprintf("The overall compliance score is %d/100 (status: %s). ", report.OverallScore, report.Status))
		criticalCount := 0
		for _, v := range report.Violations {
			if v.Severity == "critical" {
				criticalCount++
			}
		}
		if criticalCount > 0 {
			sb.WriteString(fmt.Sprintf("There are %d critical violations requiring immediate attention. ", criticalCount))
		}
		if len(report.PIIExposures) > 0 {
			sb.WriteString(fmt.Sprintf("%d PII exposure(s) were detected. ", len(report.PIIExposures)))
		}
	}
	sb.WriteString(fmt.Sprintf("Report generated at %s.", time.Now().UTC().Format(time.RFC3339)))
	return sb.String()
}

func (e *Engine) buildScoreSection(report *ComplianceReport) string {
	return fmt.Sprintf("Compliance Score: %d/100\nStatus: %s\nRegulation: %s\nTopics Analyzed: Based on cluster-wide scan",
		report.OverallScore, report.Status, report.Regulation)
}

func (e *Engine) buildViolationsSection(violations []Violation) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Total Violations: %d\n\n", len(violations)))
	for i, v := range violations {
		sb.WriteString(fmt.Sprintf("[%d] %s (Severity: %s)\n", i+1, v.Type, v.Severity))
		sb.WriteString(fmt.Sprintf("    Topic: %s\n", v.Topic))
		sb.WriteString(fmt.Sprintf("    Description: %s\n", v.Description))
		sb.WriteString(fmt.Sprintf("    Remediation: %s\n\n", v.Remediation))
	}
	return sb.String()
}

func (e *Engine) buildPIISection(exposures []PIIExposure) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Total PII Exposures: %d\n\n", len(exposures)))
	for _, exp := range exposures {
		masked := "NO"
		if exp.IsMasked {
			masked = "YES"
		}
		sb.WriteString(fmt.Sprintf("- Topic: %s | Type: %s | Masked: %s\n", exp.Topic, exp.PIIType, masked))
	}
	return sb.String()
}

func (e *Engine) buildRetentionSection(checks []RetentionCompliance) string {
	var sb strings.Builder
	compliant := 0
	for _, c := range checks {
		if c.Compliant {
			compliant++
		}
	}
	sb.WriteString(fmt.Sprintf("Topics Checked: %d | Compliant: %d | Non-Compliant: %d\n\n", len(checks), compliant, len(checks)-compliant))
	for _, c := range checks {
		status := "COMPLIANT"
		if !c.Compliant {
			status = "NON-COMPLIANT: " + c.Issue
		}
		sb.WriteString(fmt.Sprintf("- %s: %s (retention: %dms)\n", c.Topic, status, c.CurrentRetention))
	}
	return sb.String()
}

func (e *Engine) buildAuditLogSection(entries []AuditEntry) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Total Audit Entries: %d\n\n", len(entries)))
	for _, entry := range entries {
		sb.WriteString(fmt.Sprintf("[%s] Agent: %s | Tool: %s | Status: %s\n", entry.Timestamp, entry.AgentID, entry.Tool, entry.Status))
	}
	return sb.String()
}

func (e *Engine) buildAccessSection(patterns []AccessPattern) string {
	var sb strings.Builder
	for _, p := range patterns {
		sb.WriteString(fmt.Sprintf("Topic: %s | Agents: %d | Reads: %d | Writes: %d\n", p.Topic, p.AgentCount, p.ReadCount, p.WriteCount))
	}
	return sb.String()
}

func (e *Engine) buildLineageSection(summaries []LineageSummary) string {
	var sb strings.Builder
	for _, l := range summaries {
		sb.WriteString(fmt.Sprintf("Topic: %s\n", l.Topic))
		if len(l.Producers) > 0 {
			sb.WriteString(fmt.Sprintf("  Producers: %s\n", strings.Join(l.Producers, ", ")))
		}
		if len(l.Consumers) > 0 {
			sb.WriteString(fmt.Sprintf("  Consumers: %s\n", strings.Join(l.Consumers, ", ")))
		}
		if len(l.Upstream) > 0 {
			sb.WriteString(fmt.Sprintf("  Upstream: %s\n", strings.Join(l.Upstream, ", ")))
		}
		if len(l.Downstream) > 0 {
			sb.WriteString(fmt.Sprintf("  Downstream: %s\n", strings.Join(l.Downstream, ", ")))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func (e *Engine) buildRecommendationsSection(recs []string) string {
	var sb strings.Builder
	for i, r := range recs {
		sb.WriteString(fmt.Sprintf("%d. %s\n", i+1, r))
	}
	return sb.String()
}

func (e *Engine) renderHTML(report *AuditReport) string {
	var sb strings.Builder
	sb.WriteString("<!DOCTYPE html>\n<html><head><meta charset=\"utf-8\">")
	sb.WriteString(fmt.Sprintf("<title>%s</title>", html.EscapeString(report.Title)))
	sb.WriteString("<style>body{font-family:system-ui,sans-serif;max-width:900px;margin:0 auto;padding:2rem;color:#1a1a1a}")
	sb.WriteString("h1{color:#0066cc;border-bottom:2px solid #0066cc;padding-bottom:0.5rem}")
	sb.WriteString("h2{color:#333;margin-top:2rem;border-bottom:1px solid #ddd;padding-bottom:0.3rem}")
	sb.WriteString(".score{font-size:2rem;font-weight:bold;padding:1rem;border-radius:8px;text-align:center;margin:1rem 0}")
	sb.WriteString(".score.good{background:#d4edda;color:#155724}")
	sb.WriteString(".score.warn{background:#fff3cd;color:#856404}")
	sb.WriteString(".score.bad{background:#f8d7da;color:#721c24}")
	sb.WriteString("pre{background:#f5f5f5;padding:1rem;border-radius:4px;overflow-x:auto;white-space:pre-wrap}")
	sb.WriteString(".meta{color:#666;font-size:0.9rem}")
	sb.WriteString("</style></head><body>")
	sb.WriteString(fmt.Sprintf("<h1>%s</h1>", html.EscapeString(report.Title)))
	sb.WriteString(fmt.Sprintf("<p class=\"meta\">Generated: %s | Cluster: %s | Regulation: %s</p>",
		html.EscapeString(report.GeneratedAt), html.EscapeString(report.ClusterName), html.EscapeString(string(report.Regulation))))

	scoreClass := "good"
	if report.OverallScore < 60 {
		scoreClass = "bad"
	} else if report.OverallScore < 90 {
		scoreClass = "warn"
	}
	sb.WriteString(fmt.Sprintf("<div class=\"score %s\">Compliance Score: %d/100 — %s</div>",
		scoreClass, report.OverallScore, html.EscapeString(report.Status)))

	for _, section := range report.Sections {
		sb.WriteString(fmt.Sprintf("<h2>%s</h2>", html.EscapeString(section.Title)))
		sb.WriteString(fmt.Sprintf("<pre>%s</pre>", html.EscapeString(section.Content)))
	}

	sb.WriteString("</body></html>")
	return sb.String()
}

func (e *Engine) renderText(report *AuditReport) string {
	var sb strings.Builder
	sb.WriteString(strings.Repeat("=", 60) + "\n")
	sb.WriteString(report.Title + "\n")
	sb.WriteString(strings.Repeat("=", 60) + "\n\n")
	sb.WriteString(fmt.Sprintf("Generated: %s\nCluster: %s\nRegulation: %s\nScore: %d/100\nStatus: %s\n\n",
		report.GeneratedAt, report.ClusterName, report.Regulation, report.OverallScore, report.Status))

	for _, section := range report.Sections {
		sb.WriteString(strings.Repeat("-", 40) + "\n")
		sb.WriteString(section.Title + "\n")
		sb.WriteString(strings.Repeat("-", 40) + "\n")
		sb.WriteString(section.Content + "\n\n")
	}
	return sb.String()
}
