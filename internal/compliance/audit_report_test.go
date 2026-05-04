package compliance

import (
	"log/slog"
	"strings"
	"testing"
)

func TestGenerateAuditReport_HTML(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	topics := []TopicData{
		{Name: "orders", RetentionMs: 86400000, SampleValues: []string{"user@example.com ordered item"}, HasMasking: false},
		{Name: "logs", RetentionMs: 604800000},
	}
	report := e.GenerateReport("test-cluster", GDPR, topics)

	input := AuditReportInput{
		ClusterName: "test-cluster",
		Regulation:  GDPR,
		Report:      report,
		AuditEntries: []AuditEntry{
			{Timestamp: "2024-01-01T00:00:00Z", AgentID: "agent-1", Tool: "kafka_consume", Status: "success"},
		},
		AccessPatterns: []AccessPattern{
			{Topic: "orders", AgentCount: 2, Agents: []string{"a1", "a2"}, ReadCount: 100, WriteCount: 50},
		},
		LineageSummaries: []LineageSummary{
			{Topic: "orders", Producers: []string{"order-service"}, Consumers: []string{"analytics"}},
		},
	}

	auditReport, err := e.GenerateAuditReport(input, FormatHTML)
	if err != nil {
		t.Fatalf("GenerateAuditReport: %v", err)
	}
	if auditReport.Content == "" {
		t.Error("expected HTML content")
	}
	if !strings.Contains(auditReport.Content, "<!DOCTYPE html>") {
		t.Error("expected valid HTML document")
	}
	if auditReport.OverallScore == 0 && len(report.Violations) == 0 {
		t.Error("expected non-zero score when no violations")
	}
	if len(auditReport.Sections) < 3 {
		t.Errorf("expected at least 3 sections, got %d", len(auditReport.Sections))
	}
}

func TestGenerateAuditReport_HTMLEscaping(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	input := AuditReportInput{
		ClusterName: `<script>alert("xss")</script>`,
		Regulation:  GDPR,
		Report: &ComplianceReport{
			ClusterName:  `<script>alert("xss")</script>`,
			Regulation:   GDPR,
			OverallScore: 50,
			Status:       `non_compliant&"dangerous"`,
		},
	}

	auditReport, err := e.GenerateAuditReport(input, FormatHTML)
	if err != nil {
		t.Fatalf("GenerateAuditReport: %v", err)
	}
	if strings.Contains(auditReport.Content, "<script>") {
		t.Error("HTML content contains unescaped script tag — XSS vulnerability")
	}
	if !strings.Contains(auditReport.Content, "&lt;script&gt;") {
		t.Error("expected script tags to be HTML-escaped")
	}
}

func TestGenerateAuditReport_Text(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	input := AuditReportInput{
		ClusterName: "prod",
		Regulation:  SOC2,
		Report: &ComplianceReport{
			ClusterName:     "prod",
			Regulation:      SOC2,
			OverallScore:    85,
			Status:          "partially_compliant",
			Recommendations: []string{"Enable encryption at rest"},
		},
	}

	auditReport, err := e.GenerateAuditReport(input, FormatText)
	if err != nil {
		t.Fatalf("GenerateAuditReport: %v", err)
	}
	if auditReport.Content == "" {
		t.Error("expected text content")
	}
	if !strings.Contains(auditReport.Content, "Score: 85/100") {
		t.Error("expected score in text output")
	}
}

func TestGenerateAuditReport_JSON(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	input := AuditReportInput{
		ClusterName: "prod",
		Regulation:  HIPAA,
		Report: &ComplianceReport{
			ClusterName:  "prod",
			Regulation:   HIPAA,
			OverallScore: 95,
			Status:       "compliant",
		},
	}

	auditReport, err := e.GenerateAuditReport(input, FormatJSON)
	if err != nil {
		t.Fatalf("GenerateAuditReport: %v", err)
	}
	// JSON format should not set Content field (returned as structured data)
	if auditReport.Format != FormatJSON {
		t.Errorf("expected json format, got %s", auditReport.Format)
	}
	if auditReport.OverallScore != 95 {
		t.Errorf("expected score 95, got %d", auditReport.OverallScore)
	}
}

func TestGenerateAuditReport_DefaultFormat(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	input := AuditReportInput{
		ClusterName: "prod",
		Regulation:  SOC2,
	}

	auditReport, err := e.GenerateAuditReport(input, "")
	if err != nil {
		t.Fatalf("GenerateAuditReport: %v", err)
	}
	if auditReport.Format != FormatJSON {
		t.Errorf("expected default json format, got %s", auditReport.Format)
	}
}

func TestGenerateAuditReport_Validation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	_, err := e.GenerateAuditReport(AuditReportInput{}, FormatJSON)
	if err == nil {
		t.Error("expected error for missing regulation")
	}
}

func TestGenerateAuditReport_FromScanResult(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	topics := []TopicData{
		{Name: "sensitive", RetentionMs: 86400000, SampleValues: []string{"patient diagnosed with flu"}, HasMasking: false},
	}
	scan := e.Scan("medical-cluster", HIPAA, topics)

	input := AuditReportInput{
		ClusterName: "medical-cluster",
		Regulation:  HIPAA,
		Scan:        scan,
	}

	auditReport, err := e.GenerateAuditReport(input, FormatHTML)
	if err != nil {
		t.Fatalf("GenerateAuditReport: %v", err)
	}
	if auditReport.OverallScore == 0 {
		t.Error("expected computed score from scan")
	}
}

func TestGenerateAuditReport_EmptyCluster(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	input := AuditReportInput{
		Regulation: GDPR,
	}

	auditReport, err := e.GenerateAuditReport(input, FormatText)
	if err != nil {
		t.Fatalf("GenerateAuditReport: %v", err)
	}
	if auditReport.ClusterName != "" {
		t.Errorf("expected empty cluster name, got %q", auditReport.ClusterName)
	}
}
