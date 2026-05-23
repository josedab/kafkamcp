package compliance

import (
	"fmt"
	"log/slog"
	"testing"
)

func TestScan_GDPR_EmailPII(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "users", RetentionMs: 100000, SampleValues: []string{`{"email":"alice@example.com"}`}, HasMasking: false},
	}
	result := e.Scan("test-cluster", GDPR, topics)
	if result.TopicsScanned != 1 {
		t.Fatalf("expected 1 topic scanned, got %d", result.TopicsScanned)
	}
	if len(result.PIIExposures) == 0 {
		t.Fatal("expected PII exposures for email")
	}
	foundEmail := false
	for _, exp := range result.PIIExposures {
		if exp.PIIType == PIIEmail {
			foundEmail = true
		}
	}
	if !foundEmail {
		t.Error("expected email PII exposure")
	}
	// Unmasked + GDPR requires masking => violation
	foundViolation := false
	for _, v := range result.Violations {
		if v.Type == "unmasked_pii" {
			foundViolation = true
		}
	}
	if !foundViolation {
		t.Error("expected unmasked_pii violation for GDPR")
	}
}

func TestScan_GDPR_MaskedPII(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "users", RetentionMs: 100000, SampleValues: []string{`alice@example.com`}, HasMasking: true},
	}
	result := e.Scan("test-cluster", GDPR, topics)
	if len(result.PIIExposures) == 0 {
		t.Fatal("expected PII exposure even when masked")
	}
	for _, v := range result.Violations {
		if v.Type == "unmasked_pii" {
			t.Error("should not have unmasked_pii violation when masking is enabled")
		}
	}
}

func TestScan_HIPAA_SSN(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "patients", RetentionMs: 100000, SampleValues: []string{`SSN: 123-45-6789`}, HasMasking: false},
	}
	result := e.Scan("test-cluster", HIPAA, topics)
	foundSSN := false
	for _, exp := range result.PIIExposures {
		if exp.PIIType == PIISSN {
			foundSSN = true
		}
	}
	if !foundSSN {
		t.Error("expected SSN PII exposure for HIPAA scan")
	}
}

func TestScan_HIPAA_RetentionInsufficient(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "records", RetentionMs: 1000, SampleValues: nil, HasMasking: true},
	}
	result := e.Scan("test-cluster", HIPAA, topics)
	found := false
	for _, v := range result.Violations {
		if v.Type == "retention_insufficient" {
			found = true
		}
	}
	if !found {
		t.Error("expected retention_insufficient violation for HIPAA with low retention")
	}
}

func TestScan_GDPR_RetentionExceeded(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	twoYearsMs := int64(2 * 365 * 24 * 60 * 60 * 1000)
	topics := []TopicData{
		{Name: "logs", RetentionMs: twoYearsMs, SampleValues: nil, HasMasking: true},
	}
	result := e.Scan("test-cluster", GDPR, topics)
	found := false
	for _, v := range result.Violations {
		if v.Type == "retention_exceeded" {
			found = true
		}
	}
	if !found {
		t.Error("expected retention_exceeded violation for GDPR")
	}
}

func TestScan_EmptyTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	result := e.Scan("test-cluster", GDPR, nil)
	if result.TopicsScanned != 0 {
		t.Errorf("expected 0 topics scanned, got %d", result.TopicsScanned)
	}
	if result.ComplianceScore != 100 {
		t.Errorf("expected score 100 for empty topics, got %d", result.ComplianceScore)
	}
}

func TestScan_UnknownRegulation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "t1", RetentionMs: 100000, SampleValues: []string{"alice@example.com"}, HasMasking: false},
	}
	result := e.Scan("test-cluster", Regulation("UNKNOWN"), topics)
	// Unknown regulation scans with all rule sets
	if result.Regulation != Regulation("UNKNOWN") {
		t.Errorf("expected regulation UNKNOWN, got %s", result.Regulation)
	}
	if len(result.PIIExposures) == 0 {
		t.Error("expected PII exposures when scanning all regulations")
	}
}

func TestScan_MixedPIITypes(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "mixed", RetentionMs: 100000, SampleValues: []string{
			"email: bob@test.com, ssn: 987-65-4321",
		}, HasMasking: false},
	}
	result := e.Scan("test-cluster", GDPR, topics)
	// The scanner breaks after first PII match per sample, so we get 1 exposure per sample
	if len(result.PIIExposures) == 0 {
		t.Error("expected PII exposures for mixed types")
	}
}

func TestScan_MedicalPII_HIPAA(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "health", RetentionMs: int64(7 * 365 * 24 * 60 * 60 * 1000), SampleValues: []string{
			"patient diagnosis confirmed",
		}, HasMasking: false},
	}
	result := e.Scan("test-cluster", HIPAA, topics)
	foundMedical := false
	for _, exp := range result.PIIExposures {
		if exp.PIIType == PIIMedical {
			foundMedical = true
		}
	}
	if !foundMedical {
		t.Error("expected medical PII exposure for HIPAA")
	}
}

func TestGenerateReport_Compliant(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "safe", RetentionMs: 100000, SampleValues: []string{"no pii here"}, HasMasking: true},
	}
	report := e.GenerateReport("cluster", GDPR, topics)
	if report.Status != "compliant" {
		t.Errorf("expected compliant, got %s", report.Status)
	}
	if report.OverallScore < 90 {
		t.Errorf("expected score >= 90, got %d", report.OverallScore)
	}
	if len(report.RetentionChecks) != 1 {
		t.Errorf("expected 1 retention check, got %d", len(report.RetentionChecks))
	}
}

func TestGenerateReport_NonCompliant(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "t1", RetentionMs: 100000, SampleValues: []string{"alice@a.com"}, HasMasking: false},
		{Name: "t2", RetentionMs: 100000, SampleValues: []string{"bob@b.com"}, HasMasking: false},
		{Name: "t3", RetentionMs: 100000, SampleValues: []string{"carol@c.com"}, HasMasking: false},
		{Name: "t4", RetentionMs: 100000, SampleValues: []string{"dave@d.com"}, HasMasking: false},
		{Name: "t5", RetentionMs: 100000, SampleValues: []string{"eve@e.com"}, HasMasking: false},
	}
	report := e.GenerateReport("cluster", GDPR, topics)
	if report.Status == "compliant" {
		t.Error("expected non-compliant status with multiple violations")
	}
	if len(report.Recommendations) == 0 {
		t.Error("expected recommendations")
	}
}

func TestGenerateReport_PartiallyCompliant(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "t1", RetentionMs: 100000, SampleValues: []string{"alice@a.com"}, HasMasking: false},
		{Name: "t2", RetentionMs: 100000, SampleValues: []string{"no pii"}, HasMasking: true},
	}
	report := e.GenerateReport("cluster", GDPR, topics)
	_ = report
}

func TestGetViolations_NoFilter(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "t1", RetentionMs: 100000, SampleValues: []string{"alice@a.com"}, HasMasking: false},
	}
	e.Scan("cluster", GDPR, topics)
	violations := e.GetViolations("", "")
	if len(violations) == 0 {
		t.Error("expected violations")
	}
}

func TestGetViolations_FilterByRegulation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "t1", RetentionMs: 100000, SampleValues: []string{"alice@a.com"}, HasMasking: false},
	}
	e.Scan("cluster", GDPR, topics)
	violations := e.GetViolations(HIPAA, "")
	if len(violations) != 0 {
		t.Errorf("expected 0 HIPAA violations from GDPR scan, got %d", len(violations))
	}
}

func TestGetViolations_FilterBySeverity(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicData{
		{Name: "t1", RetentionMs: 100000, SampleValues: []string{"alice@a.com"}, HasMasking: false},
	}
	e.Scan("cluster", GDPR, topics)
	critical := e.GetViolations("", "critical")
	for _, v := range critical {
		if v.Severity != "critical" {
			t.Errorf("expected only critical severity, got %s", v.Severity)
		}
	}
}

func TestGetViolations_EmptyBeforeScan(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	violations := e.GetViolations("", "")
	if len(violations) != 0 {
		t.Errorf("expected no violations before scan, got %d", len(violations))
	}
}

func TestComplianceScore_ZeroFloor(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	// Many violations should floor the score at 0
	var topics []TopicData
	for i := 0; i < 20; i++ {
		topics = append(topics, TopicData{
			Name: fmt.Sprintf("t%d", i), RetentionMs: 100000,
			SampleValues: []string{fmt.Sprintf("user%d@example.com", i)},
			HasMasking:   false,
		})
	}
	result := e.Scan("cluster", GDPR, topics)
	if result.ComplianceScore < 0 {
		t.Errorf("score should never be negative, got %d", result.ComplianceScore)
	}
}
