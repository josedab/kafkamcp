package compliance

import (
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"time"
)

// Regulation defines supported compliance frameworks.
type Regulation string

// GDPR and related values define constants for this package.
const (
	GDPR   Regulation = "GDPR"
	HIPAA  Regulation = "HIPAA"
	SOC2   Regulation = "SOC2"
	PCIDSS Regulation = "PCI-DSS"
)

// PIIType categorizes personal data.
type PIIType string

// PIIEmail and related values define constants for this package.
const (
	PIIEmail       PIIType = "email"
	PIIPhone       PIIType = "phone"
	PIISSN         PIIType = "ssn"
	PIICreditCard  PIIType = "credit_card"
	PIIAddress     PIIType = "address"
	PIIName        PIIType = "person_name"
	PIIDateOfBirth PIIType = "date_of_birth"
	PIIIP          PIIType = "ip_address"
	PIIMedical     PIIType = "medical_record"
)

// Violation represents a compliance violation.
type Violation struct {
	ID          string     `json:"id"`
	Regulation  Regulation `json:"regulation"`
	Severity    string     `json:"severity"` // critical, high, medium, low
	Type        string     `json:"type"`
	Topic       string     `json:"topic"`
	Description string     `json:"description"`
	PIITypes    []PIIType  `json:"pii_types,omitempty"`
	Remediation string     `json:"remediation"`
	DetectedAt  string     `json:"detected_at"`
}

// ScanResult captures the outcome of a compliance scan.
type ScanResult struct {
	ClusterName     string        `json:"cluster_name"`
	Regulation      Regulation    `json:"regulation,omitempty"`
	TopicsScanned   int           `json:"topics_scanned"`
	Violations      []Violation   `json:"violations"`
	PIIExposures    []PIIExposure `json:"pii_exposures"`
	ComplianceScore int           `json:"compliance_score"` // 0-100
	ScannedAt       string        `json:"scanned_at"`
}

// PIIExposure records PII found in a topic.
type PIIExposure struct {
	Topic       string  `json:"topic"`
	PIIType     PIIType `json:"pii_type"`
	FieldPath   string  `json:"field_path,omitempty"`
	SampleCount int     `json:"sample_count"`
	IsMasked    bool    `json:"is_masked"`
	DetectedAt  string  `json:"detected_at"`
}

// RetentionCompliance checks topic retention against regulations.
type RetentionCompliance struct {
	Topic            string     `json:"topic"`
	Regulation       Regulation `json:"regulation"`
	CurrentRetention int64      `json:"current_retention_ms"`
	RequiredMax      int64      `json:"required_max_retention_ms"`
	RequiredMin      int64      `json:"required_min_retention_ms"`
	Compliant        bool       `json:"compliant"`
	Issue            string     `json:"issue,omitempty"`
}

// ComplianceReport is a comprehensive report.
type ComplianceReport struct {
	ClusterName     string                `json:"cluster_name"`
	Regulation      Regulation            `json:"regulation"`
	OverallScore    int                   `json:"overall_score"`
	Status          string                `json:"status"` // compliant, non_compliant, partially_compliant
	Violations      []Violation           `json:"violations"`
	PIIExposures    []PIIExposure         `json:"pii_exposures"`
	RetentionChecks []RetentionCompliance `json:"retention_checks"`
	Recommendations []string              `json:"recommendations"`
	GeneratedAt     string                `json:"generated_at"`
}

// TopicData holds topic info for compliance scanning.
type TopicData struct {
	Name         string
	RetentionMs  int64
	SampleValues []string
	HasMasking   bool
}

// RuleSet defines compliance rules for a regulation.
type RuleSet struct {
	Regulation     Regulation
	MaxRetentionMs int64
	MinRetentionMs int64
	RequireMasking bool
	RequireEncrypt bool
	PIIDetectors   []PIIDetector
}

// PIIDetector detects a specific type of PII.
type PIIDetector struct {
	Type    PIIType
	Pattern *regexp.Regexp
}

// Engine manages compliance scanning and reporting.
type Engine struct {
	ruleSets   map[Regulation]*RuleSet
	violations []Violation
	logger     *slog.Logger
	mu         sync.RWMutex
}

// NewEngine creates a new compliance engine.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		ruleSets: make(map[Regulation]*RuleSet),
		logger:   logger,
	}
	e.initRuleSets()
	return e
}

func (e *Engine) initRuleSets() {
	detectors := []PIIDetector{
		{PIIEmail, regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)},
		{PIIPhone, regexp.MustCompile(`\b(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b`)},
		{PIISSN, regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)},
		{PIICreditCard, regexp.MustCompile(`\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b`)},
		{PIIIP, regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b`)},
	}

	e.ruleSets[GDPR] = &RuleSet{
		Regulation:     GDPR,
		MaxRetentionMs: 365 * 24 * 60 * 60 * 1000, // 1 year
		RequireMasking: true,
		PIIDetectors:   detectors,
	}

	e.ruleSets[HIPAA] = &RuleSet{
		Regulation:     HIPAA,
		MinRetentionMs: 6 * 365 * 24 * 60 * 60 * 1000, // 6 years
		RequireMasking: true,
		RequireEncrypt: true,
		PIIDetectors:   append(detectors, PIIDetector{PIIMedical, regexp.MustCompile(`(?i)\b(diagnosis|patient|medical|health|prescription)\b`)}),
	}

	e.ruleSets[SOC2] = &RuleSet{
		Regulation:     SOC2,
		RequireMasking: true,
		PIIDetectors:   detectors,
	}

	e.ruleSets[PCIDSS] = &RuleSet{
		Regulation:     PCIDSS,
		MaxRetentionMs: 365 * 24 * 60 * 60 * 1000,
		RequireMasking: true,
		RequireEncrypt: true,
		PIIDetectors:   append(detectors[:0:0], detectors...), // copy
	}
}

// Scan performs a compliance scan on topics.
func (e *Engine) Scan(clusterName string, regulation Regulation, topics []TopicData) *ScanResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	result := &ScanResult{
		ClusterName:   clusterName,
		Regulation:    regulation,
		TopicsScanned: len(topics),
		ScannedAt:     time.Now().UTC().Format(time.RFC3339),
	}

	ruleSet := e.ruleSets[regulation]
	if ruleSet == nil {
		// If no specific regulation, scan all
		for _, rs := range e.ruleSets {
			e.scanWithRuleSet(rs, topics, result)
		}
	} else {
		e.scanWithRuleSet(ruleSet, topics, result)
	}

	// Calculate score
	result.ComplianceScore = e.calculateComplianceScore(result)

	// Store violations for later querying
	e.violations = append(e.violations, result.Violations...)

	return result
}

func (e *Engine) scanWithRuleSet(rs *RuleSet, topics []TopicData, result *ScanResult) {
	for _, topic := range topics {
		// Retention compliance
		if rs.MaxRetentionMs > 0 && topic.RetentionMs > rs.MaxRetentionMs {
			result.Violations = append(result.Violations, Violation{
				ID:          fmt.Sprintf("ret-max-%s-%s", rs.Regulation, topic.Name),
				Regulation:  rs.Regulation,
				Severity:    "high",
				Type:        "retention_exceeded",
				Topic:       topic.Name,
				Description: fmt.Sprintf("Topic retention (%d ms) exceeds %s maximum (%d ms)", topic.RetentionMs, rs.Regulation, rs.MaxRetentionMs),
				Remediation: "Reduce topic retention.ms to comply with data retention limits",
				DetectedAt:  time.Now().UTC().Format(time.RFC3339),
			})
		}
		if rs.MinRetentionMs > 0 && topic.RetentionMs > 0 && topic.RetentionMs < rs.MinRetentionMs {
			result.Violations = append(result.Violations, Violation{
				ID:          fmt.Sprintf("ret-min-%s-%s", rs.Regulation, topic.Name),
				Regulation:  rs.Regulation,
				Severity:    "high",
				Type:        "retention_insufficient",
				Topic:       topic.Name,
				Description: fmt.Sprintf("Topic retention (%d ms) below %s minimum (%d ms)", topic.RetentionMs, rs.Regulation, rs.MinRetentionMs),
				Remediation: "Increase topic retention.ms to meet minimum retention requirements",
				DetectedAt:  time.Now().UTC().Format(time.RFC3339),
			})
		}

		// PII exposure detection
		for _, sample := range topic.SampleValues {
			for _, detector := range rs.PIIDetectors {
				if detector.Pattern.MatchString(sample) {
					exposure := PIIExposure{
						Topic:       topic.Name,
						PIIType:     detector.Type,
						SampleCount: 1,
						IsMasked:    topic.HasMasking,
						DetectedAt:  time.Now().UTC().Format(time.RFC3339),
					}
					result.PIIExposures = append(result.PIIExposures, exposure)

					if !topic.HasMasking && rs.RequireMasking {
						result.Violations = append(result.Violations, Violation{
							ID:          fmt.Sprintf("pii-%s-%s-%s", rs.Regulation, topic.Name, detector.Type),
							Regulation:  rs.Regulation,
							Severity:    "critical",
							Type:        "unmasked_pii",
							Topic:       topic.Name,
							PIITypes:    []PIIType{detector.Type},
							Description: fmt.Sprintf("Unmasked %s data detected in topic %q", detector.Type, topic.Name),
							Remediation: "Enable data masking for this topic via kafkamcp masking configuration",
							DetectedAt:  time.Now().UTC().Format(time.RFC3339),
						})
					}
					break
				}
			}
		}
	}
}

// GenerateReport creates a comprehensive compliance report.
func (e *Engine) GenerateReport(clusterName string, regulation Regulation, topics []TopicData) *ComplianceReport {
	scan := e.Scan(clusterName, regulation, topics)

	report := &ComplianceReport{
		ClusterName:  clusterName,
		Regulation:   regulation,
		OverallScore: scan.ComplianceScore,
		Violations:   scan.Violations,
		PIIExposures: scan.PIIExposures,
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	// Status determination
	if scan.ComplianceScore >= 90 {
		report.Status = "compliant"
	} else if scan.ComplianceScore >= 60 {
		report.Status = "partially_compliant"
	} else {
		report.Status = "non_compliant"
	}

	// Retention checks
	rs := e.ruleSets[regulation]
	if rs != nil {
		for _, t := range topics {
			check := RetentionCompliance{
				Topic:            t.Name,
				Regulation:       regulation,
				CurrentRetention: t.RetentionMs,
				RequiredMax:      rs.MaxRetentionMs,
				RequiredMin:      rs.MinRetentionMs,
				Compliant:        true,
			}
			if rs.MaxRetentionMs > 0 && t.RetentionMs > rs.MaxRetentionMs {
				check.Compliant = false
				check.Issue = "exceeds maximum retention"
			}
			if rs.MinRetentionMs > 0 && t.RetentionMs > 0 && t.RetentionMs < rs.MinRetentionMs {
				check.Compliant = false
				check.Issue = "below minimum retention"
			}
			report.RetentionChecks = append(report.RetentionChecks, check)
		}
	}

	// Recommendations
	if len(scan.PIIExposures) > 0 {
		report.Recommendations = append(report.Recommendations, "Enable PII masking for topics containing personal data")
	}
	criticalCount := 0
	for _, v := range scan.Violations {
		if v.Severity == "critical" {
			criticalCount++
		}
	}
	if criticalCount > 0 {
		report.Recommendations = append(report.Recommendations, fmt.Sprintf("Address %d critical violations immediately", criticalCount))
	}
	report.Recommendations = append(report.Recommendations, "Schedule regular compliance scans (recommended: weekly)")

	return report
}

// GetViolations returns stored violations with optional filters.
func (e *Engine) GetViolations(regulation Regulation, severity string) []Violation {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Violation
	for _, v := range e.violations {
		if regulation != "" && v.Regulation != regulation {
			continue
		}
		if severity != "" && v.Severity != severity {
			continue
		}
		result = append(result, v)
	}
	return result
}

func (e *Engine) calculateComplianceScore(result *ScanResult) int {
	if result.TopicsScanned == 0 {
		return 100
	}

	score := 100
	for _, v := range result.Violations {
		switch v.Severity {
		case "critical":
			score -= 20
		case "high":
			score -= 10
		case "medium":
			score -= 5
		case "low":
			score -= 2
		}
	}
	if score < 0 {
		score = 0
	}
	return score
}
