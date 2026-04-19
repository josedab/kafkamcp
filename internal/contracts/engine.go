package contracts

import (
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// Contract defines a schema-aware data contract for a Kafka topic.
type Contract struct {
	Name        string        `json:"name"`
	Version     string        `json:"version"`
	Topic       string        `json:"topic"`
	Owner       string        `json:"owner"`
	Description string        `json:"description"`
	Schema      SchemaRef     `json:"schema"`
	SLA         SLA           `json:"sla"`
	Quality     []QualityRule `json:"quality"`
	CreatedAt   string        `json:"created_at"`
	UpdatedAt   string        `json:"updated_at"`
}

// SchemaRef references a schema in the registry.
type SchemaRef struct {
	Subject     string `json:"subject"`
	Version     string `json:"version"`
	CompatLevel string `json:"compat_level"`
}

// SLA defines service-level agreements for the contract.
type SLA struct {
	MaxLatencyMs    int64   `json:"max_latency_ms"`
	MinThroughput   float64 `json:"min_throughput"`
	MaxErrorRate    float64 `json:"max_error_rate"`
	RetentionHours  int     `json:"retention_hours"`
	AvailabilityPct float64 `json:"availability_pct"`
}

// QualityRule defines a data quality rule for field validation.
// Supported rules: not_null, enum, regex, min_length, max_length.
type QualityRule struct {
	Field    string `json:"field"`
	Rule     string `json:"rule"`
	Value    string `json:"value"`
	Severity string `json:"severity"`
}

// ValidationResult holds the outcome of validating a message against a contract.
type ValidationResult struct {
	ContractName string      `json:"contract_name"`
	Topic        string      `json:"topic"`
	Valid        bool        `json:"valid"`
	Violations   []Violation `json:"violations"`
	Timestamp    string      `json:"timestamp"`
}

// Violation describes a single quality rule violation.
type Violation struct {
	Rule     string `json:"rule"`
	Field    string `json:"field"`
	Message  string `json:"message"`
	Severity string `json:"severity"`
}

// ComplianceReport tracks aggregate compliance statistics for a topic.
type ComplianceReport struct {
	ContractName    string         `json:"contract_name"`
	Topic           string         `json:"topic"`
	TotalChecked    int            `json:"total_checked"`
	PassedCount     int            `json:"passed_count"`
	FailedCount     int            `json:"failed_count"`
	ComplianceRate  float64        `json:"compliance_rate"`
	ViolationsByRule map[string]int `json:"violations_by_rule"`
	Timestamp       string         `json:"timestamp"`
}

// Engine manages data contracts and validates messages against them.
type Engine struct {
	mu        sync.RWMutex
	contracts map[string]*Contract
	stats     map[string]*ComplianceReport
}

// NewEngine creates a new contract engine.
func NewEngine() *Engine {
	return &Engine{
		contracts: make(map[string]*Contract),
		stats:     make(map[string]*ComplianceReport),
	}
}

// RegisterContract registers a data contract. It validates that required fields
// (Name, Version, Topic, Owner) are present.
func (e *Engine) RegisterContract(c Contract) error {
	if c.Name == "" {
		return fmt.Errorf("contract name is required")
	}
	if c.Version == "" {
		return fmt.Errorf("contract version is required")
	}
	if c.Topic == "" {
		return fmt.Errorf("contract topic is required")
	}
	if c.Owner == "" {
		return fmt.Errorf("contract owner is required")
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if c.CreatedAt == "" {
		c.CreatedAt = now
	}
	c.UpdatedAt = now

	e.mu.Lock()
	defer e.mu.Unlock()

	stored := c
	e.contracts[c.Topic] = &stored
	return nil
}

// GetContract retrieves the contract registered for the given topic.
func (e *Engine) GetContract(topic string) (*Contract, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	c, ok := e.contracts[topic]
	if !ok {
		return nil, fmt.Errorf("no contract found for topic %q", topic)
	}
	return c, nil
}

// ListContracts returns all registered contracts.
func (e *Engine) ListContracts() []Contract {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]Contract, 0, len(e.contracts))
	for _, c := range e.contracts {
		result = append(result, *c)
	}
	return result
}

// ValidateMessage validates a message value against the contract for the given topic.
// Returns nil if no contract is registered for the topic.
func (e *Engine) ValidateMessage(topic string, value map[string]any) *ValidationResult {
	e.mu.RLock()
	c, ok := e.contracts[topic]
	e.mu.RUnlock()

	if !ok {
		return nil
	}

	result := &ValidationResult{
		ContractName: c.Name,
		Topic:        topic,
		Valid:        true,
		Violations:   []Violation{},
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	}

	for _, rule := range c.Quality {
		if v := e.checkRule(rule, value); v != nil {
			result.Violations = append(result.Violations, *v)
			result.Valid = false
		}
	}

	e.recordCompliance(topic, c.Name, result)
	return result
}

// checkRule evaluates a single quality rule against the message value.
func (e *Engine) checkRule(rule QualityRule, value map[string]any) *Violation {
	fieldVal, exists := value[rule.Field]

	switch rule.Rule {
	case "not_null":
		if !exists || fieldVal == nil || fieldVal == "" {
			return &Violation{
				Rule:     rule.Rule,
				Field:    rule.Field,
				Message:  fmt.Sprintf("field %q must not be null or empty", rule.Field),
				Severity: rule.Severity,
			}
		}

	case "enum":
		str, _ := fieldVal.(string) //nolint:errcheck // safe: zero value if not string
		if !exists || str == "" {
			return &Violation{
				Rule:     rule.Rule,
				Field:    rule.Field,
				Message:  fmt.Sprintf("field %q is missing or empty, expected one of: %s", rule.Field, rule.Value),
				Severity: rule.Severity,
			}
		}
		allowed := splitCSV(rule.Value)
		found := false
		for _, a := range allowed {
			if str == a {
				found = true
				break
			}
		}
		if !found {
			return &Violation{
				Rule:     rule.Rule,
				Field:    rule.Field,
				Message:  fmt.Sprintf("field %q value %q not in allowed values: %s", rule.Field, str, rule.Value),
				Severity: rule.Severity,
			}
		}

	case "regex":
		str, _ := fieldVal.(string) //nolint:errcheck // safe: zero value if not string
		if !exists {
			return &Violation{
				Rule:     rule.Rule,
				Field:    rule.Field,
				Message:  fmt.Sprintf("field %q is missing", rule.Field),
				Severity: rule.Severity,
			}
		}
		re, err := regexp.Compile(rule.Value)
		if err != nil {
			return &Violation{
				Rule:     rule.Rule,
				Field:    rule.Field,
				Message:  fmt.Sprintf("invalid regex pattern %q: %v", rule.Value, err),
				Severity: rule.Severity,
			}
		}
		if !re.MatchString(str) {
			return &Violation{
				Rule:     rule.Rule,
				Field:    rule.Field,
				Message:  fmt.Sprintf("field %q value %q does not match pattern %q", rule.Field, str, rule.Value),
				Severity: rule.Severity,
			}
		}

	case "min_length":
		str, _ := fieldVal.(string) //nolint:errcheck // safe: zero value if not string
		minLen, err := strconv.Atoi(rule.Value)
		if err != nil {
			return &Violation{
				Rule:     rule.Rule,
				Field:    rule.Field,
				Message:  fmt.Sprintf("invalid min_length value %q: %v", rule.Value, err),
				Severity: rule.Severity,
			}
		}
		if !exists || len(str) < minLen {
			return &Violation{
				Rule:     rule.Rule,
				Field:    rule.Field,
				Message:  fmt.Sprintf("field %q length %d is below minimum %d", rule.Field, len(str), minLen),
				Severity: rule.Severity,
			}
		}

	case "max_length":
		str, _ := fieldVal.(string) //nolint:errcheck // safe: zero value if not string
		maxLen, err := strconv.Atoi(rule.Value)
		if err != nil {
			return &Violation{
				Rule:     rule.Rule,
				Field:    rule.Field,
				Message:  fmt.Sprintf("invalid max_length value %q: %v", rule.Value, err),
				Severity: rule.Severity,
			}
		}
		if exists && len(str) > maxLen {
			return &Violation{
				Rule:     rule.Rule,
				Field:    rule.Field,
				Message:  fmt.Sprintf("field %q length %d exceeds maximum %d", rule.Field, len(str), maxLen),
				Severity: rule.Severity,
			}
		}
	}

	return nil
}

// recordCompliance updates the compliance report for a topic.
func (e *Engine) recordCompliance(topic, contractName string, result *ValidationResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	report, ok := e.stats[topic]
	if !ok {
		report = &ComplianceReport{
			ContractName:    contractName,
			Topic:           topic,
			ViolationsByRule: make(map[string]int),
		}
		e.stats[topic] = report
	}

	report.TotalChecked++
	if result.Valid {
		report.PassedCount++
	} else {
		report.FailedCount++
		for _, v := range result.Violations {
			report.ViolationsByRule[v.Rule]++
		}
	}

	if report.TotalChecked > 0 {
		report.ComplianceRate = float64(report.PassedCount) / float64(report.TotalChecked) * 100.0
	}
	report.Timestamp = result.Timestamp
}

// GetComplianceReport returns the compliance report for the given topic.
func (e *Engine) GetComplianceReport(topic string) (*ComplianceReport, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	report, ok := e.stats[topic]
	if !ok {
		return nil, fmt.Errorf("no compliance report found for topic %q", topic)
	}
	return report, nil
}

// splitCSV splits a comma-separated string into trimmed parts.
func splitCSV(s string) []string {
	parts := make([]string, 0)
	for _, p := range regexp.MustCompile(`,\s*`).Split(s, -1) {
		if p != "" {
			parts = append(parts, p)
		}
	}
	return parts
}
