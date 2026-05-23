package contracts

import (
	"fmt"
	"time"
)

// EnforcementMode defines how contract violations are handled.
type EnforcementMode string

// ModeWarn and related values define constants for this package.
const (
	ModeWarn       EnforcementMode = "warn"
	ModeReject     EnforcementMode = "reject"
	ModeQuarantine EnforcementMode = "quarantine"
)

// EnforcementResult is the outcome of enforcing a contract on a message.
type EnforcementResult struct {
	ContractName string          `json:"contract_name"`
	Topic        string          `json:"topic"`
	Mode         EnforcementMode `json:"mode"`
	Allowed      bool            `json:"allowed"`
	Violations   []Violation     `json:"violations"`
	Action       string          `json:"action"`
	Timestamp    string          `json:"timestamp"`
}

// ViolationSummary provides aggregate violation stats for a topic.
type ViolationSummary struct {
	Topic             string            `json:"topic"`
	ContractName      string            `json:"contract_name"`
	TotalViolations   int               `json:"total_violations"`
	ViolationsByRule  map[string]int    `json:"violations_by_rule"`
	ViolationsByField map[string]int    `json:"violations_by_field"`
	RecentViolations  []ViolationRecord `json:"recent_violations"`
	ComplianceRate    float64           `json:"compliance_rate"`
	Timestamp         string            `json:"timestamp"`
}

// ViolationRecord is a single recorded violation event.
type ViolationRecord struct {
	Rule      string `json:"rule"`
	Field     string `json:"field"`
	Message   string `json:"message"`
	Severity  string `json:"severity"`
	Timestamp string `json:"timestamp"`
}

// SetEnforcementMode sets the enforcement mode for a topic's contract.
func (e *Engine) SetEnforcementMode(topic string, mode EnforcementMode) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.contracts[topic]; !ok {
		return fmt.Errorf("no contract found for topic %q", topic)
	}

	e.modes[topic] = mode
	return nil
}

// getEnforcementMode returns the enforcement mode for a topic.
func (e *Engine) getEnforcementMode(topic string) EnforcementMode {
	if mode, ok := e.modes[topic]; ok {
		return mode
	}
	return ModeWarn
}

// EnforceContract validates a message and applies the enforcement mode.
func (e *Engine) EnforceContract(topic string, value map[string]any) *EnforcementResult {
	e.mu.RLock()
	_, ok := e.contracts[topic]
	if !ok {
		e.mu.RUnlock()
		return nil
	}
	mode := e.getEnforcementMode(topic)
	e.mu.RUnlock()

	validation := e.ValidateMessage(topic, value)
	if validation == nil {
		return nil
	}

	result := &EnforcementResult{
		ContractName: validation.ContractName,
		Topic:        topic,
		Mode:         mode,
		Violations:   validation.Violations,
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
	}

	if validation.Valid {
		result.Allowed = true
		result.Action = "passed"
	} else {
		switch mode {
		case ModeWarn:
			result.Allowed = true
			result.Action = "warned"
		case ModeReject:
			result.Allowed = false
			result.Action = "rejected"
		case ModeQuarantine:
			result.Allowed = false
			result.Action = "quarantined"
		}
	}

	// Violations already recorded by ValidateMessage → recordCompliance

	return result
}

// recordViolation stores violation records for querying.
func (e *Engine) recordViolation(topic string, result *ValidationResult) {
	e.mu.Lock()
	defer e.mu.Unlock()

	report, ok := e.stats[topic]
	if !ok {
		return
	}

	for _, v := range result.Violations {
		report.ViolationsByRule[v.Rule]++
	}
}

// GetViolationSummary returns a violation summary for a topic.
func (e *Engine) GetViolationSummary(topic string) (*ViolationSummary, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	c, ok := e.contracts[topic]
	if !ok {
		return nil, fmt.Errorf("no contract found for topic %q", topic)
	}

	report, ok := e.stats[topic]
	if !ok {
		return &ViolationSummary{
			Topic:             topic,
			ContractName:      c.Name,
			ViolationsByRule:  make(map[string]int),
			ViolationsByField: make(map[string]int),
			Timestamp:         time.Now().UTC().Format(time.RFC3339),
		}, nil
	}

	violationsByField := make(map[string]int)
	for _, rule := range c.Quality {
		if count, exists := report.ViolationsByRule[rule.Rule]; exists {
			violationsByField[rule.Field] += count
		}
	}

	return &ViolationSummary{
		Topic:             topic,
		ContractName:      c.Name,
		TotalViolations:   report.FailedCount,
		ViolationsByRule:  report.ViolationsByRule,
		ViolationsByField: violationsByField,
		ComplianceRate:    report.ComplianceRate,
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
	}, nil
}
