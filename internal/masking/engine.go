package masking

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

// Built-in PII detection patterns.
var (
	emailRegex      = regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)
	ssnRegex        = regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
	phoneRegex      = regexp.MustCompile(`\b(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b`)
	creditCardRegex = regexp.MustCompile(`\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b`)
)

const defaultReplacement = "***REDACTED***"

// Rule is a compiled masking rule ready for evaluation.
type Rule struct {
	Name        string
	Type        string
	FieldPath   string
	Pattern     *regexp.Regexp
	Replace     string
	Agents      map[string]bool // empty = apply to all
	Sensitivity SensitivityLevel
	Action      string // "redact", "hash", "mask_partial", "tokenize"
}

// Engine applies masking rules to message values before they reach agents.
type Engine struct {
	mu            sync.RWMutex
	rules         []Rule
	enabled       bool
	logger        *slog.Logger
	policyManager *PolicyManager
}

// NewEngine creates a masking engine from configuration.
func NewEngine(cfg config.MaskingConfig, logger *slog.Logger) *Engine {
	e := &Engine{
		enabled:       cfg.Enabled,
		logger:        logger,
		policyManager: NewPolicyManager(),
	}

	for _, r := range cfg.Rules {
		compiled := Rule{
			Name:      r.Name,
			Type:      r.Type,
			FieldPath: r.FieldPath,
			Replace:   r.Replace,
		}
		if compiled.Replace == "" {
			compiled.Replace = defaultReplacement
		}

		if len(r.Agents) > 0 {
			compiled.Agents = make(map[string]bool, len(r.Agents))
			for _, a := range r.Agents {
				compiled.Agents[a] = true
			}
		}

		switch r.Type {
		case "email":
			compiled.Pattern = emailRegex
		case "ssn":
			compiled.Pattern = ssnRegex
		case "phone":
			compiled.Pattern = phoneRegex
		case "credit_card":
			compiled.Pattern = creditCardRegex
		case "regex":
			if r.Pattern != "" {
				p, err := regexp.Compile(r.Pattern)
				if err != nil {
					logger.Warn("invalid masking regex, skipping rule", "rule", r.Name, "error", err)
					continue
				}
				compiled.Pattern = p
			}
		case "ner":
			// NER type uses all built-in patterns
		}

		e.rules = append(e.rules, compiled)
	}

	if e.enabled {
		logger.Info("masking engine initialized", "rules", len(e.rules))
	}

	return e
}

// MaskValue applies masking rules to a message value for a given agent.
// Returns the masked value.
func (e *Engine) MaskValue(agentID string, value any) any {
	if !e.enabled || len(e.rules) == 0 {
		return value
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	switch v := value.(type) {
	case string:
		return e.maskString(agentID, v)
	case map[string]any:
		return e.maskMap(agentID, v, "")
	default:
		// Try to re-parse as JSON
		data, err := json.Marshal(v)
		if err != nil {
			return value
		}
		var m map[string]any
		if err := json.Unmarshal(data, &m); err != nil {
			return e.maskString(agentID, string(data))
		}
		return e.maskMap(agentID, m, "")
	}
}

// maskMap recursively masks fields in a JSON object.
func (e *Engine) maskMap(agentID string, m map[string]any, prefix string) map[string]any {
	result := make(map[string]any, len(m))
	for k, v := range m {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}

		switch val := v.(type) {
		case string:
			result[k] = e.maskField(agentID, path, val)
		case map[string]any:
			result[k] = e.maskMap(agentID, val, path)
		case []any:
			masked := make([]any, len(val))
			for i, item := range val {
				if m2, ok := item.(map[string]any); ok {
					masked[i] = e.maskMap(agentID, m2, path)
				} else if s, ok := item.(string); ok {
					masked[i] = e.maskField(agentID, path, s)
				} else {
					masked[i] = item
				}
			}
			result[k] = masked
		default:
			result[k] = v
		}
	}
	return result
}

// maskField applies field-path-specific rules and NER detection to a string value.
func (e *Engine) maskField(agentID, fieldPath, value string) string {
	for _, rule := range e.rules {
		if !e.ruleApplies(rule, agentID) {
			continue
		}

		if rule.Type == "ner" {
			value = e.applyNER(value, rule.Replace)
			continue
		}

		if rule.FieldPath != "" && !matchFieldPath(rule.FieldPath, fieldPath) {
			continue
		}

		if rule.Pattern != nil {
			value = rule.Pattern.ReplaceAllString(value, rule.Replace)
		}
	}
	return value
}

// maskString applies all applicable rules to a raw string value.
func (e *Engine) maskString(agentID, value string) string {
	for _, rule := range e.rules {
		if !e.ruleApplies(rule, agentID) {
			continue
		}

		if rule.Type == "ner" {
			value = e.applyNER(value, rule.Replace)
			continue
		}

		// For non-field-path rules, apply to the whole string
		if rule.FieldPath == "" && rule.Pattern != nil {
			value = rule.Pattern.ReplaceAllString(value, rule.Replace)
		}
	}
	return value
}

// ruleApplies checks if a rule should be applied for a given agent.
func (e *Engine) ruleApplies(rule Rule, agentID string) bool {
	if len(rule.Agents) == 0 {
		return true
	}
	return rule.Agents[agentID]
}

// applyNER applies lightweight named entity recognition to detect PII.
func (e *Engine) applyNER(value, replacement string) string {
	value = emailRegex.ReplaceAllString(value, replacement)
	value = ssnRegex.ReplaceAllString(value, replacement)
	value = creditCardRegex.ReplaceAllString(value, replacement)
	value = phoneRegex.ReplaceAllString(value, replacement)

	// Luhn check for numeric sequences that might be card numbers
	value = maskLuhnNumbers(value, replacement)
	return value
}

// matchFieldPath checks if a field path matches a pattern.
// Supports ** for any depth and * for single segment.
func matchFieldPath(pattern, path string) bool {
	if pattern == path {
		return true
	}

	// **field** means the field name appears anywhere in the path
	if strings.HasPrefix(pattern, "**") && strings.HasSuffix(pattern, "**") {
		needle := pattern[2 : len(pattern)-2]
		return strings.Contains(strings.ToLower(path), strings.ToLower(needle))
	}

	// Simple glob matching on path segments
	patParts := strings.Split(pattern, ".")
	pathParts := strings.Split(path, ".")

	return matchParts(patParts, pathParts)
}

func matchParts(pattern, path []string) bool {
	if len(pattern) == 0 && len(path) == 0 {
		return true
	}
	if len(pattern) == 0 {
		return false
	}
	if pattern[0] == "**" {
		if len(pattern) == 1 {
			return true
		}
		for i := range path {
			if matchParts(pattern[1:], path[i:]) {
				return true
			}
		}
		return false
	}
	if len(path) == 0 {
		return false
	}
	if pattern[0] == "*" || strings.EqualFold(pattern[0], path[0]) {
		return matchParts(pattern[1:], path[1:])
	}
	return false
}

// maskLuhnNumbers finds digit sequences and masks those passing the Luhn check.
func maskLuhnNumbers(value, replacement string) string {
	// Find sequences of 13-19 digits (card number length)
	digitSeqRegex := regexp.MustCompile(`\b\d{13,19}\b`)
	return digitSeqRegex.ReplaceAllStringFunc(value, func(s string) string {
		if luhnCheck(s) {
			return replacement
		}
		return s
	})
}

// luhnCheck validates a number string against the Luhn algorithm.
func luhnCheck(number string) bool {
	digits := strings.ReplaceAll(number, " ", "")
	digits = strings.ReplaceAll(digits, "-", "")
	if len(digits) < 13 || len(digits) > 19 {
		return false
	}

	sum := 0
	double := false
	for i := len(digits) - 1; i >= 0; i-- {
		d, err := strconv.Atoi(string(digits[i]))
		if err != nil {
			return false
		}
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}

// Enabled returns whether the masking engine is active.
func (e *Engine) Enabled() bool {
	return e.enabled
}

// PolicyManager returns the engine's policy manager.
func (e *Engine) PolicyManager() *PolicyManager {
	return e.policyManager
}

// ApplyAction applies a specific masking action to a value.
func (e *Engine) ApplyAction(action, value, replacement string) string {
	switch action {
	case "hash":
		return HashValue(value)
	case "mask_partial":
		return MaskPartial(value, 4)
	case "tokenize":
		return HashValue(value)
	case "redact":
		if replacement == "" {
			return defaultReplacement
		}
		return replacement
	default:
		if replacement == "" {
			return defaultReplacement
		}
		return replacement
	}
}

// DetectPIIInValue scans a value and returns detected PII types.
func (e *Engine) DetectPIIInValue(value any) []PIIDetection {
	switch v := value.(type) {
	case string:
		return DetectPII(v)
	case map[string]any:
		var all []PIIDetection
		for _, val := range v {
			if s, ok := val.(string); ok {
				all = append(all, DetectPII(s)...)
			}
		}
		return all
	default:
		return nil
	}
}

// RuleCount returns the number of compiled masking rules.
func (e *Engine) RuleCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.rules)
}

// AddRule dynamically adds a masking rule at runtime.
func (e *Engine) AddRule(name, ruleType, fieldPath, pattern, replace string, agents []string) error {
	compiled := Rule{
		Name:      name,
		Type:      ruleType,
		FieldPath: fieldPath,
		Replace:   replace,
	}
	if compiled.Replace == "" {
		compiled.Replace = defaultReplacement
	}
	if len(agents) > 0 {
		compiled.Agents = make(map[string]bool, len(agents))
		for _, a := range agents {
			compiled.Agents[a] = true
		}
	}

	switch ruleType {
	case "email":
		compiled.Pattern = emailRegex
	case "ssn":
		compiled.Pattern = ssnRegex
	case "phone":
		compiled.Pattern = phoneRegex
	case "credit_card":
		compiled.Pattern = creditCardRegex
	case "regex":
		if pattern == "" {
			return fmt.Errorf("regex pattern is required for type 'regex'")
		}
		p, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("invalid regex pattern: %w", err)
		}
		compiled.Pattern = p
	case "ner":
		// NER uses all built-in patterns
	default:
		return fmt.Errorf("unknown masking type %q; must be: email, ssn, phone, credit_card, regex, ner", ruleType)
	}

	e.mu.Lock()
	e.rules = append(e.rules, compiled)
	e.mu.Unlock()
	return nil
}
