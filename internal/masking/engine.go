package masking

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/josedab/kafkamcp/internal/config"
)

// Built-in PII detection patterns.
var (
	emailRegex      = regexp.MustCompile(`[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}`)
	ssnRegex        = regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
	phoneRegex      = regexp.MustCompile(`\b(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b`)
	creditCardRegex = regexp.MustCompile(`\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b`)
	digitSeqRegex   = regexp.MustCompile(`\b\d{13,19}\b`)
)

const defaultReplacement = "***REDACTED***"

const maxRegexPatternLength = 256

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
	tokenizer     *Tokenizer
	// warnedNoKey ensures the "fail closed" warning is only logged once to avoid
	// flooding logs on every masked value.
	warnedNoKey atomic.Bool
}

// NewEngine creates a masking engine from configuration.
//
// The HMAC key for the "hash"/"tokenize" actions is read from the environment
// variable named by cfg.KeyEnv (default KAFKAMCP_MASKING_KEY). If the variable
// is unset the tokenizer is left unconfigured and those actions fail closed
// (fully redact). If the variable is set but the key is too short, the tokenizer
// is also left unconfigured and an error is logged so the misconfiguration is
// visible rather than silently weakening protection.
func NewEngine(cfg config.MaskingConfig, logger *slog.Logger) *Engine {
	e := &Engine{
		enabled:       cfg.Enabled,
		logger:        logger,
		policyManager: NewPolicyManager(),
	}

	tok, err := TokenizerFromEnv(cfg.KeyEnv)
	if err != nil {
		if logger != nil {
			logger.Error("masking tokenization key rejected; hash/tokenize actions will fail closed",
				"key_env", keyEnvName(cfg.KeyEnv), "error", err)
		}
		tok = &Tokenizer{} // unconfigured: fail closed
	}
	e.tokenizer = tok

	for _, r := range cfg.Rules {
		compiled, compileErr := compileMaskingRule(ruleCompileInput{
			Name: r.Name, Type: r.Type, FieldPath: r.FieldPath,
			Pattern: r.Pattern, Replace: r.Replace, Agents: r.Agents,
		}, ruleCompileConfig)
		if compileErr != nil {
			switch compileErr.kind {
			case ruleCompilePatternTooLong:
				logger.Warn("masking regex pattern too long, skipping rule", "rule", r.Name, "length", len(r.Pattern), "max", maxRegexPatternLength)
			case ruleCompilePatternUnsafe:
				logger.Warn("masking regex contains potentially dangerous constructs, skipping rule", "rule", r.Name)
			default:
				logger.Warn("invalid masking regex, skipping rule", "rule", r.Name, "error", compileErr.cause)
			}
			continue
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
	masked, err := e.MaskValueChecked(agentID, value)
	if err != nil {
		return value
	}
	return masked
}

// MaskValueChecked applies masking rules to a message value for a given agent
// and, unlike MaskValue, propagates errors so callers can fail closed. When a
// value cannot be safely serialized for structured masking, it returns an error
// instead of silently returning the original (unmasked) value. Nil values and a
// disabled/empty engine pass through unchanged.
func (e *Engine) MaskValueChecked(agentID string, value any) (any, error) {
	if !e.enabled || len(e.rules) == 0 {
		return value, nil
	}
	if value == nil {
		return nil, nil
	}

	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.maskAny(agentID, value, "")
}

func (e *Engine) maskAny(agentID string, value any, prefix string) (any, error) {
	switch v := value.(type) {
	case string:
		return e.maskString(agentID, v), nil
	case map[string]any:
		return e.maskMap(agentID, v, prefix), nil
	case []any:
		masked := make([]any, len(v))
		for i, item := range v {
			value, err := e.maskAny(agentID, item, prefix)
			if err != nil {
				return nil, err
			}
			masked[i] = value
		}
		return masked, nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("masking: cannot serialize value for masking: %w", err)
		}
		var decoded any
		if err := json.Unmarshal(data, &decoded); err != nil {
			return nil, fmt.Errorf("masking: cannot decode serialized value: %w", err)
		}
		switch decoded.(type) {
		case map[string]any, []any, string:
			return e.maskAny(agentID, decoded, prefix)
		default:
			// Numeric and boolean scalars cannot match a field path or textual
			// pattern, so preserve their original Go type.
			return value, nil
		}
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
//
// The "hash" and "tokenize" actions use keyed HMAC-SHA-256 tokenization with
// domain separation (the action name is the domain, so a value tokenized under
// "hash" differs from the same value under "tokenize"). When no tokenization key
// is configured these actions fail closed and fully redact the value rather than
// emitting a weak, unkeyed token.
func (e *Engine) ApplyAction(action, value, replacement string) string {
	switch action {
	case "hash", "tokenize":
		if tok, ok := e.tokenizer.Token(action, value); ok {
			return tok
		}
		e.warnFailClosed(action)
		return defaultReplacement
	case "mask_partial":
		return MaskPartial(value, 4)
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

// TokenizationConfigured reports whether a valid HMAC key is available for the
// hash/tokenize actions. It is used by validation/doctor tooling to warn when
// masking rules would fail closed.
func (e *Engine) TokenizationConfigured() bool {
	return e.tokenizer.Configured()
}

// warnFailClosed logs a single warning the first time a hash/tokenize action
// fails closed due to a missing key.
func (e *Engine) warnFailClosed(action string) {
	if e.logger == nil {
		return
	}
	if e.warnedNoKey.CompareAndSwap(false, true) {
		e.logger.Warn("masking action failing closed (fully redacting): no tokenization key configured",
			"action", action, "key_env", DefaultKeyEnv)
	}
}

// keyEnvName returns the effective environment variable name for logging.
func keyEnvName(configured string) string {
	if configured == "" {
		return DefaultKeyEnv
	}
	return configured
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
	compiled, compileErr := compileMaskingRule(ruleCompileInput{
		Name: name, Type: ruleType, FieldPath: fieldPath,
		Pattern: pattern, Replace: replace, Agents: agents,
	}, ruleCompileDynamic)
	if compileErr != nil {
		switch compileErr.kind {
		case ruleCompilePatternRequired:
			return fmt.Errorf("regex pattern is required for type 'regex'")
		case ruleCompilePatternTooLong:
			return fmt.Errorf("regex pattern exceeds maximum length of %d characters", maxRegexPatternLength)
		case ruleCompilePatternUnsafe:
			return fmt.Errorf("regex pattern contains potentially dangerous constructs")
		case ruleCompileUnknownType:
			return fmt.Errorf("unknown masking type %q; must be: email, ssn, phone, credit_card, regex, ner", ruleType)
		default:
			return fmt.Errorf("invalid regex pattern: %w", compileErr.cause)
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules = append(e.rules, compiled)
	return nil
}

type ruleCompileSource uint8

const (
	ruleCompileConfig ruleCompileSource = iota
	ruleCompileDynamic
	ruleCompilePolicy
)

type ruleCompileErrorKind uint8

const (
	ruleCompilePatternRequired ruleCompileErrorKind = iota
	ruleCompilePatternTooLong
	ruleCompilePatternUnsafe
	ruleCompilePatternInvalid
	ruleCompileUnknownType
)

type ruleCompileError struct {
	kind  ruleCompileErrorKind
	cause error
}

type ruleCompileInput struct {
	Name      string
	Type      string
	FieldPath string
	Pattern   string
	Replace   string
	Agents    []string
}

func compileMaskingRule(input ruleCompileInput, source ruleCompileSource) (Rule, *ruleCompileError) {
	compiled := Rule{
		Name:      input.Name,
		Type:      input.Type,
		FieldPath: input.FieldPath,
		Replace:   input.Replace,
	}
	if compiled.Replace == "" {
		compiled.Replace = defaultReplacement
	}
	if len(input.Agents) > 0 {
		compiled.Agents = make(map[string]bool, len(input.Agents))
		for _, agent := range input.Agents {
			compiled.Agents[agent] = true
		}
	}

	if source == ruleCompilePolicy {
		pattern, compileErr := compileMaskingPattern(input.Pattern, false, false)
		compiled.Pattern = pattern
		return compiled, compileErr
	}

	switch input.Type {
	case "email":
		compiled.Pattern = emailRegex
	case "ssn":
		compiled.Pattern = ssnRegex
	case "phone":
		compiled.Pattern = phoneRegex
	case "credit_card":
		compiled.Pattern = creditCardRegex
	case "regex":
		pattern, compileErr := compileMaskingPattern(input.Pattern, source == ruleCompileDynamic, source == ruleCompileDynamic)
		compiled.Pattern = pattern
		return compiled, compileErr
	case "ner":
		// NER uses all built-in patterns.
	default:
		if source == ruleCompileDynamic {
			return Rule{}, &ruleCompileError{kind: ruleCompileUnknownType}
		}
	}
	return compiled, nil
}

func compileMaskingPattern(pattern string, requirePattern, strictFlags bool) (*regexp.Regexp, *ruleCompileError) {
	if pattern == "" {
		if requirePattern {
			return nil, &ruleCompileError{kind: ruleCompilePatternRequired}
		}
		return nil, nil
	}
	if len(pattern) > maxRegexPatternLength {
		return nil, &ruleCompileError{kind: ruleCompilePatternTooLong}
	}
	if idx := strings.Index(pattern, "(?"); idx >= 0 {
		allowed := strings.Contains(pattern, "(?i)")
		if strictFlags {
			allowed = idx == 0 && strings.HasPrefix(pattern, "(?i)") && !strings.Contains(pattern[4:], "(?")
		}
		if !allowed {
			return nil, &ruleCompileError{kind: ruleCompilePatternUnsafe}
		}
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, &ruleCompileError{kind: ruleCompilePatternInvalid, cause: err}
	}
	return compiled, nil
}
