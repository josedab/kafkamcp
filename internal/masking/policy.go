package masking

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// SensitivityLevel defines the sensitivity classification of data.
type SensitivityLevel string

// SensitivityLow and related values define constants for this package.
const (
	SensitivityLow      SensitivityLevel = "low"
	SensitivityMedium   SensitivityLevel = "medium"
	SensitivityHigh     SensitivityLevel = "high"
	SensitivityCritical SensitivityLevel = "critical"
)

// PolicyRule defines a masking rule in the DSL.
type PolicyRule struct {
	Name        string           `yaml:"name" json:"name"`
	Type        string           `yaml:"type" json:"type"`
	FieldPaths  []string         `yaml:"field_paths" json:"field_paths"`
	Pattern     string           `yaml:"pattern,omitempty" json:"pattern,omitempty"`
	Replace     string           `yaml:"replace,omitempty" json:"replace,omitempty"`
	Sensitivity SensitivityLevel `yaml:"sensitivity" json:"sensitivity"`
	Agents      []string         `yaml:"agents,omitempty" json:"agents,omitempty"`
	Action      string           `yaml:"action" json:"action"` // "redact", "hash", "mask_partial", "tokenize"
}

// AgentPolicy defines per-agent masking configuration.
type AgentPolicy struct {
	AgentID        string           `yaml:"agent_id" json:"agent_id"`
	MinSensitivity SensitivityLevel `yaml:"min_sensitivity" json:"min_sensitivity"`
	AllowedFields  []string         `yaml:"allowed_fields,omitempty" json:"allowed_fields,omitempty"`
	DeniedFields   []string         `yaml:"denied_fields,omitempty" json:"denied_fields,omitempty"`
	OverrideAction string           `yaml:"override_action,omitempty" json:"override_action,omitempty"`
}

// MaskingPolicy is a complete masking policy loaded from YAML.
type MaskingPolicy struct {
	Version       string        `yaml:"version" json:"version"`
	Rules         []PolicyRule  `yaml:"rules" json:"rules"`
	AgentPolicies []AgentPolicy `yaml:"agent_policies,omitempty" json:"agent_policies,omitempty"`
}

// PolicyManager manages masking policies with hot-reloading support.
type PolicyManager struct {
	mu            sync.RWMutex
	policy        *MaskingPolicy
	compiledRules []compiledPolicyRule
	agentPolicies map[string]*AgentPolicy
}

type compiledPolicyRule struct {
	rule    PolicyRule
	pattern *regexp.Regexp
	paths   []string
}

// NewPolicyManager creates a new policy manager.
func NewPolicyManager() *PolicyManager {
	return &PolicyManager{
		agentPolicies: make(map[string]*AgentPolicy),
	}
}

// LoadFromYAML loads a masking policy from YAML bytes.
func (pm *PolicyManager) LoadFromYAML(data []byte) error {
	var policy MaskingPolicy
	if err := yaml.Unmarshal(data, &policy); err != nil {
		return fmt.Errorf("parsing masking policy: %w", err)
	}
	return pm.Apply(&policy)
}

// Apply sets the active policy and compiles rules.
func (pm *PolicyManager) Apply(policy *MaskingPolicy) error {
	compiled := make([]compiledPolicyRule, 0, len(policy.Rules))
	for _, r := range policy.Rules {
		cr := compiledPolicyRule{
			rule:  r,
			paths: r.FieldPaths,
		}
		rule, compileErr := compileMaskingRule(ruleCompileInput{
			Name: r.Name, Type: r.Type, Pattern: r.Pattern, Replace: r.Replace, Agents: r.Agents,
		}, ruleCompilePolicy)
		if compileErr != nil {
			switch compileErr.kind {
			case ruleCompilePatternTooLong:
				return fmt.Errorf("pattern in rule %q exceeds maximum length of %d characters", r.Name, maxRegexPatternLength)
			case ruleCompilePatternUnsafe:
				return fmt.Errorf("pattern in rule %q contains potentially dangerous constructs", r.Name)
			default:
				return fmt.Errorf("invalid pattern in rule %q: %w", r.Name, compileErr.cause)
			}
		}
		cr.pattern = rule.Pattern
		compiled = append(compiled, cr)
	}

	agentMap := make(map[string]*AgentPolicy, len(policy.AgentPolicies))
	for i := range policy.AgentPolicies {
		ap := &policy.AgentPolicies[i]
		agentMap[ap.AgentID] = ap
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.policy = policy
	pm.compiledRules = compiled
	pm.agentPolicies = agentMap

	return nil
}

// GetAgentPolicy returns the masking policy for a specific agent.
func (pm *PolicyManager) GetAgentPolicy(agentID string) *AgentPolicy {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.agentPolicies[agentID]
}

// GetPolicy returns the current active policy.
func (pm *PolicyManager) GetPolicy() *MaskingPolicy {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.policy
}

// ShouldMaskForAgent determines if a field should be masked for a given agent
// based on the sensitivity level.
func (pm *PolicyManager) ShouldMaskForAgent(agentID, fieldPath string, sensitivity SensitivityLevel) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	ap, ok := pm.agentPolicies[agentID]
	if !ok {
		return true // Default: mask everything for unknown agents
	}

	// Check denied fields first
	for _, denied := range ap.DeniedFields {
		if matchFieldPath(denied, fieldPath) {
			return true
		}
	}

	// Check allowed fields
	for _, allowed := range ap.AllowedFields {
		if matchFieldPath(allowed, fieldPath) {
			return false
		}
	}

	// Check sensitivity threshold
	return sensitivityOrd(sensitivity) >= sensitivityOrd(ap.MinSensitivity)
}

func sensitivityOrd(s SensitivityLevel) int {
	switch s {
	case SensitivityLow:
		return 1
	case SensitivityMedium:
		return 2
	case SensitivityHigh:
		return 3
	case SensitivityCritical:
		return 4
	default:
		return 2
	}
}

// JSONPathTraverser traverses JSON structures and applies masking at specific paths.
type JSONPathTraverser struct {
	engine *Engine
}

// NewJSONPathTraverser creates a new JSON path traverser.
func NewJSONPathTraverser(engine *Engine) *JSONPathTraverser {
	return &JSONPathTraverser{engine: engine}
}

// TraverseAndMask applies masking rules at specified JSON paths.
func (t *JSONPathTraverser) TraverseAndMask(agentID string, data []byte) ([]byte, error) {
	var parsed any
	if err := json.Unmarshal(data, &parsed); err != nil {
		// Not valid JSON — treat as raw string
		masked := t.engine.MaskValue(agentID, string(data))
		if s, ok := masked.(string); ok {
			return []byte(s), nil
		}
		return data, nil
	}

	masked := t.engine.MaskValue(agentID, parsed)
	return json.Marshal(masked)
}

// DetectPII scans a value and returns detected PII types without masking.
func DetectPII(value string) []PIIDetection {
	var detections []PIIDetection

	if emailRegex.MatchString(value) {
		for _, loc := range emailRegex.FindAllStringIndex(value, -1) {
			detections = append(detections, PIIDetection{
				Type:     "email",
				Start:    loc[0],
				End:      loc[1],
				Severity: SensitivityHigh,
			})
		}
	}

	if ssnRegex.MatchString(value) {
		for _, loc := range ssnRegex.FindAllStringIndex(value, -1) {
			detections = append(detections, PIIDetection{
				Type:     "ssn",
				Start:    loc[0],
				End:      loc[1],
				Severity: SensitivityCritical,
			})
		}
	}

	if creditCardRegex.MatchString(value) {
		for _, loc := range creditCardRegex.FindAllStringIndex(value, -1) {
			match := value[loc[0]:loc[1]]
			digits := strings.ReplaceAll(strings.ReplaceAll(match, " ", ""), "-", "")
			if luhnCheck(digits) {
				detections = append(detections, PIIDetection{
					Type:     "credit_card",
					Start:    loc[0],
					End:      loc[1],
					Severity: SensitivityCritical,
				})
			}
		}
	}

	if phoneRegex.MatchString(value) {
		for _, loc := range phoneRegex.FindAllStringIndex(value, -1) {
			detections = append(detections, PIIDetection{
				Type:     "phone",
				Start:    loc[0],
				End:      loc[1],
				Severity: SensitivityMedium,
			})
		}
	}

	return detections
}

// PIIDetection represents a detected PII occurrence.
type PIIDetection struct {
	Type     string           `json:"type"`
	Start    int              `json:"start"`
	End      int              `json:"end"`
	Severity SensitivityLevel `json:"severity"`
}

// MaskPartial masks all but the last N characters of a value.
func MaskPartial(value string, visibleChars int) string {
	if len(value) <= visibleChars {
		return strings.Repeat("*", len(value))
	}
	masked := strings.Repeat("*", len(value)-visibleChars)
	return masked + value[len(value)-visibleChars:]
}

// HashValue is retained only as a thin compatibility shim over the keyed
// tokenizer path. It intentionally has NO unkeyed fallback: without a configured
// key it fails closed and returns the standard redaction marker. Prefer
// Engine.ApplyAction("tokenize", ...) which carries domain separation.
//
// Deprecated: use a keyed Tokenizer via Engine.ApplyAction. This helper exists
// for callers that only need best-effort redaction and cannot supply a key.
func HashValue(value string) string {
	tok, err := TokenizerFromEnv("")
	if err != nil {
		return defaultReplacement
	}
	if t, ok := tok.Token("tokenize", value); ok {
		return t
	}
	return defaultReplacement
}
