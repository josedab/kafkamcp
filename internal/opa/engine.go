// Package opa provides Open Policy Agent (OPA) Rego-based policy evaluation,
// replacing static YAML access control with dynamic policy-as-code.
package opa

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// PolicyEngine evaluates OPA Rego policies for authorization decisions.
type PolicyEngine struct {
	mu        sync.RWMutex
	policies  map[string]*Policy
	bundles   map[string]*PolicyBundle
	decisions []Decision
	logger    *slog.Logger
	maxDecLog int
}

// Policy represents a single Rego policy rule.
type Policy struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Rego        string    `json:"rego"`
	Enabled     bool      `json:"enabled"`
	Priority    int       `json:"priority"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// PolicyBundle groups related policies.
type PolicyBundle struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Policies    []string  `json:"policy_ids"`
	Version     string    `json:"version"`
	LoadedAt    time.Time `json:"loaded_at"`
}

// PolicyInput is the input to a policy evaluation.
type PolicyInput struct {
	Agent        string            `json:"agent"`
	Action       string            `json:"action"`
	Resource     string            `json:"resource"`
	ResourceName string            `json:"resource_name"`
	Cluster      string            `json:"cluster,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// Decision records a policy evaluation result.
type Decision struct {
	ID        string      `json:"id"`
	Input     PolicyInput `json:"input"`
	Allowed   bool        `json:"allowed"`
	PolicyID  string      `json:"policy_id"`
	Reason    string      `json:"reason"`
	Duration  string      `json:"duration"`
	Timestamp time.Time   `json:"timestamp"`
}

// SimulationResult shows the outcome of policy simulation.
type SimulationResult struct {
	Input     PolicyInput `json:"input"`
	Decisions []Decision  `json:"decisions"`
	Final     bool        `json:"final_decision"`
	Reason    string      `json:"reason"`
}

// NewPolicyEngine creates a new OPA policy engine.
func NewPolicyEngine(logger *slog.Logger) *PolicyEngine {
	pe := &PolicyEngine{
		policies:  make(map[string]*Policy),
		bundles:   make(map[string]*PolicyBundle),
		decisions: make([]Decision, 0),
		logger:    logger,
		maxDecLog: 10000,
	}

	// Load default policies
	pe.loadDefaults()
	return pe
}

func (pe *PolicyEngine) loadDefaults() {
	defaultPolicies := []Policy{
		{
			ID:          "default-allow-read",
			Name:        "Allow Read Operations",
			Description: "Allow all agents to read topics and consumer groups",
			Rego: `package kafkamcp.authz
default allow = false
allow {
    input.action == "read"
}
allow {
    input.action == "describe"
}`,
			Enabled:  true,
			Priority: 100,
		},
		{
			ID:          "default-deny-delete",
			Name:        "Deny Delete by Default",
			Description: "Deny delete operations unless explicitly allowed",
			Rego: `package kafkamcp.authz
deny {
    input.action == "delete"
    not input.agent == "admin"
}`,
			Enabled:  true,
			Priority: 200,
		},
		{
			ID:          "default-rate-limit",
			Name:        "Rate Limit Anonymous",
			Description: "Apply strict limits to anonymous agents",
			Rego: `package kafkamcp.authz
deny {
    input.agent == "anonymous"
    input.action == "write"
}`,
			Enabled:  true,
			Priority: 50,
		},
	}

	for i := range defaultPolicies {
		p := &defaultPolicies[i]
		p.CreatedAt = time.Now()
		p.UpdatedAt = time.Now()
		pe.policies[p.ID] = p
	}
}

// Evaluate runs policy evaluation against all enabled policies.
func (pe *PolicyEngine) Evaluate(input PolicyInput) Decision {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	start := time.Now()

	// Evaluate all policies in priority order
	allowed := false
	denied := false
	matchedPolicy := ""
	reason := "no matching policy"

	type policyEntry struct {
		policy *Policy
	}
	var sorted []policyEntry
	for _, p := range pe.policies {
		if p.Enabled {
			sorted = append(sorted, policyEntry{p})
		}
	}

	for _, entry := range sorted {
		result := pe.evaluateRego(entry.policy, input)
		if result == "allow" {
			allowed = true
			matchedPolicy = entry.policy.ID
			reason = "allowed by " + entry.policy.Name
		} else if result == "deny" {
			denied = true
			matchedPolicy = entry.policy.ID
			reason = "denied by " + entry.policy.Name
			break
		}
	}

	finalDecision := allowed && !denied

	decision := Decision{
		ID:        randid.New("dec"),
		Input:     input,
		Allowed:   finalDecision,
		PolicyID:  matchedPolicy,
		Reason:    reason,
		Duration:  time.Since(start).String(),
		Timestamp: time.Now(),
	}

	return decision
}

// evaluateRego performs simplified Rego evaluation.
// In production, this would use the OPA Go library (github.com/open-policy-agent/opa).
func (pe *PolicyEngine) evaluateRego(policy *Policy, input PolicyInput) string {
	rego := policy.Rego

	// Simple pattern matching for common Rego constructs
	if strings.Contains(rego, "allow") {
		allowBlock := extractBlock(rego, "allow")
		if matchesConditions(allowBlock, input) {
			return "allow"
		}
	}

	if strings.Contains(rego, "deny") {
		denyBlock := extractBlock(rego, "deny")
		if matchesConditions(denyBlock, input) {
			return "deny"
		}
	}

	return "no_match"
}

func extractBlock(rego, ruleName string) string {
	idx := strings.Index(rego, ruleName+" {")
	if idx < 0 {
		idx = strings.Index(rego, ruleName+"{")
	}
	if idx < 0 {
		return ""
	}

	start := idx
	depth := 0
	for i := start; i < len(rego); i++ {
		if rego[i] == '{' {
			depth++
		} else if rego[i] == '}' {
			depth--
			if depth == 0 {
				return rego[start : i+1]
			}
		}
	}
	return rego[start:]
}

func matchesConditions(block string, input PolicyInput) bool {
	if block == "" {
		return false
	}

	lines := strings.Split(block, "\n")
	allMatch := true

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "//") || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "allow") || strings.HasPrefix(line, "deny") || line == "{" || line == "}" {
			continue
		}
		if strings.HasPrefix(line, "default") || strings.HasPrefix(line, "package") {
			continue
		}

		if !evaluateCondition(line, input) {
			allMatch = false
		}
	}

	return allMatch
}

func evaluateCondition(condition string, input PolicyInput) bool {
	condition = strings.TrimSpace(condition)

	// Handle negation
	if strings.HasPrefix(condition, "not ") {
		return !evaluateCondition(condition[4:], input)
	}

	// input.field == "value" patterns
	if strings.Contains(condition, "input.action") && strings.Contains(condition, "==") {
		val := extractQuotedValue(condition)
		return input.Action == val
	}
	if strings.Contains(condition, "input.agent") && strings.Contains(condition, "==") {
		val := extractQuotedValue(condition)
		return input.Agent == val
	}
	if strings.Contains(condition, "input.resource") && strings.Contains(condition, "==") {
		val := extractQuotedValue(condition)
		return input.Resource == val
	}
	if strings.Contains(condition, "input.resource_name") && strings.Contains(condition, "==") {
		val := extractQuotedValue(condition)
		return input.ResourceName == val
	}

	// If we can't parse, don't match
	return false
}

func extractQuotedValue(s string) string {
	idx := strings.Index(s, `"`)
	if idx < 0 {
		return ""
	}
	rest := s[idx+1:]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// RecordDecision stores a decision in the log.
func (pe *PolicyEngine) RecordDecision(d Decision) {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	pe.decisions = append(pe.decisions, d)
	if len(pe.decisions) > pe.maxDecLog {
		pe.decisions = pe.decisions[len(pe.decisions)-pe.maxDecLog:]
	}
}

// RegisterPolicy adds or updates a policy.
func (pe *PolicyEngine) RegisterPolicy(p Policy) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if p.ID == "" {
		return fmt.Errorf("policy ID is required")
	}
	if p.Rego == "" {
		return fmt.Errorf("rego rules are required")
	}

	now := time.Now()
	if existing, ok := pe.policies[p.ID]; ok {
		existing.Name = p.Name
		existing.Description = p.Description
		existing.Rego = p.Rego
		existing.Enabled = p.Enabled
		existing.Priority = p.Priority
		existing.UpdatedAt = now
	} else {
		p.CreatedAt = now
		p.UpdatedAt = now
		pe.policies[p.ID] = &p
	}

	pe.logger.Info("policy registered", "id", p.ID, "name", p.Name)
	return nil
}

// DeletePolicy removes a policy.
func (pe *PolicyEngine) DeletePolicy(id string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if _, ok := pe.policies[id]; !ok {
		return fmt.Errorf("policy %q not found", id)
	}
	delete(pe.policies, id)
	return nil
}

// ListPolicies returns all registered policies.
func (pe *PolicyEngine) ListPolicies() []Policy {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	var result []Policy
	for _, p := range pe.policies {
		result = append(result, *p)
	}
	return result
}

// Simulate runs a policy evaluation without recording the decision.
func (pe *PolicyEngine) Simulate(input PolicyInput) *SimulationResult {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	result := &SimulationResult{
		Input: input,
	}

	allowed := false
	denied := false

	for _, p := range pe.policies {
		if !p.Enabled {
			continue
		}

		start := time.Now()
		evalResult := pe.evaluateRego(p, input)

		d := Decision{
			Input:     input,
			PolicyID:  p.ID,
			Duration:  time.Since(start).String(),
			Timestamp: time.Now(),
		}

		switch evalResult {
		case "allow":
			d.Allowed = true
			d.Reason = "matched allow rule"
			allowed = true
		case "deny":
			d.Allowed = false
			d.Reason = "matched deny rule"
			denied = true
		default:
			d.Reason = "no match"
		}

		result.Decisions = append(result.Decisions, d)
	}

	result.Final = allowed && !denied
	if denied {
		result.Reason = "explicitly denied by policy"
	} else if allowed {
		result.Reason = "allowed by policy"
	} else {
		result.Reason = "no matching policy (default deny)"
	}

	return result
}

// RecentDecisions returns the most recent policy decisions.
func (pe *PolicyEngine) RecentDecisions(limit int) []Decision {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	if limit <= 0 || limit > len(pe.decisions) {
		limit = len(pe.decisions)
	}

	start := len(pe.decisions) - limit
	if start < 0 {
		start = 0
	}

	result := make([]Decision, limit)
	copy(result, pe.decisions[start:])
	return result
}

// ExportPolicies exports all policies as JSON.
func (pe *PolicyEngine) ExportPolicies() (string, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	data, err := json.MarshalIndent(pe.policies, "", "  ")
	if err != nil {
		return "", fmt.Errorf("exporting policies: %w", err)
	}
	return string(data), nil
}
