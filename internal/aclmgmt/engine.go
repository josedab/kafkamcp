package aclmgmt

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// ACLRule defines a desired ACL entry.
type ACLRule struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	ResourceType string `json:"resource_type"` // topic, group, cluster, transactional_id
	ResourceName string `json:"resource_name"`
	PatternType  string `json:"pattern_type"` // literal, prefixed
	Operation    string `json:"operation"`    // read, write, create, delete, alter, describe, all
	Permission   string `json:"permission"`   // allow, deny
}

// ACLDiffEntry represents a difference between desired and actual ACLs.
type ACLDiffEntry struct {
	Rule   ACLRule `json:"rule"`
	Status string  `json:"status"` // to_add, to_remove, in_sync
}

// ACLDiffResult is the complete diff between desired and actual ACLs.
type ACLDiffResult struct {
	ToAdd    []ACLDiffEntry `json:"to_add"`
	ToRemove []ACLDiffEntry `json:"to_remove"`
	InSync   []ACLDiffEntry `json:"in_sync"`
	Summary  string         `json:"summary"`
}

// AuditResult summarizes an ACL audit.
type AuditResult struct {
	TotalACLs        int            `json:"total_acls"`
	ByPrincipal      map[string]int `json:"by_principal"`
	ByResourceType   map[string]int `json:"by_resource_type"`
	OverPermissioned []AuditFinding `json:"over_permissioned"`
	WildcardRules    []AuditFinding `json:"wildcard_rules"`
	DenyRules        []ACLRule      `json:"deny_rules"`
	Recommendations  []string       `json:"recommendations"`
	AuditedAt        time.Time      `json:"audited_at"`
}

// AuditFinding describes a security concern found during audit.
type AuditFinding struct {
	Rule     ACLRule `json:"rule"`
	Finding  string  `json:"finding"`
	Severity string  `json:"severity"` // info, warning, critical
}

// DryRunResult simulates applying ACL changes without modifying the cluster.
type DryRunResult struct {
	WouldAdd    []ACLRule `json:"would_add"`
	WouldRemove []ACLRule `json:"would_remove"`
	NoChange    int       `json:"no_change"`
	Summary     string    `json:"summary"`
}

// Engine manages ACL operations with dry-run and diff capabilities.
type Engine struct {
	mu           sync.RWMutex
	desiredState map[string][]ACLRule // principal -> desired rules
	logger       *slog.Logger
}

// NewEngine creates a new ACL management engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		desiredState: make(map[string][]ACLRule),
		logger:       logger,
	}
}

// SetDesiredState stores the desired ACL state for a principal.
func (e *Engine) SetDesiredState(principal string, rules []ACLRule) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for i := range rules {
		rules[i].Principal = principal
		if rules[i].Host == "" {
			rules[i].Host = "*"
		}
		if rules[i].PatternType == "" {
			rules[i].PatternType = "literal"
		}
		if rules[i].Permission == "" {
			rules[i].Permission = "allow"
		}
	}
	e.desiredState[principal] = rules
}

// GetDesiredState returns the stored desired state for a principal.
func (e *Engine) GetDesiredState(principal string) []ACLRule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.desiredState[principal]
}

// ListDesiredPrincipals returns all principals with desired state.
func (e *Engine) ListDesiredPrincipals() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var principals []string
	for p := range e.desiredState {
		principals = append(principals, p)
	}
	sort.Strings(principals)
	return principals
}

// ComputeDiff compares desired ACL state against actual ACLs.
func (e *Engine) ComputeDiff(desired []ACLRule, actual []ACLRule) *ACLDiffResult {
	result := &ACLDiffResult{}

	actualSet := make(map[string]ACLRule)
	for _, a := range actual {
		key := ruleKey(a)
		actualSet[key] = a
	}

	desiredSet := make(map[string]ACLRule)
	for _, d := range desired {
		key := ruleKey(d)
		desiredSet[key] = d

		if _, exists := actualSet[key]; exists {
			result.InSync = append(result.InSync, ACLDiffEntry{Rule: d, Status: "in_sync"})
		} else {
			result.ToAdd = append(result.ToAdd, ACLDiffEntry{Rule: d, Status: "to_add"})
		}
	}

	for key, a := range actualSet {
		if _, exists := desiredSet[key]; !exists {
			result.ToRemove = append(result.ToRemove, ACLDiffEntry{Rule: a, Status: "to_remove"})
		}
	}

	result.Summary = fmt.Sprintf("%d to add, %d to remove, %d in sync", len(result.ToAdd), len(result.ToRemove), len(result.InSync))
	return result
}

// Audit analyzes ACL entries for security concerns.
func (e *Engine) Audit(acls []ACLRule) *AuditResult {
	result := &AuditResult{
		TotalACLs:      len(acls),
		ByPrincipal:    make(map[string]int),
		ByResourceType: make(map[string]int),
		AuditedAt:      time.Now(),
	}

	for _, acl := range acls {
		result.ByPrincipal[acl.Principal]++
		result.ByResourceType[acl.ResourceType]++

		// Check for wildcard resources
		if acl.ResourceName == "*" {
			result.WildcardRules = append(result.WildcardRules, AuditFinding{
				Rule:     acl,
				Finding:  fmt.Sprintf("Wildcard resource name grants %s access to all %s resources", acl.Operation, acl.ResourceType),
				Severity: "warning",
			})
		}

		// Check for over-permissioned (operation = "all")
		if acl.Operation == "all" || acl.Operation == "All" {
			result.OverPermissioned = append(result.OverPermissioned, AuditFinding{
				Rule:     acl,
				Finding:  fmt.Sprintf("Principal %q has ALL operations on %s:%s", acl.Principal, acl.ResourceType, acl.ResourceName),
				Severity: "warning",
			})
		}

		// Track deny rules
		if acl.Permission == "deny" || acl.Permission == "Deny" {
			result.DenyRules = append(result.DenyRules, acl)
		}
	}

	// Recommendations
	if len(result.WildcardRules) > 0 {
		result.Recommendations = append(result.Recommendations, "Replace wildcard resource ACLs with specific resource names for least-privilege access")
	}
	if len(result.OverPermissioned) > 0 {
		result.Recommendations = append(result.Recommendations, "Replace 'all' operation grants with specific operations needed")
	}
	if len(result.DenyRules) == 0 && len(acls) > 0 {
		result.Recommendations = append(result.Recommendations, "Consider adding deny rules for sensitive resources as defense-in-depth")
	}

	return result
}

// DryRun simulates applying desired state changes.
func (e *Engine) DryRun(desired, actual []ACLRule) *DryRunResult {
	diff := e.ComputeDiff(desired, actual)
	result := &DryRunResult{
		NoChange: len(diff.InSync),
	}
	for _, entry := range diff.ToAdd {
		result.WouldAdd = append(result.WouldAdd, entry.Rule)
	}
	for _, entry := range diff.ToRemove {
		result.WouldRemove = append(result.WouldRemove, entry.Rule)
	}
	result.Summary = fmt.Sprintf("Dry run: would add %d, remove %d, %d unchanged", len(result.WouldAdd), len(result.WouldRemove), result.NoChange)
	return result
}

func ruleKey(r ACLRule) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s",
		r.Principal, r.Host, r.ResourceType, r.ResourceName, r.PatternType, r.Operation, r.Permission)
}
