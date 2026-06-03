package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
)

// ACLEntry represents a single Kafka ACL.
type ACLEntry struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	ResourceType string `json:"resource_type"`
	ResourceName string `json:"resource_name"`
	Operation    string `json:"operation"`
	Permission   string `json:"permission"`
	PatternType  string `json:"pattern_type"`
}

// ACLResult contains ACL listing and security analysis.
type ACLResult struct {
	ACLs             []ACLEntry        `json:"acls"`
	TotalACLs        int               `json:"total_acls"`
	SecurityAnalysis *SecurityAnalysis `json:"security_analysis,omitempty"`
}

// SecurityAnalysis summarizes risky patterns in a set of Kafka ACLs.
type SecurityAnalysis struct {
	OverPermissioned []SecurityFinding `json:"over_permissioned"`
	WildcardACLs     []SecurityFinding `json:"wildcard_acls"`
	Findings         int               `json:"total_findings"`
}

// SecurityFinding describes one potentially risky Kafka ACL pattern.
type SecurityFinding struct {
	Principal  string  `json:"principal"`
	Issue      string  `json:"issue"`
	Severity   string  `json:"severity"`
	Confidence float64 `json:"confidence"`
}

// ListACLs returns Kafka ACLs matching the optional principal and resource filters.
func (m *Manager) ListACLs(ctx context.Context, cluster, principal, resourceType string, includeAnalysis bool) (*ACLResult, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	builder := kadm.NewACLs().
		AnyResource().
		Allow().
		Deny().
		AllowHosts().
		DenyHosts().
		Operations(kadm.OpAny).
		ResourcePatternType(kadm.ACLPatternAny)

	aclResults, err := admin.DescribeACLs(ctx, builder)
	if err != nil {
		return nil, fmt.Errorf("describing ACLs: %w", err)
	}

	result := &ACLResult{}
	for _, aclResult := range aclResults {
		for _, acl := range aclResult.Described {
			entry := ACLEntry{
				Principal:    acl.Principal,
				Host:         acl.Host,
				ResourceType: acl.Type.String(),
				ResourceName: acl.Name,
				Operation:    acl.Operation.String(),
				Permission:   acl.Permission.String(),
				PatternType:  acl.Pattern.String(),
			}
			if !aclMatchesFilters(entry, principal, resourceType) {
				continue
			}
			result.ACLs = append(result.ACLs, entry)
		}
	}
	result.TotalACLs = len(result.ACLs)
	if includeAnalysis {
		result.SecurityAnalysis = analyzeACLSecurity(result.ACLs)
	}
	return result, nil
}

func aclMatchesFilters(entry ACLEntry, principal, resourceType string) bool {
	if principal != "" && entry.Principal != principal {
		return false
	}
	return resourceType == "" || strings.EqualFold(entry.ResourceType, resourceType)
}

func analyzeACLSecurity(acls []ACLEntry) *SecurityAnalysis {
	analysis := &SecurityAnalysis{}
	principalOps := make(map[string][]ACLEntry)
	for _, acl := range acls {
		principalOps[acl.Principal] = append(principalOps[acl.Principal], acl)
	}
	for principal, rules := range principalOps {
		for _, rule := range rules {
			if rule.ResourceName == "*" {
				analysis.WildcardACLs = append(analysis.WildcardACLs, SecurityFinding{
					Principal:  principal,
					Issue:      fmt.Sprintf("Wildcard %s access on %s resources", rule.Operation, rule.ResourceType),
					Severity:   "warning",
					Confidence: 0.9,
				})
			}
		}
		if len(rules) > 10 {
			analysis.OverPermissioned = append(analysis.OverPermissioned, SecurityFinding{
				Principal:  principal,
				Issue:      fmt.Sprintf("Principal has %d ACL entries - consider consolidation", len(rules)),
				Severity:   "info",
				Confidence: 0.7,
			})
		}
	}
	analysis.Findings = len(analysis.OverPermissioned) + len(analysis.WildcardACLs)
	return analysis
}
