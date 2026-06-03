package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ACLMutationResult reports the outcome of a single ACL create/delete request.
type ACLMutationResult struct {
	Rule    ACLEntry `json:"rule"`
	Status  string   `json:"status"` // created | deleted | not_found | error
	Matched int      `json:"matched,omitempty"`
	Error   string   `json:"error,omitempty"`
}

// canonicalResourceType normalises the many spellings of a Kafka ACL resource
// type into the lowercase token accepted by kmsg.ParseACLResourceType.
func canonicalResourceType(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "topic", "topics":
		return "topic"
	case "group", "groups", "consumer_group", "consumer-group":
		return "group"
	case "cluster":
		return "cluster"
	case "transactional_id", "transactionalid", "transactional-id", "txn", "txnid":
		return "transactionalid"
	case "delegation_token", "delegationtoken", "delegation-token", "token":
		return "delegationtoken"
	default:
		return strings.ToLower(strings.TrimSpace(t))
	}
}

// ValidateACLRule validates a single ACL rule before it is applied to a broker.
// It returns a descriptive error naming the first invalid field so callers can
// surface a per-rule reason. Defaults are NOT applied here; use
// NormalizeACLRule for that.
func ValidateACLRule(rule ACLEntry) error {
	principal := strings.TrimSpace(rule.Principal)
	if principal == "" {
		return fmt.Errorf("principal is required (e.g. User:alice)")
	}
	if !strings.Contains(principal, ":") {
		return fmt.Errorf("principal %q must be of the form PrincipalType:name (e.g. User:alice)", principal)
	}

	if strings.TrimSpace(rule.Host) == "" {
		return fmt.Errorf("host is required (use * for any host)")
	}

	resourceType := canonicalResourceType(rule.ResourceType)
	if _, err := kmsg.ParseACLResourceType(resourceType); err != nil {
		return fmt.Errorf("invalid resource_type %q: want topic, group, cluster, transactional_id or delegation_token", rule.ResourceType)
	}
	switch resourceType {
	case "any", "user":
		return fmt.Errorf("invalid resource_type %q for an ACL rule", rule.ResourceType)
	case "cluster":
		// The cluster resource name is fixed by the Kafka protocol.
		if name := strings.TrimSpace(rule.ResourceName); name != "" && name != "kafka-cluster" {
			return fmt.Errorf("cluster ACLs must not set resource_name (got %q); it is always kafka-cluster", rule.ResourceName)
		}
	default:
		if strings.TrimSpace(rule.ResourceName) == "" {
			return fmt.Errorf("resource_name is required for resource_type %q", resourceType)
		}
	}

	pattern := strings.ToLower(strings.TrimSpace(rule.PatternType))
	if pattern == "" {
		pattern = "literal"
	}
	if pattern != "literal" && pattern != "prefixed" {
		return fmt.Errorf("invalid pattern_type %q: want literal or prefixed", rule.PatternType)
	}

	op, err := kmsg.ParseACLOperation(strings.TrimSpace(rule.Operation))
	if err != nil {
		return fmt.Errorf("invalid operation %q", rule.Operation)
	}
	switch op {
	case kmsg.ACLOperationUnknown, kmsg.ACLOperationAny:
		return fmt.Errorf("operation %q cannot be used to create an ACL", rule.Operation)
	}

	permission, err := kmsg.ParseACLPermissionType(strings.TrimSpace(rule.Permission))
	if err != nil {
		return fmt.Errorf("invalid permission %q: want allow or deny", rule.Permission)
	}
	if permission != kmsg.ACLPermissionTypeAllow && permission != kmsg.ACLPermissionTypeDeny {
		return fmt.Errorf("invalid permission %q: want allow or deny", rule.Permission)
	}
	return nil
}

// NormalizeACLRule returns a copy of rule with defaults applied (host *,
// pattern literal, permission allow) and canonical resource type.
func NormalizeACLRule(rule ACLEntry) ACLEntry {
	out := rule
	out.Principal = strings.TrimSpace(rule.Principal)
	out.ResourceType = canonicalResourceType(rule.ResourceType)
	out.ResourceName = strings.TrimSpace(rule.ResourceName)
	if strings.TrimSpace(out.Host) == "" {
		out.Host = "*"
	}
	if strings.TrimSpace(out.PatternType) == "" {
		out.PatternType = "literal"
	} else {
		out.PatternType = strings.ToLower(strings.TrimSpace(out.PatternType))
	}
	if strings.TrimSpace(out.Permission) == "" {
		out.Permission = "allow"
	} else {
		out.Permission = strings.ToLower(strings.TrimSpace(out.Permission))
	}
	out.Operation = strings.ToLower(strings.TrimSpace(out.Operation))
	return out
}

// aclBuilderForRule builds a single-rule kadm ACL builder describing exactly
// one (principal, host, resource, operation, permission) tuple. The rule must
// already have passed ValidateACLRule.
func aclBuilderForRule(rule ACLEntry) (*kadm.ACLBuilder, error) {
	rule = NormalizeACLRule(rule)

	op, err := kmsg.ParseACLOperation(rule.Operation)
	if err != nil {
		return nil, fmt.Errorf("invalid operation %q: %w", rule.Operation, err)
	}
	perm, err := kmsg.ParseACLPermissionType(rule.Permission)
	if err != nil {
		return nil, fmt.Errorf("invalid permission %q: %w", rule.Permission, err)
	}

	b := kadm.NewACLs().Operations(op)

	switch rule.PatternType {
	case "prefixed":
		b.ResourcePatternType(kadm.ACLPatternPrefixed)
	default:
		b.ResourcePatternType(kadm.ACLPatternLiteral)
	}

	switch rule.ResourceType {
	case "topic":
		b.Topics(rule.ResourceName)
	case "group":
		b.Groups(rule.ResourceName)
	case "cluster":
		b.Clusters()
	case "transactionalid":
		b.TransactionalIDs(rule.ResourceName)
	case "delegationtoken":
		b.DelegationTokens(rule.ResourceName)
	default:
		return nil, fmt.Errorf("unsupported resource_type %q", rule.ResourceType)
	}

	if perm == kmsg.ACLPermissionTypeDeny {
		b.Deny(rule.Principal)
		b.DenyHosts(rule.Host)
	} else {
		b.Allow(rule.Principal)
		b.AllowHosts(rule.Host)
	}
	return b, nil
}

// CreateACL creates a single exact ACL rule on the cluster. It is idempotent
// from the caller's perspective: creating an ACL that already exists is a
// no-op on the broker and is reported as "created".
func (m *Manager) CreateACL(ctx context.Context, cluster string, rule ACLEntry) error {
	if err := ValidateACLRule(rule); err != nil {
		return err
	}
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return err
	}
	b, err := aclBuilderForRule(rule)
	if err != nil {
		return err
	}
	results, err := admin.CreateACLs(ctx, b)
	if err != nil {
		return fmt.Errorf("creating ACL: %w", err)
	}
	for _, r := range results {
		if r.Err != nil {
			if r.ErrMessage != "" {
				return fmt.Errorf("creating ACL: %s: %w", r.ErrMessage, r.Err)
			}
			return fmt.Errorf("creating ACL: %w", r.Err)
		}
	}
	return nil
}

// DeleteACL deletes a single exact ACL rule from the cluster. It returns the
// number of broker ACLs that matched the delete filter (0 when the rule did
// not exist).
func (m *Manager) DeleteACL(ctx context.Context, cluster string, rule ACLEntry) (int, error) {
	if err := ValidateACLRule(rule); err != nil {
		return 0, err
	}
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return 0, err
	}
	b, err := aclBuilderForRule(rule)
	if err != nil {
		return 0, err
	}
	results, err := admin.DeleteACLs(ctx, b)
	if err != nil {
		return 0, fmt.Errorf("deleting ACL: %w", err)
	}
	matched := 0
	for _, r := range results {
		if r.Err != nil {
			if r.ErrMessage != "" {
				return matched, fmt.Errorf("deleting ACL: %s: %w", r.ErrMessage, r.Err)
			}
			return matched, fmt.Errorf("deleting ACL: %w", r.Err)
		}
		matched += len(r.Deleted)
	}
	return matched, nil
}
