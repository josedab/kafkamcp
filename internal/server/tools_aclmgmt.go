package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/aclmgmt"
	"github.com/josedab/kafkamcp/internal/auth"
	"github.com/josedab/kafkamcp/internal/kafka"
)

// handleACLManage inspects and reconciles live Kafka ACLs. It is fully
// stateless: read-only actions (list, audit, diff, dry_run) read live broker
// ACLs, and the reconciling actions (apply, reconcile) create missing and
// delete obsolete exact ACLs on the broker. The desired state is always
// supplied by the caller; KafkaMCP keeps no in-memory desired-state plan.
func (s *Server) handleACLManage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_acl_manage", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required; expected one of: list, audit, diff, dry_run, apply, reconcile"), nil
		}
		cluster := request.GetString("cluster", "")
		principal := request.GetString("principal", "")
		resourceType := request.GetString("resource_type", "")
		if deny := validateStringLength("principal", principal); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("resource_type", resourceType); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		switch action {
		case "list":
			result, err := s.kafkaManager.ListACLs(ctx, cluster, principal, resourceType, true)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to list ACLs", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "success", result.TotalACLs, nil, start)
			return marshalResult(result)

		case "audit":
			result, err := s.kafkaManager.ListACLs(ctx, cluster, principal, resourceType, false)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to list ACLs", err), nil
			}
			auditResult := s.aclMgmtEngine.Audit(aclRulesFromEntries(result.ACLs))
			s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "success", auditResult.TotalACLs, nil, start)
			return marshalResult(auditResult)

		case "diff", "dry_run", "apply", "reconcile":
			desired, invalid, err := s.parseDesiredRules(request, principal)
			if err != nil {
				return s.toolError("Invalid rules", err), nil
			}
			if len(invalid) > 0 {
				return marshalResult(map[string]any{
					"status":        "validation_failed",
					"invalid_rules": invalid,
					"valid_rules":   len(desired),
					"message":       "one or more desired ACL rules are invalid; fix them before diff/apply",
				})
			}
			if len(desired) == 0 {
				return mcp.NewToolResultError("rules array is required for action=" + action + " (each rule: {principal, host, resource_type, resource_name, pattern_type, operation, permission})"), nil
			}

			actual, err := s.liveACLsForPrincipals(ctx, cluster, principal, desired)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to list current ACLs", err), nil
			}

			diff := s.aclMgmtEngine.ComputeDiff(desired, actual)
			switch action {
			case "diff":
				s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "success", len(diff.ToAdd)+len(diff.ToRemove), nil, start)
				return marshalResult(diff)
			case "dry_run":
				s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "success", len(diff.ToAdd)+len(diff.ToRemove), nil, start)
				return marshalResult(s.aclMgmtEngine.DryRun(desired, actual))
			default: // apply, reconcile
				return s.applyACLReconcile(ctx, agentID, args, cluster, action, diff, start)
			}

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; expected one of: list, audit, diff, dry_run, apply, reconcile", action)), nil
		}
	})(ctx, request)
}

// applyACLReconcile creates missing and deletes obsolete exact ACLs, returning
// per-rule results. It never aborts on the first error: every rule is attempted
// and its outcome recorded so the caller can retry only the failures.
func (s *Server) applyACLReconcile(ctx context.Context, agentID string, args map[string]any, cluster, action string, diff *aclmgmt.ACLDiffResult, start time.Time) (*mcp.CallToolResult, error) {
	results := make([]kafka.ACLMutationResult, 0, len(diff.ToAdd)+len(diff.ToRemove))
	created, deleted, failed := 0, 0, 0

	for _, entry := range diff.ToAdd {
		rule := aclEntryFromRule(entry.Rule)
		res := kafka.ACLMutationResult{Rule: rule}
		if err := s.kafkaManager.CreateACL(ctx, cluster, rule); err != nil {
			res.Status = "error"
			res.Error = err.Error()
			failed++
		} else {
			res.Status = "created"
			created++
		}
		results = append(results, res)
	}

	if failed > 0 {
		for _, entry := range diff.ToRemove {
			results = append(results, kafka.ACLMutationResult{
				Rule:   aclEntryFromRule(entry.Rule),
				Status: "skipped",
				Error:  "deletion skipped because one or more ACL creates failed",
			})
		}
		s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "error", created, fmt.Errorf("%d ACL creates failed", failed), start)
		result, err := marshalResult(map[string]any{
			"status":    "failed",
			"action":    action,
			"cluster":   s.resolveClusterName(cluster),
			"created":   created,
			"deleted":   0,
			"failed":    failed,
			"unchanged": len(diff.InSync),
			"results":   results,
			"summary":   fmt.Sprintf("%s stopped before deletion: %d created, %d create failures, %d deletions skipped", action, created, failed, len(diff.ToRemove)),
		})
		if err != nil {
			return nil, err
		}
		result.IsError = true
		return result, nil
	}

	for _, entry := range diff.ToRemove {
		rule := aclEntryFromRule(entry.Rule)
		res := kafka.ACLMutationResult{Rule: rule}
		matched, err := s.kafkaManager.DeleteACL(ctx, cluster, rule)
		switch {
		case err != nil:
			res.Status = "error"
			res.Error = err.Error()
			failed++
		case matched == 0:
			res.Status = "not_found"
		default:
			res.Status = "deleted"
			res.Matched = matched
			deleted++
		}
		results = append(results, res)
	}

	status := "applied"
	if failed > 0 {
		status = "partial"
	}
	var applyErr error
	if failed > 0 {
		applyErr = fmt.Errorf("%d ACL deletions failed", failed)
	}
	s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, status, created+deleted, applyErr, start)
	result, err := marshalResult(map[string]any{
		"status":    status,
		"action":    action,
		"cluster":   s.resolveClusterName(cluster),
		"created":   created,
		"deleted":   deleted,
		"failed":    failed,
		"unchanged": len(diff.InSync),
		"results":   results,
		"summary":   fmt.Sprintf("%s: %d created, %d deleted, %d unchanged, %d failed", action, created, deleted, len(diff.InSync), failed),
	})
	if err != nil {
		return nil, err
	}
	if failed > 0 {
		result.IsError = true
	}
	return result, nil
}

// parseDesiredRules extracts, normalises and validates the caller-supplied
// desired ACL rules. It returns the valid normalised rules, a list of
// human-readable validation errors for invalid rules, and a hard error only
// when the rules payload itself is malformed.
func (s *Server) parseDesiredRules(request mcp.CallToolRequest, principal string) ([]aclmgmt.ACLRule, []string, error) {
	var raw []aclmgmt.ACLRule
	if rawArgs := request.GetRawArguments(); rawArgs != nil {
		if m, ok := rawArgs.(map[string]any); ok {
			if rulesRaw, ok := m["rules"]; ok && rulesRaw != nil {
				b, err := json.Marshal(rulesRaw)
				if err != nil {
					return nil, nil, err
				}
				if err := json.Unmarshal(b, &raw); err != nil {
					return nil, nil, fmt.Errorf("rules must be an array of {principal, host, resource_type, resource_name, pattern_type, operation, permission}: %w", err)
				}
			}
		}
	}

	normalised := aclmgmt.NormalizeRules(principal, raw)
	valid := make([]aclmgmt.ACLRule, 0, len(normalised))
	var invalid []string
	for i, rule := range normalised {
		if err := kafka.ValidateACLRule(aclEntryFromRule(rule)); err != nil {
			invalid = append(invalid, fmt.Sprintf("rule[%d] (%s %s:%s %s): %s", i, rule.Principal, rule.ResourceType, rule.ResourceName, rule.Operation, err.Error()))
			continue
		}
		valid = append(valid, rule)
	}
	return valid, invalid, nil
}

// liveACLsForPrincipals fetches the current broker ACLs that are candidates for
// reconciliation. To avoid deleting unrelated ACLs, the actual set is scoped to
// the principals present in the desired rules (plus any explicit principal
// filter).
func (s *Server) liveACLsForPrincipals(ctx context.Context, cluster, principal string, desired []aclmgmt.ACLRule) ([]aclmgmt.ACLRule, error) {
	principals := map[string]struct{}{}
	if principal != "" {
		principals[principal] = struct{}{}
	}
	for _, r := range desired {
		if r.Principal != "" {
			principals[r.Principal] = struct{}{}
		}
	}
	if len(principals) == 0 {
		principals[""] = struct{}{}
	}

	names := make([]string, 0, len(principals))
	for p := range principals {
		names = append(names, p)
	}
	sort.Strings(names)

	seen := map[string]struct{}{}
	var out []aclmgmt.ACLRule
	for _, p := range names {
		result, err := s.kafkaManager.ListACLs(ctx, cluster, p, "", false)
		if err != nil {
			return nil, err
		}
		for _, acl := range result.ACLs {
			key := fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s", acl.Principal, acl.Host, acl.ResourceType, acl.ResourceName, acl.PatternType, acl.Operation, acl.Permission)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, aclmgmt.NormalizeRules("", []aclmgmt.ACLRule{{
				Principal:    acl.Principal,
				Host:         acl.Host,
				ResourceType: acl.ResourceType,
				ResourceName: acl.ResourceName,
				PatternType:  acl.PatternType,
				Operation:    acl.Operation,
				Permission:   acl.Permission,
			}})[0])
		}
	}
	return out, nil
}

func aclRulesFromEntries(entries []kafka.ACLEntry) []aclmgmt.ACLRule {
	rules := make([]aclmgmt.ACLRule, 0, len(entries))
	for _, acl := range entries {
		rules = append(rules, aclmgmt.ACLRule{
			Principal:    acl.Principal,
			Host:         acl.Host,
			ResourceType: acl.ResourceType,
			ResourceName: acl.ResourceName,
			PatternType:  acl.PatternType,
			Operation:    acl.Operation,
			Permission:   acl.Permission,
		})
	}
	return rules
}

func aclEntryFromRule(rule aclmgmt.ACLRule) kafka.ACLEntry {
	return kafka.ACLEntry{
		Principal:    rule.Principal,
		Host:         rule.Host,
		ResourceType: rule.ResourceType,
		ResourceName: rule.ResourceName,
		PatternType:  rule.PatternType,
		Operation:    rule.Operation,
		Permission:   rule.Permission,
	}
}
