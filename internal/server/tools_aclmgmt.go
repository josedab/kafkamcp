package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/kafkamcp/kafkamcp/internal/aclmgmt"
	"github.com/kafkamcp/kafkamcp/internal/auth"
)

func (s *Server) handleACLManage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_acl_manage", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		action := request.GetString("action", "")
		if action == "" {
			return mcp.NewToolResultError("action is required"), nil
		}

		switch action {
		case "list":
			principal := request.GetString("principal", "")
			resourceType := request.GetString("resource_type", "")
			cluster := request.GetString("cluster", "")

			if deny := validateStringLength("principal", principal); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("resource_type", resourceType); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("cluster", cluster); deny != nil {
				return deny, nil
			}

			result, err := s.kafkaManager.ListACLs(ctx, cluster, principal, resourceType, true)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to list ACLs", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "success", result.TotalACLs, nil, start)
			return marshalResult(result)

		case "audit":
			principal := request.GetString("principal", "")
			resourceType := request.GetString("resource_type", "")
			cluster := request.GetString("cluster", "")

			if deny := validateStringLength("principal", principal); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("resource_type", resourceType); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("cluster", cluster); deny != nil {
				return deny, nil
			}

			result, err := s.kafkaManager.ListACLs(ctx, cluster, principal, resourceType, false)
			if err != nil {
				s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "error", 0, err, start)
				return s.toolError("Failed to list ACLs", err), nil
			}

			// Convert kafka ACLEntry to aclmgmt.ACLRule
			var rules []aclmgmt.ACLRule
			for _, acl := range result.ACLs {
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

			audit := s.aclMgmtEngine.Audit(rules)
			s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "success", audit.TotalACLs, nil, start)
			return marshalResult(audit)

		case "set_desired":
			principal := request.GetString("principal", "")
			if principal == "" {
				return mcp.NewToolResultError("principal is required"), nil
			}
			if deny := validateStringLength("principal", principal); deny != nil {
				return deny, nil
			}

			var rules []aclmgmt.ACLRule
			if rawArgs := request.GetRawArguments(); rawArgs != nil {
				if m, ok := rawArgs.(map[string]any); ok {
					if rulesRaw, ok := m["rules"]; ok {
						b, err := json.Marshal(rulesRaw)
						if err != nil {
							return s.toolError("Invalid rules format", err), nil
						}
						if err := json.Unmarshal(b, &rules); err != nil {
							return s.toolError("Invalid rules JSON", err), nil
						}
					}
				}
			}
			if len(rules) == 0 {
				return mcp.NewToolResultError("rules array is required"), nil
			}

			s.aclMgmtEngine.SetDesiredState(principal, rules)
			s.logAudit(ctx, agentID, "kafka_acl_manage", args, "", "success", len(rules), nil, start)
			return marshalResult(map[string]any{"status": "desired_state_set", "principal": principal, "rules": len(rules)})

		case "diff":
			principal := request.GetString("principal", "")
			cluster := request.GetString("cluster", "")
			if principal == "" {
				return mcp.NewToolResultError("principal is required"), nil
			}
			if deny := validateStringLength("principal", principal); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("cluster", cluster); deny != nil {
				return deny, nil
			}

			desired := s.aclMgmtEngine.GetDesiredState(principal)
			if len(desired) == 0 {
				return mcp.NewToolResultError("no desired state set for principal; use set_desired first"), nil
			}

			result, err := s.kafkaManager.ListACLs(ctx, cluster, principal, "", false)
			if err != nil {
				return s.toolError("Failed to list current ACLs", err), nil
			}

			var actual []aclmgmt.ACLRule
			for _, acl := range result.ACLs {
				actual = append(actual, aclmgmt.ACLRule{
					Principal:    acl.Principal,
					Host:         acl.Host,
					ResourceType: acl.ResourceType,
					ResourceName: acl.ResourceName,
					PatternType:  acl.PatternType,
					Operation:    acl.Operation,
					Permission:   acl.Permission,
				})
			}

			diff := s.aclMgmtEngine.ComputeDiff(desired, actual)
			s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "success", 1, nil, start)
			return marshalResult(diff)

		case "dry_run":
			principal := request.GetString("principal", "")
			cluster := request.GetString("cluster", "")
			if principal == "" {
				return mcp.NewToolResultError("principal is required"), nil
			}
			if deny := validateStringLength("principal", principal); deny != nil {
				return deny, nil
			}
			if deny := validateStringLength("cluster", cluster); deny != nil {
				return deny, nil
			}

			desired := s.aclMgmtEngine.GetDesiredState(principal)
			if len(desired) == 0 {
				return mcp.NewToolResultError("no desired state set for principal"), nil
			}

			result, err := s.kafkaManager.ListACLs(ctx, cluster, principal, "", false)
			if err != nil {
				return s.toolError("Failed to list current ACLs", err), nil
			}

			var actual []aclmgmt.ACLRule
			for _, acl := range result.ACLs {
				actual = append(actual, aclmgmt.ACLRule{
					Principal:    acl.Principal,
					Host:         acl.Host,
					ResourceType: acl.ResourceType,
					ResourceName: acl.ResourceName,
					PatternType:  acl.PatternType,
					Operation:    acl.Operation,
					Permission:   acl.Permission,
				})
			}

			dryRun := s.aclMgmtEngine.DryRun(desired, actual)
			s.logAudit(ctx, agentID, "kafka_acl_manage", args, cluster, "success", 1, nil, start)
			return marshalResult(dryRun)

		case "principals":
			principals := s.aclMgmtEngine.ListDesiredPrincipals()
			return marshalResult(map[string]any{"principals": principals, "total": len(principals)})

		default:
			return mcp.NewToolResultError(fmt.Sprintf("unknown action %q", action)), nil
		}
	})(ctx, request)
}
