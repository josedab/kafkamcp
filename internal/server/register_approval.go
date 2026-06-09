package server

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/approval"
	"github.com/josedab/kafkamcp/internal/auth"
)

// registerApprovalTools registers the approval-grant lifecycle tool. It gates
// the write/destructive ("ops-write") tools with short-lived, single-use,
// human-approvable grants. The tool itself does not mutate Kafka, so it is not
// in the write set.
func (s *Server) registerApprovalTools() {
	s.addTool(mcp.NewTool("kafka_approval",
		withCategory("governance"),
		mcp.WithDescription("Manage identity-bound, short-lived approval grants for ops-write tools. Grants are two-person by default, single-use, argument-scoped, persisted when SQLite is enabled, and recorded in a tamper-evident decision chain."),
		mcp.WithString("action", mcp.Required(), mcp.Description("request | approve | list | get | revoke | status")),
		mcp.WithString("tool", mcp.Description("Target write tool (for request), e.g. kafka_delete_topic")),
		mcp.WithObject("args", mcp.Description("Exact target-tool arguments excluding approval_token. Required for request."),
			mcp.AdditionalProperties(true)),
		mcp.WithString("request_reason", mcp.Description("Why the mutation is needed (for request)")),
		mcp.WithString("grant_id", mcp.Description("Grant identifier (for approve/get/revoke)")),
		mcp.WithNumber("ttl_seconds", mcp.Description("Requested grant lifetime in seconds (for approve); capped by config")),
		mcp.WithString("reason", mcp.Description("Reason (for revoke)")),
		mcp.WithString("agent_id", mcp.Description("Optional agent filter for list (admin only)")),
	), s.handleApproval)

	s.addTool(mcp.NewTool("kafka_governance_decision",
		withCategory("governance"),
		mcp.WithDescription("Simulate mutation policy, preview blast radius, generate least-privilege guidance, and verify/export tamper-evident approval decisions. Actions: simulate, verify, export."),
		mcp.WithString("action", mcp.Required(), mcp.Description("simulate | verify | export")),
		mcp.WithString("tool", mcp.Description("Target write tool (for simulate)")),
		mcp.WithObject("args", mcp.Description("Exact target-tool arguments (for simulate)"), mcp.AdditionalProperties(true)),
		mcp.WithString("grant_id", mcp.Description("Grant identifier (for verify)")),
		mcp.WithString("agent_id", mcp.Description("Optional agent filter (for export; admin only when different from caller)")),
	), s.handleGovernanceDecision)
}

func (s *Server) handleApproval(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_approval", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		if s.approvalManager == nil {
			return mcp.NewToolResultError("approval manager is not configured"), nil
		}
		action := request.GetString("action", "")
		switch action {
		case "request":
			tool := request.GetString("tool", "")
			if tool == "" {
				return mcp.NewToolResultError("tool is required for action=request"), nil
			}
			if !IsWriteTool(tool) {
				return mcp.NewToolResultError("tool is not a mutating/destructive tool; approval is only required for write tools"), nil
			}
			requestReason := request.GetString("request_reason", "")
			if requestReason == "" {
				return mcp.NewToolResultError("request_reason is required for action=request"), nil
			}
			targetArgs, ok := args["args"].(map[string]any)
			if !ok || len(targetArgs) == 0 {
				return mcp.NewToolResultError("args is required for action=request and must contain the exact target-tool arguments"), nil
			}
			if !requiresApproval(tool, targetArgs) {
				return mcp.NewToolResultError("the requested tool action is read-only and does not require approval"), nil
			}
			scope, err := s.approvalScopeFor(ctx, tool, agentID, targetArgs)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			policy, _ := policyForWriteTool(tool)
			decision := s.policyEngine.Evaluate(agentID, policy.ResourceType, scope.Resource, policy.Action)
			if !decision.Allowed {
				s.logAudit(ctx, agentID, "kafka_approval", args, scope.Cluster, "denied", 0, nil, start)
				return mcp.NewToolResultError("approval request denied by policy: " + decision.Reason + "; " +
					leastPrivilegeRecommendation(decision)), nil
			}
			preview := s.blastRadiusPreview(ctx, tool, scope, targetArgs)
			g, err := s.approvalManager.Request(ctx, approval.RequestSpec{
				Scope: scope, Preview: preview["summary"].(string), Requester: agentID, Reason: requestReason,
			})
			if err != nil {
				return s.toolError("failed to request approval", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_approval", args, scope.Cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{
				"action": "request", "grant": g,
				"policy_simulation": decision,
				"blast_radius":      preview,
				"request_reason":    requestReason,
				"recovery_guidance": recoveryGuidance(tool),
				"next":              "have an operator approve grant_id via action=approve; then call the target tool with approval_token=<token>",
			})

		case "approve":
			if deny := s.checkAuth(agentID, "kafka_approval", auth.ResourceCluster, "*", auth.ActionAdmin); deny != nil {
				return deny, nil
			}
			id := request.GetString("grant_id", "")
			if id == "" {
				return mcp.NewToolResultError("grant_id is required for action=approve"), nil
			}
			ttl := time.Duration(request.GetInt("ttl_seconds", 0)) * time.Second
			g, token, err := s.approvalManager.Approve(ctx, id, agentID, ttl)
			if err != nil {
				return s.toolError("failed to approve grant", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_approval", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{
				"action": "approve", "grant": g, "approval_token": token,
				"warning": "the token is shown once; pass it as approval_token to the target tool before it expires",
			})

		case "list":
			filter := request.GetString("agent_id", "")
			if filter == "" {
				filter = agentID
			}
			if filter != agentID && !s.isAdmin(agentID) {
				return mcp.NewToolResultError("admin role is required to list another agent's approvals"), nil
			}
			grants, err := s.approvalManager.List(ctx, filter)
			if err != nil {
				return s.toolError("failed to list grants", err), nil
			}
			return marshalResult(map[string]any{"action": "list", "grants": grants, "total": len(grants)})

		case "get":
			id := request.GetString("grant_id", "")
			if id == "" {
				return mcp.NewToolResultError("grant_id is required for action=get"), nil
			}
			g, err := s.approvalManager.Get(ctx, id)
			if err != nil {
				return s.toolError("failed to get grant", err), nil
			}
			if g.Scope.AgentID != agentID && !s.isAdmin(agentID) {
				return mcp.NewToolResultError("admin role is required to inspect another agent's approval"), nil
			}
			return marshalResult(map[string]any{"action": "get", "grant": g})

		case "revoke":
			id := request.GetString("grant_id", "")
			if id == "" {
				return mcp.NewToolResultError("grant_id is required for action=revoke"), nil
			}
			existing, err := s.approvalManager.Get(ctx, id)
			if err != nil {
				return s.toolError("failed to get grant", err), nil
			}
			if existing.Scope.AgentID != agentID && !s.isAdmin(agentID) {
				return mcp.NewToolResultError("admin role is required to revoke another agent's approval"), nil
			}
			g, err := s.approvalManager.Revoke(ctx, id, agentID, request.GetString("reason", ""))
			if err != nil {
				return s.toolError("failed to revoke grant", err), nil
			}
			s.logAudit(ctx, agentID, "kafka_approval", args, "", "success", 1, nil, start)
			return marshalResult(map[string]any{"action": "revoke", "grant": g})

		case "status":
			return marshalResult(map[string]any{
				"action":         "status",
				"enforced":       s.approvalEnforced,
				"write_tools":    WriteToolNames(),
				"token_argument": approvalTokenArg,
			})

		default:
			return mcp.NewToolResultError("unknown action; expected one of: request, approve, list, get, revoke, status"), nil
		}
	})(ctx, request)
}

func (s *Server) handleGovernanceDecision(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_governance_decision", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		switch request.GetString("action", "") {
		case "simulate":
			tool := request.GetString("tool", "")
			policy, ok := policyForWriteTool(tool)
			if !ok {
				return mcp.NewToolResultError("tool must be a supported ops-write tool"), nil
			}
			targetArgs, ok := args["args"].(map[string]any)
			if !ok || len(targetArgs) == 0 {
				return mcp.NewToolResultError("args is required for action=simulate"), nil
			}
			if !requiresApproval(tool, targetArgs) {
				return marshalResult(map[string]any{
					"action":            "simulate",
					"approval_required": false,
					"message":           "the requested tool action is read-only",
				})
			}
			scope, err := s.approvalScopeFor(ctx, tool, agentID, targetArgs)
			if err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
			decision := s.policyEngine.Evaluate(agentID, policy.ResourceType, scope.Resource, policy.Action)
			s.logAudit(ctx, agentID, "kafka_governance_decision", args, scope.Cluster, "success", 1, nil, start)
			return marshalResult(map[string]any{
				"action":                         "simulate",
				"scope":                          scope,
				"policy_decision":                decision,
				"blast_radius":                   s.blastRadiusPreview(ctx, tool, scope, targetArgs),
				"recovery_guidance":              recoveryGuidance(tool),
				"least_privilege_recommendation": leastPrivilegeRecommendation(decision),
			})
		case "verify":
			id := request.GetString("grant_id", "")
			if id == "" {
				return mcp.NewToolResultError("grant_id is required for action=verify"), nil
			}
			g, err := s.approvalManager.Get(ctx, id)
			if err != nil {
				return s.toolError("failed to get grant", err), nil
			}
			if g.Scope.AgentID != agentID && !s.isAdmin(agentID) {
				return mcp.NewToolResultError("admin role is required to verify another agent's approval"), nil
			}
			if err := s.approvalManager.Verify(ctx, id); err != nil {
				return s.toolError("decision verification failed", err), nil
			}
			return marshalResult(map[string]any{"action": "verify", "valid": true, "grant": g})
		case "export":
			filter := request.GetString("agent_id", "")
			if filter == "" {
				filter = agentID
			}
			if filter != agentID && !s.isAdmin(agentID) {
				return mcp.NewToolResultError("admin role is required to export another agent's decisions"), nil
			}
			grants, err := s.approvalManager.List(ctx, filter)
			if err != nil {
				return s.toolError("failed to export decisions", err), nil
			}
			return marshalResult(map[string]any{"action": "export", "grants": grants, "total": len(grants)})
		default:
			return mcp.NewToolResultError("unknown action; expected one of: simulate, verify, export"), nil
		}
	})(ctx, request)
}

func (s *Server) isAdmin(agentID string) bool {
	role, ok := s.policyEngine.AgentRole(agentID)
	return ok && role == "admin"
}

func leastPrivilegeRecommendation(decision auth.Decision) string {
	if decision.Allowed {
		return "No policy change is required; the current policy already grants this exact action."
	}
	switch decision.ResourceType {
	case auth.ResourceTopic:
		return "Grant only permission " + string(decision.Action) + " on the exact topic pattern " + decision.ResourceName + " to agent " + decision.AgentID + "."
	case auth.ResourceConsumerGroup:
		return "Grant only consumer_groups permission " + string(decision.Action) + " to agent " + decision.AgentID + "."
	case auth.ResourceSchema:
		return "Grant only schemas permission " + string(decision.Action) + " to agent " + decision.AgentID + "."
	case auth.ResourceCluster:
		return "Assign the admin role only if this cluster-wide mutation is required; prefer a topic-scoped tool when possible."
	default:
		return "Do not broaden policy until the target resource and action are explicit."
	}
}

// blastRadiusPreview returns a server-derived preview. It never trusts a
// caller-supplied summary and uses read-only broker metadata where available.
func (s *Server) blastRadiusPreview(ctx context.Context, tool string, scope approval.Scope, args map[string]any) map[string]any {
	target := scope.Resource
	if target == "" {
		target = "(unspecified resource)"
	}
	cluster := scope.Cluster
	if cluster == "" {
		cluster = "default"
	}
	preview := map[string]any{
		"tool":     tool,
		"cluster":  cluster,
		"resource": target,
	}
	switch tool {
	case "kafka_delete_topic":
		summary := "DESTRUCTIVE: permanently deletes topic " + target + " (and all its data) on cluster " + cluster
		if detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, target); err == nil {
			var retained int64
			for _, partition := range detail.Partitions {
				retained += partition.LatestOffset - partition.EarliestOffset
			}
			preview["partitions"] = len(detail.Partitions)
			preview["estimated_retained_records"] = retained
			summary += fmt.Sprintf("; %d partitions and approximately %d retained records are in scope", len(detail.Partitions), retained)
		} else {
			preview["inspection_warning"] = "topic metadata unavailable: " + categorizeError(err)
		}
		preview["summary"] = summary
	case "kafka_reset_offsets":
		summary := "DISRUPTIVE: resets consumer group offsets for " + target + " on cluster " + cluster + "; may cause reprocessing or data skips"
		if detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, target); err == nil {
			preview["current_state"] = detail.State
			preview["current_total_lag"] = detail.TotalLag
			preview["members"] = len(detail.Members)
		} else {
			preview["inspection_warning"] = "consumer-group metadata unavailable: " + categorizeError(err)
		}
		preview["summary"] = summary
	case "kafka_alter_topic":
		preview["summary"] = "MUTATING: alters topic " + target + " on cluster " + cluster
		if detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, target); err == nil {
			preview["current_partitions"] = len(detail.Partitions)
		} else {
			preview["inspection_warning"] = "topic metadata unavailable: " + categorizeError(err)
		}
		if partitions, ok := args["partitions"]; ok {
			preview["requested_partitions"] = partitions
		}
		if configs, ok := args["configs"].(map[string]any); ok {
			preview["config_keys"] = sortedMapKeys(configs)
		}
	case "kafka_acl_manage":
		aclAction, _ := args["action"].(string)
		preview["acl_action"] = aclAction
		preview["summary"] = "SECURITY: reconciles broker ACLs on cluster " + cluster + " (creates missing and deletes obsolete exact ACLs)"
		if rules, ok := args["rules"].([]any); ok {
			preview["desired_rules"] = len(rules)
		}
	case "kafka_produce":
		preview["summary"] = "WRITE: produces message(s) to topic " + target + " on cluster " + cluster
		if messages, ok := args["messages"].([]any); ok {
			preview["messages"] = len(messages)
		}
	case "kafka_create_topic":
		preview["summary"] = "CREATE: creates topic " + target + " on cluster " + cluster
		preview["partitions"] = args["partitions"]
		preview["replication_factor"] = args["replication_factor"]
	case "kafka_create_connector", "kafka_update_connector", "kafka_delete_connector", "kafka_restart_connector":
		preview["summary"] = "CONNECTOR MUTATION: " + tool + " for " + target + " on cluster " + cluster
	default:
		preview["summary"] = "MUTATING: " + tool + " on " + target + " (cluster " + cluster + ")"
	}
	return preview
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// recoveryGuidance returns rollback/recovery guidance for a write tool. Most
// Kafka mutations are NOT automatically reversible, so this guidance describes
// the manual recovery path an operator should be prepared to execute. It is
// surfaced at approval-request time so the blast radius and recovery cost are
// visible before an operation is approved.
func recoveryGuidance(tool string) string {
	switch tool {
	case "kafka_delete_topic":
		return "NOT reversible: deleting a topic permanently discards its data. Recovery requires restoring from a backup/mirror or replaying from an upstream source; recreate the topic with the same config before replay."
	case "kafka_reset_offsets":
		return "Recoverable: record the current committed offsets before resetting (kafka_describe_consumer_group). To undo, reset the group back to the saved offsets. Reprocessing side effects on downstream systems are NOT automatically reversible."
	case "kafka_alter_topic":
		return "Partially reversible: capture the current config (kafka_describe_topic) before altering. Config changes can be re-applied; partition increases CANNOT be undone."
	case "kafka_acl_manage":
		return "Reversible: ACL changes can be reverted by re-applying the previous ACL state. Capture existing ACLs (kafka_acl_manage action=list) before reconciling."
	case "kafka_produce":
		return "NOT reversible: produced/replayed records cannot be un-sent. Downstream consumers may have already processed them; use idempotent producers and unique keys to make retries safe."
	case "kafka_create_topic":
		return "Reversible: a newly created topic can be deleted (kafka_delete_topic) if it was created in error, provided no critical data has been produced to it."
	case "kafka_create_connector", "kafka_update_connector", "kafka_delete_connector", "kafka_restart_connector":
		return "Recoverable: capture the connector config before changing it. Recreate/reconfigure from the saved config to roll back; in-flight data may be duplicated or skipped depending on the connector's offset handling."
	default:
		return "This operation may not be automatically reversible. Capture the current state before proceeding and prepare a manual recovery plan."
	}
}
