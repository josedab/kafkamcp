package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/approval"
	"github.com/josedab/kafkamcp/internal/audit"
)

// approvalTokenArg is the tool argument carrying a short-lived approval token.
const approvalTokenArg = "approval_token"

// approvalScopeFor builds the approval scope for a write-tool invocation. The
// argument digest excludes the approval token itself so that presenting the
// token does not change the digest the grant was minted against.
func (s *Server) approvalScopeFor(ctx context.Context, tool, agentID string, args map[string]any) (approval.Scope, error) {
	policy, ok := policyForWriteTool(tool)
	if !ok {
		return approval.Scope{}, fmt.Errorf("tool %q is not in the supported mutation policy", tool)
	}
	cluster := s.resolveClusterName("")
	resource := ""
	if args != nil {
		if c, ok := args["cluster"].(string); ok {
			cluster = s.resolveClusterName(c)
		}
		for _, k := range policy.ResourceKeys {
			if v, ok := args[k]; ok {
				resource = fmt.Sprintf("%v", v)
				break
			}
		}
	}
	method := "stdio-local"
	if ident := identityFromContext(ctx); ident != nil {
		method = ident.Method
	} else if s.config != nil && s.config.NetworkTransport() {
		return approval.Scope{}, fmt.Errorf("verified identity is required for network approval")
	}
	return approval.Scope{
		AgentID:        agentID,
		IdentityMethod: method,
		Tool:           tool,
		Cluster:        cluster,
		Resource:       resource,
		ArgsDigest:     approval.DigestArgs(argsWithoutControlArgs(args)),
	}, nil
}

// argsWithoutControlArgs returns a copy without authorization/retry metadata.
// These values control KafkaMCP execution but are not target-operation inputs.
func argsWithoutControlArgs(args map[string]any) map[string]any {
	if len(args) == 0 {
		return args
	}
	out := make(map[string]any, len(args))
	for k, v := range args {
		if k == approvalTokenArg || k == idempotencyKeyArg {
			continue
		}
		out[k] = v
	}
	return out
}

// enforceApproval checks the approval grant for a write/destructive tool. It
// returns a non-nil MCP error result when the operation must be denied, or nil
// when the call may proceed. Read-only tools and disabled enforcement short
// circuit to nil. This is the single common enforcement path for all write
// tools; it is keyed by the canonical tool name and cannot be bypassed via
// header fallback or tool aliases.
func (s *Server) enforceApproval(ctx context.Context, tool, agentID string, req mcp.CallToolRequest, args map[string]any) *mcp.CallToolResult {
	if !s.approvalEnforced || s.approvalManager == nil {
		return nil
	}
	if !requiresApproval(tool, args) {
		return nil
	}

	token := req.GetString(approvalTokenArg, "")
	if token == "" {
		s.recordDeniedUnsafe(tool)
		return mcp.NewToolResultError(fmt.Sprintf(
			"tool %q is a mutating operation and requires an approval grant. "+
				"Request one via kafka_approval (action=request), have it approved, "+
				"then re-invoke with %q=<token>.", tool, approvalTokenArg))
	}

	scope, err := s.approvalScopeFor(ctx, tool, agentID, args)
	if err != nil {
		s.recordDeniedUnsafe(tool)
		return mcp.NewToolResultError(err.Error())
	}
	if _, err := s.approvalManager.Consume(ctx, token, scope); err != nil {
		s.recordDeniedUnsafe(tool)
		return mcp.NewToolResultError(fmt.Sprintf("approval denied for %q: %s", tool, approvalDenyReason(err)))
	}
	if s.activation != nil {
		s.activation.RecordCoreWorkflow("approved_write")
	}
	return nil
}

func requiresApproval(tool string, args map[string]any) bool {
	if !IsWriteTool(tool) {
		return false
	}
	if tool == "kafka_acl_manage" {
		// Only the reconciling actions mutate broker ACLs; the read-only
		// actions (list, audit, diff, dry_run) never require approval.
		action, _ := args["action"].(string)
		switch action {
		case "apply", "reconcile":
			return true
		default:
			return false
		}
	}
	return true
}

// recordDeniedUnsafe records a denied write/destructive call in activation
// metrics (bounded tool label).
func (s *Server) recordDeniedUnsafe(tool string) {
	if s.activation != nil {
		s.activation.RecordDeniedUnsafe(tool)
	}
}

// approvalDenyReason maps approval errors to a stable, non-leaky message.
func approvalDenyReason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, approval.ErrScopeMismatch):
		return "the approval does not match this agent/tool/cluster/resource/arguments"
	default:
		return err.Error()
	}
}

// approvalAuditHook returns a lifecycle audit hook that records approval events
// through the server logger. Grant tokens are never included.
func (s *Server) approvalAuditHook() approval.AuditFunc {
	return func(event, grantID string, scope approval.Scope, detail map[string]any) {
		if s.activation != nil {
			s.activation.RecordApprovalOutcome(event)
		}
		if s.logger == nil {
			return
		}
		actor := scope.AgentID
		if value, ok := detail["actor"].(string); ok && value != "" {
			actor = value
		}
		s.logger.Info("approval lifecycle",
			"event", event,
			"grant_id", grantID,
			"agent", scope.AgentID,
			"tool", scope.Tool,
			"cluster", scope.Cluster,
			"resource", scope.Resource)
		if s.auditLogger != nil {
			s.auditLogger.Log(audit.Entry{
				AgentID:        actor,
				IdentityMethod: scope.IdentityMethod,
				Tool:           "kafka_approval." + event,
				Parameters: sanitizeAuditParams("kafka_approval", map[string]any{
					"grant_id":    grantID,
					"target_tool": scope.Tool,
					"resource":    scope.Resource,
					"detail":      detail,
				}),
				Cluster: scope.Cluster,
				Result:  audit.Result{Status: event},
			})
		}
	}
}

// ApprovalManager exposes the manager for the lifecycle tool and tests.
func (s *Server) ApprovalManager() *approval.Manager { return s.approvalManager }

// ApprovalEnforced reports whether write-tool approval enforcement is active.
func (s *Server) ApprovalEnforced() bool { return s.approvalEnforced }
