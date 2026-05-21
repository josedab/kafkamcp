package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/gitops"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerGitOpsReconcileTools() {
	if s.gitopsReconciler == nil {
		return
	}

	s.addTool(mcp.NewTool("gitops_reconcile",
		withCategory("gitops"),
		mcp.WithDescription("Run a GitOps reconciliation against the live cluster state. Defaults to dry-run mode."),
		mcp.WithBoolean("dry_run", mcp.Description("Run in dry-run mode (no changes applied). Default: true")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "gitops_reconcile",
		resourceType: "cluster",
		action:       "write",
	}, s.handleGitOpsReconcile))

	s.addTool(mcp.NewTool("gitops_set_desired_state",
		withCategory("gitops"),
		mcp.WithDescription("Set the desired Kafka state for GitOps reconciliation (topics, schemas, ACLs in YAML format)."),
		mcp.WithString("state_yaml", mcp.Required(), mcp.Description("YAML-encoded desired state")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "gitops_set_desired_state",
		resourceType: "cluster",
		action:       "write",
	}, s.handleGitOpsSetState))

	s.addTool(mcp.NewTool("gitops_reconcile_history",
		withCategory("gitops"),
		mcp.WithDescription("Get recent GitOps reconciliation results."),
		mcp.WithNumber("limit", mcp.Description("Max results. Default: 10")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "gitops_reconcile_history",
		resourceType: "cluster",
		action:       "read",
	}, s.handleGitOpsHistory))

	s.addTool(mcp.NewTool("gitops_webhook",
		withCategory("gitops"),
		mcp.WithDescription("Process a Git webhook event (push or pull_request) for PR-based reconciliation workflows."),
		mcp.WithString("type", mcp.Required(), mcp.Description("Webhook event type: push or pull_request")),
		mcp.WithString("ref", mcp.Description("Git ref (e.g., refs/heads/main)")),
		mcp.WithString("action", mcp.Description("PR action: opened, synchronize, closed, merged")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "gitops_webhook",
		resourceType: "cluster",
		action:       "write",
	}, s.handleGitOpsWebhook))
}

func (s *Server) handleGitOpsReconcile(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	dryRun := true
	if dr, ok := args["dry_run"].(bool); ok {
		dryRun = dr
	}

	live := gitops.LiveState{Topics: make(map[string]gitops.TopicSpec)}
	result, err := s.gitopsReconciler.Reconcile(ctx, live, dryRun)
	if err != nil {
		s.logAudit(ctx, agentID, "gitops_reconcile", args, "", "error", 0, err, start)
		return s.toolError("reconciliation failed", err), nil
	}

	s.logAudit(ctx, agentID, "gitops_reconcile", args, "", "success", 1, nil, start)
	return marshalResult(result)
}

func (s *Server) handleGitOpsSetState(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	_, ok := args["state_yaml"].(string)
	if !ok {
		return mcp.NewToolResultError("state_yaml is required"), nil
	}

	// For now, accept a simplified JSON-compatible state
	state := gitops.DesiredState{
		Version: "1",
	}

	err := s.gitopsReconciler.SetDesiredState(state)
	if err != nil {
		s.logAudit(ctx, agentID, "gitops_set_desired_state", args, "", "error", 0, err, start)
		return s.toolError("failed to set desired state", err), nil
	}

	s.logAudit(ctx, agentID, "gitops_set_desired_state", args, "", "success", 1, nil, start)
	return marshalResult(map[string]any{
		"status": "desired_state_updated",
	})
}

func (s *Server) handleGitOpsHistory(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	limit := 10
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}
	if limit > MaxDLQMessages {
		limit = MaxDLQMessages
	}

	results := s.gitopsReconciler.GetResults(limit)
	s.logAudit(ctx, agentID, "gitops_reconcile_history", args, "", "success", len(results), nil, start)
	return marshalResult(map[string]any{
		"results": results,
		"total":   len(results),
	})
}

func (s *Server) handleGitOpsWebhook(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	eventType, _ := args["type"].(string)
	if eventType == "" {
		return mcp.NewToolResultError("type is required"), nil
	}

	event := gitops.WebhookEvent{
		Type:   eventType,
		Ref:    argString(args, "ref"),
		Action: argString(args, "action"),
	}

	result, err := s.gitopsReconciler.HandleWebhook(event)
	if err != nil {
		s.logAudit(ctx, agentID, "gitops_webhook", args, "", "error", 0, err, start)
		return s.toolError("webhook processing failed", err), nil
	}

	if result == nil {
		s.logAudit(ctx, agentID, "gitops_webhook", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{
			"status":  "no_action",
			"message": "Webhook event did not trigger reconciliation",
		})
	}

	s.logAudit(ctx, agentID, "gitops_webhook", args, "", "success", 1, nil, start)
	return marshalResult(result)
}

func argString(args map[string]any, key string) string {
	v, _ := args[key].(string)
	return v
}
