package server

import (
	"context"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/approval"
	"github.com/josedab/kafkamcp/internal/auth"
	"github.com/josedab/kafkamcp/internal/engine/backend"
)

// enableApproval installs an enforcing approval manager on a test server.
func enableApproval(t *testing.T, s *Server) {
	t.Helper()
	var err error
	s.approvalManager, err = approval.NewManager(context.Background(), backend.NewInMemory(), approval.DefaultConfig())
	if err != nil {
		t.Fatalf("new approval manager: %v", err)
	}
	s.approvalEnforced = true
}

func writeToolHandler(called *bool) toolHandler {
	return func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		*called = true
		return mcp.NewToolResultText("ok"), nil
	}
}

func TestEnforceApproval_ReadToolUnaffected(t *testing.T) {
	s := newTestServer(t)
	enableApproval(t, s)
	called := false
	h := s.withToolAuth(toolAuthConfig{
		tool: "kafka_list_topics", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, writeToolHandler(&called))
	if _, err := h(context.Background(), makeToolRequest(map[string]any{"cluster": "prod"})); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("read-only tool must not require approval")
	}
}

func TestEnforceApproval_WriteToolDeniedWithoutToken(t *testing.T) {
	s := newTestServer(t)
	enableApproval(t, s)
	called := false
	h := s.withToolAuth(toolAuthConfig{
		tool: "kafka_delete_topic", resourceType: auth.ResourceTopic, action: auth.ActionAdmin,
	}, writeToolHandler(&called))
	result, err := h(context.Background(), makeToolRequest(map[string]any{"topic": "orders"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if called {
		t.Fatal("write tool must not execute without an approval grant")
	}
}

func TestEnforceApproval_WriteToolAllowedWithValidGrant(t *testing.T) {
	s := newTestServer(t)
	enableApproval(t, s)
	ctx := context.Background()
	agentID := "anonymous" // the resolved identity in default test config

	args := map[string]any{"topic": "orders", "cluster": "prod"}
	scope, err := s.approvalScopeFor(ctx, "kafka_delete_topic", agentID, args)
	if err != nil {
		t.Fatalf("scope: %v", err)
	}
	g, err := s.approvalManager.Request(ctx, approval.RequestSpec{Scope: scope, Requester: agentID})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	_, token, err := s.approvalManager.Approve(ctx, g.ID, "operator", 0)
	if err != nil {
		t.Fatalf("approve: %v", err)
	}

	called := false
	h := s.withToolAuth(toolAuthConfig{
		tool: "kafka_delete_topic", resourceType: auth.ResourceTopic, action: auth.ActionAdmin,
	}, writeToolHandler(&called))

	withToken := map[string]any{"topic": "orders", "cluster": "prod", "approval_token": token}
	result, err := h(ctx, makeToolRequest(withToken))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil && result.IsError {
		t.Fatalf("expected success with valid grant, got error result")
	}
	if !called {
		t.Fatal("write tool should execute with a valid approval grant")
	}

	// Single-use: a replay must be denied.
	called = false
	replay, _ := h(ctx, makeToolRequest(withToken))
	assertIsError(t, replay)
	if called {
		t.Fatal("single-use grant must not permit a replay")
	}
}

func TestEnforceApproval_TokenExcludedFromDigest(t *testing.T) {
	// The approval_token must not affect the args digest, otherwise the token
	// presented at use time would never match the granted digest.
	base := map[string]any{"topic": "orders", "cluster": "prod"}
	withTok := map[string]any{"topic": "orders", "cluster": "prod", "approval_token": "abc"}
	s := newTestServer(t)
	a, err := s.approvalScopeFor(context.Background(), "kafka_delete_topic", "agent", base)
	if err != nil {
		t.Fatalf("base scope: %v", err)
	}
	b, err := s.approvalScopeFor(context.Background(), "kafka_delete_topic", "agent", withTok)
	if err != nil {
		t.Fatalf("token scope: %v", err)
	}
	if a.ArgsDigest != b.ArgsDigest {
		t.Fatal("approval_token must be excluded from the args digest")
	}
}

func TestEnforceApproval_IdempotencyKeyExcludedFromDigest(t *testing.T) {
	base := map[string]any{"topic": "orders", "cluster": "prod"}
	withKey := map[string]any{"topic": "orders", "cluster": "prod", "idempotency_key": "create-orders-001"}
	s := newTestServer(t)
	a, err := s.approvalScopeFor(context.Background(), "kafka_create_topic", "agent", base)
	if err != nil {
		t.Fatalf("base scope: %v", err)
	}
	b, err := s.approvalScopeFor(context.Background(), "kafka_create_topic", "agent", withKey)
	if err != nil {
		t.Fatalf("idempotent scope: %v", err)
	}
	if a.ArgsDigest != b.ArgsDigest {
		t.Fatal("idempotency_key must be excluded from the approval argument digest")
	}
}

func TestEnforceApproval_DisabledShortCircuits(t *testing.T) {
	s := newTestServer(t)
	// approvalEnforced defaults to false and approvalManager is nil.
	called := false
	h := s.withToolAuth(toolAuthConfig{
		tool: "kafka_delete_topic", resourceType: auth.ResourceTopic, action: auth.ActionAdmin,
	}, writeToolHandler(&called))
	if _, err := h(context.Background(), makeToolRequest(map[string]any{"topic": "orders"})); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("with enforcement disabled, write tools should execute")
	}
}
