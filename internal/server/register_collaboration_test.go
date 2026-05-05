package server

import (
	"context"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/collaboration"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

func setupCollabServer(t *testing.T) (*Server, context.Context, time.Time) {
	t.Helper()
	s := newTestServer(t)
	s.kafkaManager = &kafka.MockKafkaManager{}
	s.collaborationCoord = collaboration.NewCoordinator(s.logger)
	return s, context.Background(), time.Now()
}

// --- Lock tests ---

func TestHandleLockAcquire(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	args := map[string]any{"resource": "topic:orders", "ttl_seconds": float64(30)}
	result, err := s.handleLockAcquire(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleLockAcquire_EmptyResource(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleLockAcquire(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result")
	}
}

func TestHandleLockRelease(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	// Acquire first
	args := map[string]any{"resource": "topic:orders", "ttl_seconds": float64(30)}
	_, _ = s.handleLockAcquire(ctx, mcp.CallToolRequest{}, "agent-1", args, start)

	// Release
	result, err := s.handleLockRelease(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"resource": "topic:orders"}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleLockRelease_EmptyResource(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleLockRelease(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result")
	}
}

func TestHandleLockRelease_NonExistent(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleLockRelease(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"resource": "topic:none"}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result for releasing non-existent lock")
	}
}

// --- Lock list ---

func TestHandleLockList_Empty(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleLockList(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleLockList_AfterAcquire(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	_, _ = s.handleLockAcquire(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"resource": "topic:orders"}, start)

	result, err := s.handleLockList(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

// --- Clock tests ---

func TestHandleClockGet(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleClockGet(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"resource": "topic:orders"}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleClockGet_EmptyResource(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleClockGet(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result")
	}
}

func TestHandleClockIncrement(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleClockIncrement(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"resource": "topic:orders"}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleClockIncrement_EmptyResource(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleClockIncrement(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result")
	}
}

// --- Message tests ---

func TestHandleMessageSend(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	args := map[string]any{"to": "agent-2", "subject": "hello", "body": "world"}
	result, err := s.handleMessageSend(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleMessageSend_MissingTo(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleMessageSend(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"subject": "hello"}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result")
	}
}

func TestHandleMessageSend_MissingSubject(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleMessageSend(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"to": "agent-2"}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result")
	}
}

// --- Inbox tests ---

func TestHandleMessageInbox_Empty(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleMessageInbox(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleMessageInbox_AfterSend(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	_, _ = s.handleMessageSend(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"to": "agent-2", "subject": "hi"}, start)

	result, err := s.handleMessageInbox(ctx, mcp.CallToolRequest{}, "agent-2", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

// --- Agent register/list ---

func TestHandleAgentRegister(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	args := map[string]any{"status": "active", "capabilities": []any{"read", "write"}}
	result, err := s.handleAgentRegister(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleAgentRegister_EmptyStatus(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleAgentRegister(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result, empty status should default to active")
	}
}

func TestHandleAgentList_Empty(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleAgentList(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleAgentList_AfterRegister(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	_, _ = s.handleAgentRegister(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"status": "active"}, start)

	result, err := s.handleAgentList(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

// --- Topic lease tests ---

func TestHandleTopicLeaseAcquire(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	args := map[string]any{"topic": "orders", "ttl_seconds": float64(60)}
	result, err := s.handleTopicLeaseAcquire(ctx, mcp.CallToolRequest{}, "agent-1", args, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleTopicLeaseAcquire_EmptyTopic(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleTopicLeaseAcquire(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result")
	}
}

func TestHandleTopicLeaseRelease(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	// Acquire first
	_, _ = s.handleTopicLeaseAcquire(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"topic": "orders"}, start)

	result, err := s.handleTopicLeaseRelease(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"topic": "orders"}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleTopicLeaseRelease_EmptyTopic(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleTopicLeaseRelease(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result")
	}
}

func TestHandleTopicLeaseRenew(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	// Acquire first
	_, _ = s.handleTopicLeaseAcquire(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"topic": "orders"}, start)

	result, err := s.handleTopicLeaseRenew(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"topic": "orders", "ttl_seconds": float64(60)}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

func TestHandleTopicLeaseRenew_EmptyTopic(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	result, err := s.handleTopicLeaseRenew(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected error result")
	}
}

func TestHandleTopicLeaseList_AfterAcquire(t *testing.T) {
	s, ctx, start := setupCollabServer(t)

	_, _ = s.handleTopicLeaseAcquire(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{"topic": "orders"}, start)

	result, err := s.handleTopicLeaseList(ctx, mcp.CallToolRequest{}, "agent-1", map[string]any{}, start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsError {
		t.Fatal("unexpected error in result")
	}
}

// --- registerCollaborationTools with nil coordinator ---

func TestRegisterCollaborationTools_NilCoordinator(t *testing.T) {
	s := newTestServer(t)
	// collaborationCoord is nil by default; should not panic
	s.registerCollaborationTools()
}

// --- boolStatus ---

func TestBoolStatus(t *testing.T) {
	if got := boolStatus(true); got != "success" {
		t.Fatalf("expected 'success', got %q", got)
	}
	if got := boolStatus(false); got != "denied" {
		t.Fatalf("expected 'denied', got %q", got)
	}
}
