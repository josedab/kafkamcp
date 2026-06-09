package server

import (
	"context"
	"testing"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/engine/backend"
	"github.com/josedab/kafkamcp/internal/idempotency"
)

func newIdempotentTestServer(t *testing.T) *Server {
	t.Helper()
	s := newTestServer(t)
	s.idempotencyManager = idempotency.NewManager(backend.NewInMemory(), time.Hour)
	return s
}

func TestBeginIdempotentMutation_NoKeyPassesThrough(t *testing.T) {
	t.Parallel()
	s := newIdempotentTestServer(t)
	receipt, replay, err := s.beginIdempotentMutation(context.Background(), "kafka_create_topic", "agent-1", map[string]any{"name": "orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if replay != nil {
		t.Error("expected no replay when no idempotency_key supplied")
	}
	if receipt.KeyHash != "" {
		t.Error("expected empty receipt when no idempotency_key supplied")
	}
}

func TestBeginIdempotentMutation_ShortKeyRejected(t *testing.T) {
	t.Parallel()
	s := newIdempotentTestServer(t)
	_, _, err := s.beginIdempotentMutation(context.Background(), "kafka_create_topic", "agent-1", map[string]any{
		"name":            "orders",
		idempotencyKeyArg: "short",
	})
	if err == nil {
		t.Fatal("expected error for too-short idempotency_key")
	}
}

func TestBeginIdempotentMutation_NonIdempotentToolIgnored(t *testing.T) {
	t.Parallel()
	s := newIdempotentTestServer(t)
	// kafka_produce is a write tool but NOT in the idempotent set.
	receipt, replay, err := s.beginIdempotentMutation(context.Background(), "kafka_produce", "agent-1", map[string]any{
		"topic":           "orders",
		idempotencyKeyArg: "key-abcdef12",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if replay != nil || receipt.KeyHash != "" {
		t.Error("non-idempotent tool must not engage the idempotency path")
	}
}

func TestIdempotentMutation_BeginCompleteReplay(t *testing.T) {
	t.Parallel()
	s := newIdempotentTestServer(t)
	ctx := context.Background()
	args := map[string]any{"name": "orders", idempotencyKeyArg: "key-abcdef12"}

	receipt, replay, err := s.beginIdempotentMutation(ctx, "kafka_create_topic", "agent-1", args)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if replay != nil {
		t.Fatal("first begin should not replay")
	}

	original := mcp.NewToolResultText("created orders")
	if err := s.completeIdempotentMutation(ctx, receipt, original); err != nil {
		t.Fatalf("complete: %v", err)
	}

	_, replay2, err := s.beginIdempotentMutation(ctx, "kafka_create_topic", "agent-1", args)
	if err != nil {
		t.Fatalf("second begin: %v", err)
	}
	if replay2 == nil {
		t.Fatal("second begin with same key should return the cached result")
	}
	if got := resultText(t, replay2); got != "created orders" {
		t.Errorf("expected cached text 'created orders', got %q", got)
	}
}

func TestIdempotentMutation_AbortAllowsRetry(t *testing.T) {
	t.Parallel()
	s := newIdempotentTestServer(t)
	ctx := context.Background()
	args := map[string]any{"name": "orders", idempotencyKeyArg: "key-abcdef12"}

	receipt, _, err := s.beginIdempotentMutation(ctx, "kafka_create_topic", "agent-1", args)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	s.abortIdempotentMutation(ctx, receipt)

	_, replay, err := s.beginIdempotentMutation(ctx, "kafka_create_topic", "agent-1", args)
	if err != nil {
		t.Fatalf("begin after abort: %v", err)
	}
	if replay != nil {
		t.Error("begin after abort should not replay")
	}
}
