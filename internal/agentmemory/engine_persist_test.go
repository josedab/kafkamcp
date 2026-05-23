package agentmemory

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/engine/backend"
)

// TestPersistence_RoundTrip writes a few entries, closes the engine,
// reopens with the same backend and verifies the entries are visible.
// This is the FU2 closing test for agentmemory persistence.
func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()
	be := backend.NewInMemory()
	defer func() { _ = be.Close() }()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	e1, err := NewEngineWithBackend(ctx, logger, be)
	if err != nil {
		t.Fatalf("NewEngineWithBackend: %v", err)
	}
	if _, err := e1.Store("alice", "lastTopic", "orders.created", "context", []string{"recent"}, 0, nil); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if _, err := e1.Store("bob", "schemaID", "12345", "schema", nil, 0, nil); err != nil {
		t.Fatalf("Store: %v", err)
	}

	e2, err := NewEngineWithBackend(ctx, logger, be)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := e2.Recall("alice", "lastTopic")
	if err != nil {
		t.Fatalf("Recall alice: %v", err)
	}
	if got.Value != "orders.created" {
		t.Errorf("value: %v", got.Value)
	}
	if _, err := e2.Recall("bob", "schemaID"); err != nil {
		t.Errorf("Recall bob: %v", err)
	}
}

// TestPersistence_TTLEvictionPropagates ensures expired entries are
// dropped from the backend on load.
func TestPersistence_TTLEvictionPropagates(t *testing.T) {
	t.Parallel()
	be := backend.NewInMemory()
	defer func() { _ = be.Close() }()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	e, err := NewEngineWithBackend(ctx, logger, be)
	if err != nil {
		t.Fatalf("NewEngineWithBackend: %v", err)
	}
	// store with a very short TTL via a synthetic ExpiresAt
	past := time.Now().Add(-1 * time.Hour)
	entry, err := e.Store("a", "k", "v", "", nil, 0, nil)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	entry.ExpiresAt = &past
	e.persistLocked(entry)

	// Reopen → expired entry should be GC'd.
	e2, err := NewEngineWithBackend(ctx, logger, be)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err := e2.Recall("a", "k"); err == nil {
		t.Error("expected expired entry to be evicted on load")
	}
	all, err := be.List(ctx, "agentmemory.entries", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("expected backend GC, found %d entries", len(all))
	}
}

func TestPersistence_NilBackend(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	e, err := NewEngineWithBackend(context.Background(), logger, nil)
	if err != nil {
		t.Fatalf("NewEngineWithBackend(nil): %v", err)
	}
	if _, err := e.Store("a", "k", "v", "", nil, 0, nil); err != nil {
		t.Fatalf("Store: %v", err)
	}
	got, err := e.Recall("a", "k")
	if err != nil || got.Value != "v" {
		t.Errorf("recall: %v %v", got, err)
	}
}
