package audit

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/engine/backend"
)

// TestPersistence_RoundTrip verifies that entries logged through a
// backend-enabled logger are restored on a subsequent open. This is
// the FU2 closing test for audit persistence.
func TestPersistence_RoundTrip(t *testing.T) {
	t.Parallel()
	be := backend.NewInMemory()
	defer func() { _ = be.Close() }()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	l1, err := NewLoggerWithBackend(ctx, true, 10, "", logger, be)
	if err != nil {
		t.Fatalf("NewLoggerWithBackend: %v", err)
	}
	l1.Log(Entry{AgentID: "alice", Tool: "kafka_list_topics", Result: Result{Status: "success"}})
	l1.Log(Entry{AgentID: "bob", Tool: "kafka_produce", Result: Result{Status: "error", Error: "timeout"}})
	if err := l1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen with same backend; entries must come back.
	l2, err := NewLoggerWithBackend(ctx, true, 10, "", logger, be)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got := l2.RecentEntries(10)
	if len(got) != 2 {
		t.Fatalf("RecentEntries=%d want 2 (%+v)", len(got), got)
	}
	// Newest-first ordering.
	if got[0].AgentID != "bob" || got[1].AgentID != "alice" {
		t.Errorf("order wrong: %+v", got)
	}
	if got[1].Tool != "kafka_list_topics" {
		t.Errorf("entry data wrong: %+v", got[1])
	}
}

// TestPersistence_BoundedRetention verifies that bounded retention is
// preserved even when many more entries are written than maxEntries.
// Older entries must be trimmed from the backend so disk usage stays
// bounded.
func TestPersistence_BoundedRetention(t *testing.T) {
	t.Parallel()
	be := backend.NewInMemory()
	defer func() { _ = be.Close() }()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := context.Background()

	const maxEntries = 5
	l, err := NewLoggerWithBackend(ctx, true, maxEntries, "", logger, be)
	if err != nil {
		t.Fatalf("NewLoggerWithBackend: %v", err)
	}
	for i := 0; i < 20; i++ {
		l.Log(Entry{AgentID: "x", Tool: "t", Result: Result{Status: "success"}})
	}

	// Confirm bounded retention on the backend itself.
	all, err := be.List(ctx, "audit.entries", "")
	if err != nil {
		t.Fatalf("backend.List: %v", err)
	}
	if len(all) > maxEntries+1 {
		// +1 tolerance: the trim runs after the write so transiently one
		// extra entry exists. The point is bounded-ness, not exact match.
		t.Errorf("retained %d entries; expected <= %d", len(all), maxEntries+1)
	}
}

// TestPersistence_NilBackend keeps the legacy behavior intact.
func TestPersistence_NilBackend(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	l, err := NewLoggerWithBackend(context.Background(), true, 4, "", logger, nil)
	if err != nil {
		t.Fatalf("NewLoggerWithBackend: %v", err)
	}
	l.Log(Entry{AgentID: "a", Tool: "t", Result: Result{Status: "ok"}})
	if got := l.RecentEntries(1); len(got) != 1 || got[0].AgentID != "a" {
		t.Errorf("entries=%+v", got)
	}
}
