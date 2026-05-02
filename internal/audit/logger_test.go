package audit

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

func TestAuditLoggerRingBuffer(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	al, err := NewLogger(context.Background(), true, 5, "", logger)
	if err != nil {
		t.Fatal(err)
	}

	// Log 3 entries
	for i := 0; i < 3; i++ {
		al.Log(Entry{
			AgentID: "test",
			Tool:    "kafka_list_topics",
			Result:  Result{Status: "success"},
		})
	}

	entries := al.RecentEntries(10)
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	// Log 10 more entries (overflow the ring buffer)
	for i := 0; i < 10; i++ {
		al.Log(Entry{
			AgentID: "test",
			Tool:    "kafka_consume",
			Result:  Result{Status: "success"},
		})
	}

	entries = al.RecentEntries(10)
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries (ring buffer size), got %d", len(entries))
	}
}

func TestAuditLoggerDisabled(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	al, err := NewLogger(context.Background(), false, 10, "", logger)
	if err != nil {
		t.Fatal(err)
	}

	al.Log(Entry{
		AgentID: "test",
		Tool:    "kafka_list_topics",
		Result:  Result{Status: "success"},
	})

	entries := al.RecentEntries(10)
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries when disabled, got %d", len(entries))
	}
}
