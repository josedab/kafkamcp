package audit

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

func TestRecent(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	al, err := NewLogger(true, 10, "", logger)
	if err != nil {
		t.Fatal(err)
	}

	al.Log(Entry{AgentID: "a1", Tool: "t1", Result: Result{Status: "success"}})
	al.Log(Entry{AgentID: "a2", Tool: "t2", Result: Result{Status: "success"}})

	entries := al.Recent(2)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	// Recent should return in reverse chronological order
	if entries[0].AgentID != "a2" {
		t.Errorf("expected most recent entry first, got agent_id=%s", entries[0].AgentID)
	}
}

func TestRecentZero(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	al, _ := NewLogger(true, 10, "", logger)

	entries := al.Recent(0)
	if entries != nil {
		t.Fatalf("expected nil for n=0, got %v", entries)
	}
}

func TestCloseWithFile(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	tmpFile := filepath.Join(t.TempDir(), "audit.log")

	al, err := NewLogger(true, 10, tmpFile, logger)
	if err != nil {
		t.Fatal(err)
	}

	al.Log(Entry{AgentID: "test", Tool: "t1", Result: Result{Status: "success"}})

	err = al.Close()
	if err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}
}

func TestCloseWithoutFile(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	al, _ := NewLogger(true, 10, "", logger)

	err := al.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLogWithTimestamp(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	al, _ := NewLogger(true, 10, "", logger)

	al.Log(Entry{
		Timestamp: "2024-01-01T00:00:00Z",
		AgentID:   "test",
		Tool:      "t1",
		Result:    Result{Status: "success"},
	})

	entries := al.RecentEntries(1)
	if entries[0].Timestamp != "2024-01-01T00:00:00Z" {
		t.Errorf("expected preserved timestamp, got %s", entries[0].Timestamp)
	}
}

func TestLogToFile(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	tmpFile := filepath.Join(t.TempDir(), "audit.log")

	al, err := NewLogger(true, 10, tmpFile, logger)
	if err != nil {
		t.Fatal(err)
	}

	al.Log(Entry{AgentID: "test", Tool: "kafka_produce", Result: Result{Status: "success"}})
	al.Close()

	data, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("expected non-empty audit log file")
	}
}

func TestNewLoggerInvalidFile(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	_, err := NewLogger(true, 10, "/nonexistent/path/audit.log", logger)
	if err == nil {
		t.Fatal("expected error for invalid log file path")
	}
}
