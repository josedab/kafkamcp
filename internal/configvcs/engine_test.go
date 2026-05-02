package configvcs

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestRecordAndHistory(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	change, err := e.RecordChange("topic", "orders", "create", "agent-1", "Initial config",
		nil, map[string]any{"partitions": 6, "retention.ms": "604800000"}, nil)
	if err != nil {
		t.Fatalf("RecordChange failed: %v", err)
	}
	if change.Version != 1 {
		t.Errorf("expected version 1, got %d", change.Version)
	}

	// Make second change
	e.RecordChange("topic", "orders", "update", "agent-1", "Increase partitions",
		map[string]any{"partitions": 6}, map[string]any{"partitions": 12}, nil)

	history := e.History("topic", "orders", 10)
	if len(history) != 2 {
		t.Errorf("expected 2 history entries, got %d", len(history))
	}
}

func TestCompare(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.RecordChange("topic", "orders", "create", "a1", "",
		nil, map[string]any{"partitions": 6, "retention": "7d"}, nil)
	e.RecordChange("topic", "orders", "update", "a1", "",
		map[string]any{"partitions": 6, "retention": "7d"},
		map[string]any{"partitions": 12, "retention": "30d"}, nil)

	result, err := e.Compare("topic", "orders", 1, 2)
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}
	if len(result.Differences) != 2 {
		t.Errorf("expected 2 differences, got %d", len(result.Differences))
	}
}

func TestRollback(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.RecordChange("topic", "orders", "create", "a1", "",
		nil, map[string]any{"partitions": 6}, nil)
	e.RecordChange("topic", "orders", "update", "a1", "",
		map[string]any{"partitions": 6}, map[string]any{"partitions": 12}, nil)

	result, err := e.Rollback("topic", "orders", 1, "a1", "revert partition change")
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if result.NewVersion != 3 {
		t.Errorf("expected version 3 after rollback, got %d", result.NewVersion)
	}
}

func TestAuditReport(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.RecordChange("topic", "orders", "create", "agent-1", "v1", nil, map[string]any{"p": 6}, nil)
	e.RecordChange("topic", "orders", "update", "agent-2", "v2", map[string]any{"p": 6}, map[string]any{"p": 12}, nil)

	report, err := e.AuditReport("topic", "orders", time.Time{})
	if err != nil {
		t.Fatalf("AuditReport failed: %v", err)
	}
	if report.TotalChanges != 2 {
		t.Errorf("expected 2 changes, got %d", report.TotalChanges)
	}
	if len(report.Authors) != 2 {
		t.Errorf("expected 2 authors, got %d", len(report.Authors))
	}
}
