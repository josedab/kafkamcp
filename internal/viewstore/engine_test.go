package viewstore

import (
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestCreateAndQueryView(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	view, err := e.CreateView("test-view", "orders", "_key", "count", "", 0, 0, nil, nil, "", "desc", "agent-1")
	if err != nil {
		t.Fatalf("CreateView failed: %v", err)
	}

	// Ingest records
	e.IngestRecord(view.ID, "key1", map[string]any{"amount": 100.0}, 1)
	e.IngestRecord(view.ID, "key2", map[string]any{"amount": 200.0}, 2)
	e.IngestRecord(view.ID, "key1", map[string]any{"amount": 50.0}, 3) // aggregates

	// Query
	result, err := e.Query(view.ID, QueryOptions{})
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.TotalCount != 2 {
		t.Errorf("expected 2 records, got %d", result.TotalCount)
	}
}

func TestViewPointLookup(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	view, _ := e.CreateView("lookup-test", "topic1", "_key", "", "", 0, 0, nil, nil, "", "", "agent-1")

	e.IngestRecord(view.ID, "key1", map[string]any{"val": "a"}, 1)
	e.IngestRecord(view.ID, "key2", map[string]any{"val": "b"}, 2)

	result, _ := e.Query(view.ID, QueryOptions{KeyExact: "key1"})
	if result.TotalCount != 1 {
		t.Errorf("expected 1 record for point lookup, got %d", result.TotalCount)
	}
}

func TestViewStats(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	view, _ := e.CreateView("stats-test", "topic1", "_key", "", "", 0, 0, nil, nil, "", "", "agent-1")

	e.IngestRecord(view.ID, "k1", map[string]any{}, 1)
	e.IngestRecord(view.ID, "k2", map[string]any{}, 2)

	stats, err := e.Stats(view.ID)
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}
	if stats.RecordCount != 2 {
		t.Errorf("expected 2 records, got %d", stats.RecordCount)
	}
}

func TestViewDelete(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	view, _ := e.CreateView("del-test", "topic1", "_key", "", "", 0, 0, nil, nil, "", "", "agent-1")

	err := e.DeleteView(view.ID)
	if err != nil {
		t.Fatalf("DeleteView failed: %v", err)
	}

	_, err = e.GetView(view.ID)
	if err == nil {
		t.Error("expected error for deleted view")
	}
}
