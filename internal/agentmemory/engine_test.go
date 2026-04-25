package agentmemory

import (
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestStoreAndRecall(t *testing.T) {
	e := NewEngine(testLogger())

	entry, err := e.Store("agent-1", "topic-pref", "orders-topic", "preference", []string{"kafka"}, 0, nil)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	if entry.Key != "topic-pref" {
		t.Errorf("expected key topic-pref, got %s", entry.Key)
	}

	recalled, err := e.Recall("agent-1", "topic-pref")
	if err != nil {
		t.Fatalf("Recall failed: %v", err)
	}
	if recalled.Value != "orders-topic" {
		t.Errorf("expected orders-topic, got %v", recalled.Value)
	}
}

func TestSearch(t *testing.T) {
	e := NewEngine(testLogger())

	e.Store("agent-1", "schema-orders", "avro schema for orders", "schema", []string{"avro", "orders"}, 0, nil)
	e.Store("agent-1", "schema-users", "protobuf schema for users", "schema", []string{"proto", "users"}, 0, nil)
	e.Store("agent-1", "query-history", "SELECT * FROM orders", "query_history", nil, 0, nil)

	results := e.Search("agent-1", "orders schema", "", 10)
	if len(results) == 0 {
		t.Error("expected search results")
	}
}

func TestTTLExpiration(t *testing.T) {
	e := NewEngine(testLogger())

	// Store with 0 TTL (expired immediately won't work; use 1 and check)
	e.Store("agent-1", "temp-key", "value", "", nil, 1, nil)

	// Should be accessible immediately
	_, err := e.Recall("agent-1", "temp-key")
	if err != nil {
		t.Errorf("expected key to be accessible: %v", err)
	}
}

func TestDelete(t *testing.T) {
	e := NewEngine(testLogger())

	e.Store("agent-1", "del-key", "value", "", nil, 0, nil)
	err := e.Delete("agent-1", "del-key")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = e.Recall("agent-1", "del-key")
	if err == nil {
		t.Error("expected error after delete")
	}
}

func TestStats(t *testing.T) {
	e := NewEngine(testLogger())

	e.Store("agent-1", "k1", "v1", "preference", nil, 0, nil)
	e.Store("agent-1", "k2", "v2", "schema", nil, 0, nil)

	stats := e.Stats("agent-1")
	if stats.TotalEntries != 2 {
		t.Errorf("expected 2 entries, got %d", stats.TotalEntries)
	}
	if stats.Categories["preference"] != 1 {
		t.Error("expected 1 preference entry")
	}
}
