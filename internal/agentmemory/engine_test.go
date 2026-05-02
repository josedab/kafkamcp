package agentmemory

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestStoreAndRecall(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

// --- ListEntries tests ---

func TestListEntriesAll(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Store("agent-1", "k1", "v1", "pref", nil, 0, nil)
	e.Store("agent-1", "k2", "v2", "schema", nil, 0, nil)
	e.Store("agent-1", "k3", "v3", "pref", nil, 0, nil)

	entries := e.ListEntries("agent-1", "")
	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}
}

func TestListEntriesByCategory(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Store("agent-1", "k1", "v1", "pref", nil, 0, nil)
	e.Store("agent-1", "k2", "v2", "schema", nil, 0, nil)
	e.Store("agent-1", "k3", "v3", "pref", nil, 0, nil)

	entries := e.ListEntries("agent-1", "pref")
	if len(entries) != 2 {
		t.Errorf("expected 2 pref entries, got %d", len(entries))
	}
}

func TestListEntriesCategoryNoMatch(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Store("agent-1", "k1", "v1", "pref", nil, 0, nil)

	entries := e.ListEntries("agent-1", "nonexistent")
	if len(entries) != 0 {
		t.Errorf("expected 0 entries for nonexistent category, got %d", len(entries))
	}
}

func TestListEntriesNonexistentAgent(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	entries := e.ListEntries("nonexistent", "")
	if entries != nil {
		t.Errorf("expected nil for nonexistent agent, got %v", entries)
	}
}

func TestListEntriesFiltersExpired(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Store("agent-1", "k1", "v1", "pref", nil, 0, nil)

	// Store an entry with an already-expired TTL by manipulating ExpiresAt
	e.mu.Lock()
	expired := time.Now().Add(-time.Hour)
	e.entries["agent-1"]["k1"].ExpiresAt = &expired
	e.mu.Unlock()

	entries := e.ListEntries("agent-1", "")
	if len(entries) != 0 {
		t.Errorf("expected 0 entries (expired filtered), got %d", len(entries))
	}
}

// --- CleanExpired tests ---

func TestCleanExpiredNone(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Store("agent-1", "k1", "v1", "", nil, 0, nil)
	e.Store("agent-1", "k2", "v2", "", nil, 0, nil)

	cleaned := e.CleanExpired()
	if cleaned != 0 {
		t.Errorf("expected 0 cleaned, got %d", cleaned)
	}
}

func TestCleanExpiredMixed(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Store("agent-1", "valid", "v1", "", nil, 0, nil)
	e.Store("agent-1", "expired1", "v2", "", nil, 1, nil)
	e.Store("agent-2", "expired2", "v3", "", nil, 1, nil)

	// Manually expire entries
	e.mu.Lock()
	past := time.Now().Add(-time.Hour)
	e.entries["agent-1"]["expired1"].ExpiresAt = &past
	e.entries["agent-2"]["expired2"].ExpiresAt = &past
	e.mu.Unlock()

	cleaned := e.CleanExpired()
	if cleaned != 2 {
		t.Errorf("expected 2 cleaned, got %d", cleaned)
	}

	// Valid entry should still exist
	_, err := e.Recall("agent-1", "valid")
	if err != nil {
		t.Errorf("valid entry should still exist: %v", err)
	}
}

func TestCleanExpiredTTLBoundary(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Store("agent-1", "k1", "v1", "", nil, 3600, nil)

	// Should not be cleaned - TTL hasn't expired
	cleaned := e.CleanExpired()
	if cleaned != 0 {
		t.Errorf("expected 0 cleaned for non-expired entry, got %d", cleaned)
	}

	// Now manually set to just-expired
	e.mu.Lock()
	justExpired := time.Now().Add(-time.Millisecond)
	e.entries["agent-1"]["k1"].ExpiresAt = &justExpired
	e.mu.Unlock()

	cleaned = e.CleanExpired()
	if cleaned != 1 {
		t.Errorf("expected 1 cleaned for boundary-expired entry, got %d", cleaned)
	}
}

func TestCleanExpiredEmptyStore(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	cleaned := e.CleanExpired()
	if cleaned != 0 {
		t.Errorf("expected 0 cleaned from empty store, got %d", cleaned)
	}
}

func TestStoreUpdateExisting(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.Store("agent-1", "k1", "v1", "pref", nil, 0, nil)
	e.Store("agent-1", "k1", "v2", "schema", nil, 0, nil)

	recalled, _ := e.Recall("agent-1", "k1")
	if recalled.Value != "v2" {
		t.Errorf("expected updated value v2, got %v", recalled.Value)
	}
	if recalled.Category != "schema" {
		t.Errorf("expected updated category schema, got %s", recalled.Category)
	}
}
