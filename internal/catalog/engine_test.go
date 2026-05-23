package catalog

import (
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestIndexAndSearch(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	_, err := e.Index("orders", "topic", "Customer orders stream", "team-commerce", []string{"commerce", "orders"}, nil, nil)
	if err != nil {
		t.Fatalf("Index failed: %v", err)
	}

	_, _ = e.Index("users", "topic", "User profiles", "team-identity", []string{"identity", "users"}, nil, nil)

	results := e.Search(SearchOptions{Query: "orders"})
	if len(results) == 0 {
		t.Error("expected search results for 'orders'")
	}
}

func TestSearchByType(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	_, _ = e.Index("orders", "topic", "", "", nil, nil, nil)
	_, _ = e.Index("order-schema", "schema", "", "", nil, nil, nil)

	results := e.Search(SearchOptions{Type: "topic"})
	if len(results) != 1 {
		t.Errorf("expected 1 topic result, got %d", len(results))
	}
}

func TestTagging(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	entry, _ := e.Index("orders", "topic", "", "", nil, nil, nil)
	updated, err := e.Tag(entry.ID, []string{"pii", "production"})
	if err != nil {
		t.Fatalf("Tag failed: %v", err)
	}
	if len(updated.Tags) != 2 {
		t.Errorf("expected 2 tags, got %d", len(updated.Tags))
	}
}

func TestStats(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	_, _ = e.Index("topic1", "topic", "desc", "owner1", []string{"tag1"}, nil, nil)
	_, _ = e.Index("topic2", "topic", "", "", nil, nil, nil)

	stats := e.Stats()
	if stats.TotalEntries != 2 {
		t.Errorf("expected 2 entries, got %d", stats.TotalEntries)
	}
	if stats.UntaggedCount != 1 {
		t.Errorf("expected 1 untagged, got %d", stats.UntaggedCount)
	}
}

func TestQualityScore(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	// Full metadata entry should have high quality
	entry, _ := e.Index("orders", "topic", "Full description", "team-a", []string{"prod"}, map[string]any{"partitions": 12}, map[string]string{"env": "prod"})
	if entry.QualityScore < 50 {
		t.Errorf("expected quality > 50 for well-documented entry, got %.0f", entry.QualityScore)
	}

	// Bare minimum entry should have low quality
	bare, _ := e.Index("bare-topic", "topic", "", "", nil, nil, nil)
	if bare.QualityScore > 20 {
		t.Errorf("expected quality < 20 for bare entry, got %.0f", bare.QualityScore)
	}
}
