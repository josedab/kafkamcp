package semantic

import (
	"log/slog"
	"testing"
)

func newTestEngine() *Engine {
	return NewEngine(slog.Default())
}

func TestIndexAndSearch(t *testing.T) {
	e := newTestEngine()

	e.IndexMessage("orders", 0, 1, "key1", "customer placed an order for laptop", nil)
	e.IndexMessage("orders", 0, 2, "key2", "customer returned a defective mouse", nil)
	e.IndexMessage("payments", 0, 1, "key3", "payment processed for order #123", nil)

	results := e.Search("order laptop", "", 10)
	if len(results) == 0 {
		t.Fatal("expected search results")
	}

	// First result should be the most relevant
	if results[0].Score <= 0 {
		t.Fatal("expected positive score")
	}
}

func TestSearchWithTopicFilter(t *testing.T) {
	e := newTestEngine()

	e.IndexMessage("orders", 0, 1, "", "order created", nil)
	e.IndexMessage("payments", 0, 1, "", "payment received", nil)

	results := e.Search("order", "orders", 10)
	for _, r := range results {
		if r.Document.Topic != "orders" {
			t.Fatalf("expected topic filter to work, got topic %s", r.Document.Topic)
		}
	}
}

func TestRAGContext(t *testing.T) {
	e := newTestEngine()

	e.IndexMessage("logs", 0, 1, "", "error occurred in authentication service", nil)
	e.IndexMessage("logs", 0, 2, "", "user login successful", nil)
	e.IndexMessage("logs", 0, 3, "", "database connection timeout error", nil)

	ctx := e.BuildRAGContext("authentication errors", "", 1000)
	if ctx.Query != "authentication errors" {
		t.Fatalf("expected query to be preserved, got %s", ctx.Query)
	}
	if ctx.Context == "" {
		t.Fatal("expected non-empty context")
	}
}

func TestStats(t *testing.T) {
	e := newTestEngine()

	e.IndexMessage("topic1", 0, 1, "", "hello world", nil)
	e.IndexMessage("topic2", 0, 1, "", "goodbye world", nil)

	stats := e.GetStats()
	if stats.TotalDocuments != 2 {
		t.Fatalf("expected 2 documents, got %d", stats.TotalDocuments)
	}
	if len(stats.TopicCounts) != 2 {
		t.Fatalf("expected 2 topic counts, got %d", len(stats.TopicCounts))
	}
}

func TestDeleteByTopic(t *testing.T) {
	e := newTestEngine()

	e.IndexMessage("topic1", 0, 1, "", "hello", nil)
	e.IndexMessage("topic1", 0, 2, "", "world", nil)
	e.IndexMessage("topic2", 0, 1, "", "test", nil)

	deleted := e.DeleteByTopic("topic1")
	if deleted != 2 {
		t.Fatalf("expected 2 deleted, got %d", deleted)
	}

	stats := e.GetStats()
	if stats.TotalDocuments != 1 {
		t.Fatalf("expected 1 remaining, got %d", stats.TotalDocuments)
	}
}
