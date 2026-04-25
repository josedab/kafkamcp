package semantic

import (
	"fmt"
	"log/slog"
	"strings"
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

func TestDeleteByTopicNonexistent(t *testing.T) {
	e := newTestEngine()
	deleted := e.DeleteByTopic("nonexistent")
	if deleted != 0 {
		t.Errorf("expected 0 deleted for nonexistent topic, got %d", deleted)
	}
}

// --- evictOldest tests ---

func TestEvictOldest(t *testing.T) {
	e := newTestEngine()
	e.maxDocs = 3

	e.IndexMessage("t1", 0, 1, "", "first doc", nil)
	e.IndexMessage("t1", 0, 2, "", "second doc", nil)
	e.IndexMessage("t1", 0, 3, "", "third doc", nil)
	// This should trigger eviction of the oldest
	e.IndexMessage("t1", 0, 4, "", "fourth doc", nil)

	stats := e.GetStats()
	if stats.TotalDocuments != 3 {
		t.Errorf("expected 3 docs after eviction, got %d", stats.TotalDocuments)
	}

	// Verify the first doc is gone by searching
	results := e.Search("first doc", "", 10)
	for _, r := range results {
		if r.Document.Offset == 1 {
			t.Error("expected first doc (offset 1) to be evicted")
		}
	}
}

func TestEvictOldestIndexIntegrity(t *testing.T) {
	e := newTestEngine()
	e.maxDocs = 2

	e.IndexMessage("t1", 0, 1, "", "hello world", nil)
	e.IndexMessage("t2", 0, 1, "", "goodbye world", nil)
	// Evicts oldest
	e.IndexMessage("t3", 0, 1, "", "new content", nil)

	stats := e.GetStats()
	if stats.TotalDocuments != 2 {
		t.Errorf("expected 2 docs, got %d", stats.TotalDocuments)
	}

	// Topic index should be consistent
	e.mu.RLock()
	totalInTopics := 0
	for _, ids := range e.topics {
		totalInTopics += len(ids)
	}
	e.mu.RUnlock()
	if totalInTopics != stats.TotalDocuments {
		t.Errorf("topic index mismatch: %d in topics vs %d total", totalInTopics, stats.TotalDocuments)
	}
}

// --- generateHighlight tests ---

func TestGenerateHighlightShortContent(t *testing.T) {
	content := "short text"
	result := generateHighlight(content, []string{"short"}, 200)
	if result != content {
		t.Errorf("expected full content for short text, got %q", result)
	}
}

func TestGenerateHighlightLongContent(t *testing.T) {
	content := strings.Repeat("lorem ipsum dolor sit amet ", 50)
	result := generateHighlight(content, []string{"dolor"}, 200)
	if len(result) > 210 { // 200 + prefix/suffix "..."
		t.Errorf("expected highlight <= ~206 chars, got %d", len(result))
	}
}

func TestGenerateHighlightOverlappingTerms(t *testing.T) {
	content := strings.Repeat("the quick brown fox jumps over the lazy dog ", 20)
	result := generateHighlight(content, []string{"quick", "fox", "lazy"}, 100)
	if result == "" {
		t.Error("expected non-empty highlight")
	}
}

func TestGenerateHighlightNoMatchTerms(t *testing.T) {
	content := strings.Repeat("hello world ", 50)
	result := generateHighlight(content, []string{"xyz123"}, 100)
	if result == "" {
		t.Error("expected non-empty highlight even with no matches")
	}
}

// --- BuildRAGContext edge cases ---

func TestBuildRAGContextZeroMaxTokens(t *testing.T) {
	e := newTestEngine()
	e.IndexMessage("logs", 0, 1, "", "error in authentication service", nil)

	ctx := e.BuildRAGContext("authentication", "", 0)
	if ctx.TokenCount == 0 && len(ctx.Results) > 0 {
		t.Error("expected some tokens with default maxTokens")
	}
}

func TestBuildRAGContextNoResults(t *testing.T) {
	e := newTestEngine()
	ctx := e.BuildRAGContext("nonexistent query", "", 1000)
	if ctx.Context != "" {
		t.Error("expected empty context for no results")
	}
}

func TestBuildRAGContextTokenLimitTruncation(t *testing.T) {
	e := newTestEngine()
	for i := 0; i < 10; i++ {
		e.IndexMessage("logs", 0, int64(i), "", fmt.Sprintf("error log message number %d with some content about authentication and security issues", i), nil)
	}

	ctx := e.BuildRAGContext("authentication error", "", 20)
	if ctx.TokenCount > 30 {
		t.Errorf("expected token count near limit, got %d", ctx.TokenCount)
	}
}

func TestBuildRAGContextTopicFilter(t *testing.T) {
	e := newTestEngine()
	e.IndexMessage("logs", 0, 1, "", "authentication error", nil)
	e.IndexMessage("metrics", 0, 1, "", "authentication latency", nil)

	ctx := e.BuildRAGContext("authentication", "logs", 1000)
	for _, topic := range ctx.Topics {
		if topic != "logs" {
			t.Errorf("expected only logs topic, got %s", topic)
		}
	}
}

// --- Search edge cases ---

func TestSearchLimitCap(t *testing.T) {
	e := newTestEngine()
	for i := 0; i < 5; i++ {
		e.IndexMessage("t", 0, int64(i), "", fmt.Sprintf("test message %d", i), nil)
	}
	results := e.Search("test message", "", 2)
	if len(results) > 2 {
		t.Errorf("expected at most 2 results, got %d", len(results))
	}
}

func TestSearchDefaultLimit(t *testing.T) {
	e := newTestEngine()
	e.IndexMessage("t", 0, 1, "", "some content", nil)
	results := e.Search("content", "", 0)
	// Should use default limit of 10, not crash
	if len(results) > 10 {
		t.Errorf("expected <= 10 results with default limit, got %d", len(results))
	}
}
