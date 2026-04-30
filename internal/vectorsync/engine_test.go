package vectorsync

import (
	"fmt"
	"log/slog"
	"testing"
)

func newTestEngine() *Engine {
	return NewEngine(slog.Default())
}

func TestCreateBridge(t *testing.T) {
	e := newTestEngine()

	vdb := VectorDBConfig{Type: DBPinecone, Collection: "docs"}
	emb := EmbeddingConfig{Model: ModelSimpleHash, Dimension: 128}

	b, err := e.CreateBridge("test-bridge", "articles", "agent", vdb, emb)
	if err != nil {
		t.Fatal(err)
	}
	if b.Status != "active" {
		t.Errorf("expected active, got %s", b.Status)
	}
	if b.VectorDB.Dimension != 128 {
		t.Errorf("expected dimension 128, got %d", b.VectorDB.Dimension)
	}
}

func TestCreateBridgeValidation(t *testing.T) {
	e := newTestEngine()
	vdb := VectorDBConfig{Type: DBWeaviate, Collection: "docs"}
	emb := EmbeddingConfig{}

	if _, err := e.CreateBridge("", "topic", "a", vdb, emb); err == nil {
		t.Error("expected error for empty name")
	}
	if _, err := e.CreateBridge("b", "", "a", vdb, emb); err == nil {
		t.Error("expected error for empty topic")
	}
	if _, err := e.CreateBridge("b", "t", "a", VectorDBConfig{Type: DBQdrant}, emb); err == nil {
		t.Error("expected error for empty collection")
	}
}

func TestProcessMessage(t *testing.T) {
	e := newTestEngine()
	vdb := VectorDBConfig{Type: DBChroma, Collection: "test"}
	emb := EmbeddingConfig{Model: ModelSimpleHash, Dimension: 64}

	b, _ := e.CreateBridge("test", "articles", "agent", vdb, emb)

	doc, err := e.ProcessMessage(b.ID, "key1", "The quick brown fox jumps over the lazy dog", 0, 1, "articles")
	if err != nil {
		t.Fatal(err)
	}
	if doc.ID == "" {
		t.Error("expected non-empty doc ID")
	}
	if len(doc.Embedding) != 64 {
		t.Errorf("expected 64-dim embedding, got %d", len(doc.Embedding))
	}
	if doc.Content == "" {
		t.Error("expected non-empty content")
	}

	// Check stats
	got, _ := e.GetBridge(b.ID)
	if got.Stats.DocumentsSynced != 1 {
		t.Errorf("expected 1 doc synced, got %d", got.Stats.DocumentsSynced)
	}
}

func TestSearchSimilar(t *testing.T) {
	e := newTestEngine()
	vdb := VectorDBConfig{Type: DBPinecone, Collection: "search"}
	emb := EmbeddingConfig{Model: ModelSimpleHash, Dimension: 64}

	b, _ := e.CreateBridge("test", "articles", "agent", vdb, emb)

	// Add diverse documents
	docs := []string{
		"Kafka consumer lag is increasing rapidly",
		"The weather is sunny and warm today",
		"Kafka broker is experiencing high CPU usage",
		"Machine learning models need training data",
		"Consumer group rebalancing is taking too long",
	}
	for i, d := range docs {
		e.ProcessMessage(b.ID, fmt.Sprintf("k%d", i), d, 0, int64(i), "articles")
	}

	results, err := e.SearchSimilar(b.ID, "Kafka consumer issues", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}
	if results[0].Score <= 0 {
		t.Error("expected positive similarity score")
	}
}

func TestBridgeLifecycle(t *testing.T) {
	e := newTestEngine()
	vdb := VectorDBConfig{Type: DBMilvus, Collection: "test"}
	emb := EmbeddingConfig{Dimension: 32}

	b, _ := e.CreateBridge("test", "topic", "agent", vdb, emb)

	// Pause
	if err := e.UpdateBridgeStatus(b.ID, "paused"); err != nil {
		t.Fatal(err)
	}

	// Process should fail when paused
	_, err := e.ProcessMessage(b.ID, "k", "v", 0, 0, "topic")
	if err == nil {
		t.Error("expected error for paused bridge")
	}

	// Resume
	e.UpdateBridgeStatus(b.ID, "active")

	// Delete
	e.DeleteBridge(b.ID)
	if _, err := e.GetBridge(b.ID); err == nil {
		t.Error("expected error after delete")
	}
}

func TestCosineSimilarity(t *testing.T) {
	a := []float64{1, 0, 0}
	b := []float64{1, 0, 0}
	if s := cosineSimilarity(a, b); s < 0.99 {
		t.Errorf("expected ~1.0 for identical vectors, got %f", s)
	}

	c := []float64{0, 1, 0}
	if s := cosineSimilarity(a, c); s > 0.01 {
		t.Errorf("expected ~0.0 for orthogonal vectors, got %f", s)
	}
}

func TestListBridges(t *testing.T) {
	e := newTestEngine()
	vdb := VectorDBConfig{Type: DBPinecone, Collection: "c1"}
	emb := EmbeddingConfig{Dimension: 32}
	e.CreateBridge("b1", "t1", "a", vdb, emb)
	e.CreateBridge("b2", "t2", "a", vdb, emb)

	bridges := e.ListBridges()
	if len(bridges) != 2 {
		t.Errorf("expected 2 bridges, got %d", len(bridges))
	}
}

func TestExtractFieldValue(t *testing.T) {
	tests := []struct {
		name     string
		json     string
		field    string
		expected string
	}{
		{
			name:     "simple quoted string",
			json:     `{"name": "John", "age": 30}`,
			field:    "name",
			expected: "John",
		},
		{
			name:     "field not found returns full JSON",
			json:     `{"name": "John"}`,
			field:    "missing",
			expected: `{"name": "John"}`,
		},
		{
			name:     "field at end of object",
			json:     `{"a": "first", "b": "last"}`,
			field:    "b",
			expected: "last",
		},
		{
			name:     "numeric value returns original (silent fallback)",
			json:     `{"count": 42}`,
			field:    "count",
			expected: `{"count": 42}`,
		},
		{
			name:     "nested object returns original",
			json:     `{"data": {"inner": "val"}}`,
			field:    "data",
			expected: `{"data": {"inner": "val"}}`,
		},
		{
			name:     "empty JSON returns original",
			json:     `{}`,
			field:    "name",
			expected: `{}`,
		},
		{
			name:     "non-JSON input returns original",
			json:     "plain text message",
			field:    "name",
			expected: "plain text message",
		},
		{
			name:     "unicode field name",
			json:     `{"名前": "太郎"}`,
			field:    "名前",
			expected: "太郎",
		},
		{
			name:     "unicode value",
			json:     `{"name": "日本語テスト"}`,
			field:    "name",
			expected: "日本語テスト",
		},
		{
			name:     "empty field value",
			json:     `{"name": ""}`,
			field:    "name",
			expected: "",
		},
		{
			name:     "field with spaces in value",
			json:     `{"msg": "hello world"}`,
			field:    "msg",
			expected: "hello world",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractFieldValue(tt.json, tt.field)
			if result != tt.expected {
				t.Errorf("extractFieldValue(%q, %q) = %q, want %q", tt.json, tt.field, result, tt.expected)
			}
		})
	}
}
