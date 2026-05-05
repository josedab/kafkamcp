package graph

import (
	"log/slog"
	"testing"
	"time"
)

func dummyStart() time.Time { return time.Now() }

// --- findProducersOf ---

func TestFindProducersOf_MultipleProducers(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "orders", Type: NodeTopic, Name: "orders"})
	kg.AddNode(Node{ID: "producer:api", Type: NodeProducer, Name: "api"})
	kg.AddNode(Node{ID: "producer:batch", Type: NodeProducer, Name: "batch"})
	kg.AddEdge(Edge{From: "producer:api", To: "orders", Type: EdgeProducesTo})
	kg.AddEdge(Edge{From: "producer:batch", To: "orders", Type: EdgeProducesTo})

	result, err := kg.NaturalLanguageQuery("who produces orders?")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes < 2 {
		t.Errorf("expected at least 2 producers, got %d", result.TotalNodes)
	}
}

func TestFindProducersOf_NoProducers(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "orders", Type: NodeTopic, Name: "orders"})

	result, err := kg.NaturalLanguageQuery("who produces orders?")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes != 0 {
		t.Errorf("expected 0 producers, got %d", result.TotalNodes)
	}
}

func TestFindProducersOf_NonExistentTopic(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())

	result, err := kg.NaturalLanguageQuery("who produces nonexistent?")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes != 0 {
		t.Errorf("expected 0, got %d", result.TotalNodes)
	}
}

func TestFindProducersOf_SingleProducer(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "orders", Type: NodeTopic, Name: "orders"})
	kg.AddNode(Node{ID: "producer:api", Type: NodeProducer, Name: "api"})
	kg.AddEdge(Edge{From: "producer:api", To: "orders", Type: EdgeProducesTo})

	result, err := kg.NaturalLanguageQuery("who writes orders?")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes != 1 {
		t.Errorf("expected 1 producer, got %d", result.TotalNodes)
	}
}

// --- findImpactLocked BFS ---

func TestFindImpactLocked_DeepChain(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	// Build a chain: A -> B -> C -> D -> E -> F (5+ hops)
	for _, id := range []string{"A", "B", "C", "D", "E", "F"} {
		kg.AddNode(Node{ID: id, Type: NodeTopic, Name: id})
	}
	kg.AddEdge(Edge{From: "A", To: "B", Type: EdgeDependsOn})
	kg.AddEdge(Edge{From: "B", To: "C", Type: EdgeDependsOn})
	kg.AddEdge(Edge{From: "C", To: "D", Type: EdgeDependsOn})
	kg.AddEdge(Edge{From: "D", To: "E", Type: EdgeDependsOn})
	kg.AddEdge(Edge{From: "E", To: "F", Type: EdgeDependsOn})

	result, err := kg.NaturalLanguageQuery("impact of A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should find all 6 nodes (A + 5 downstream)
	if result.TotalNodes != 6 {
		t.Errorf("expected 6 nodes in impact chain, got %d", result.TotalNodes)
	}
	if result.TotalEdges != 5 {
		t.Errorf("expected 5 edges, got %d", result.TotalEdges)
	}
}

func TestFindImpactLocked_CycleDetection(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "A", Type: NodeTopic, Name: "A"})
	kg.AddNode(Node{ID: "B", Type: NodeTopic, Name: "B"})
	kg.AddNode(Node{ID: "C", Type: NodeTopic, Name: "C"})
	kg.AddEdge(Edge{From: "A", To: "B", Type: EdgeDependsOn})
	kg.AddEdge(Edge{From: "B", To: "C", Type: EdgeDependsOn})
	kg.AddEdge(Edge{From: "C", To: "A", Type: EdgeDependsOn})

	result, err := kg.NaturalLanguageQuery("impact of A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// BFS with visited set should not infinite loop; should find all 3 nodes
	if result.TotalNodes != 3 {
		t.Errorf("expected 3 nodes with cycle, got %d", result.TotalNodes)
	}
}

func TestFindImpactLocked_IsolatedNode(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "isolated", Type: NodeTopic, Name: "isolated"})

	result, err := kg.NaturalLanguageQuery("impact of isolated")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes != 1 {
		t.Errorf("expected 1 (self), got %d", result.TotalNodes)
	}
	if result.TotalEdges != 0 {
		t.Errorf("expected 0 edges, got %d", result.TotalEdges)
	}
}

// --- findDependentsLocked ---

func TestFindDependentsLocked_ForwardAndReverse(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "A", Type: NodeTopic, Name: "A"})
	kg.AddNode(Node{ID: "B", Type: NodeTopic, Name: "B"})
	kg.AddNode(Node{ID: "C", Type: NodeTopic, Name: "C"})
	kg.AddEdge(Edge{From: "A", To: "B", Type: EdgeDependsOn})
	kg.AddEdge(Edge{From: "C", To: "A", Type: EdgeDependsOn})

	result, err := kg.NaturalLanguageQuery("dependencies of A")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Forward edge A->B and reverse edge C->A => 2 nodes
	if result.TotalNodes != 2 {
		t.Errorf("expected 2 dependent nodes, got %d", result.TotalNodes)
	}
	if result.TotalEdges != 2 {
		t.Errorf("expected 2 edges, got %d", result.TotalEdges)
	}
}

func TestFindDependentsLocked_LeafNode(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "leaf", Type: NodeTopic, Name: "leaf"})
	kg.AddNode(Node{ID: "parent", Type: NodeTopic, Name: "parent"})
	kg.AddEdge(Edge{From: "parent", To: "leaf", Type: EdgeDependsOn})

	result, err := kg.NaturalLanguageQuery("dependencies of leaf")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// leaf has no forward edges, only reverse edge from parent
	if result.TotalNodes != 1 {
		t.Errorf("expected 1 node (parent), got %d", result.TotalNodes)
	}
}

// --- searchByNameLocked ---

func TestSearchByNameLocked_CaseInsensitive(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "orders", Type: NodeTopic, Name: "Orders-Topic"})

	result := kg.searchByNameLocked("ORDERS", dummyStart())
	if result.TotalNodes != 1 {
		t.Errorf("expected 1, got %d", result.TotalNodes)
	}
}

func TestSearchByNameLocked_Substring(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "orders", Type: NodeTopic, Name: "orders-topic"})
	kg.AddNode(Node{ID: "payment-orders", Type: NodeTopic, Name: "payment-orders"})

	result := kg.searchByNameLocked("order", dummyStart())
	if result.TotalNodes != 2 {
		t.Errorf("expected 2, got %d", result.TotalNodes)
	}
}

func TestSearchByNameLocked_NoResults(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "orders", Type: NodeTopic, Name: "orders"})

	result := kg.searchByNameLocked("zzz_nonexistent", dummyStart())
	if result.TotalNodes != 0 {
		t.Errorf("expected 0, got %d", result.TotalNodes)
	}
}

func TestSearchByNameLocked_SpecialChars(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "special", Type: NodeTopic, Name: "my.topic-v2_test"})

	result := kg.searchByNameLocked("topic-v2", dummyStart())
	if result.TotalNodes != 1 {
		t.Errorf("expected 1, got %d", result.TotalNodes)
	}
}

// --- NaturalLanguageQuery patterns ---

func TestNLQ_WhoConsumes(t *testing.T) {
	t.Parallel()
	kg := buildTestGraph(t)
	result, err := kg.NaturalLanguageQuery("who consumes orders?")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes == 0 {
		t.Error("expected at least one consumer")
	}
}

func TestNLQ_ImpactOf(t *testing.T) {
	t.Parallel()
	kg := buildTestGraph(t)
	result, err := kg.NaturalLanguageQuery("impact of orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes == 0 {
		t.Error("expected impact results")
	}
}

func TestNLQ_DependenciesOf(t *testing.T) {
	t.Parallel()
	kg := buildTestGraph(t)
	result, err := kg.NaturalLanguageQuery("dependencies of orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes == 0 {
		t.Error("expected dependency results")
	}
}

func TestNLQ_UnknownPattern(t *testing.T) {
	t.Parallel()
	kg := buildTestGraph(t)
	result, err := kg.NaturalLanguageQuery("xyzzy foobar baz")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Unknown pattern with short words should return empty
	if result.TotalNodes != 0 {
		t.Errorf("expected 0 for unknown short words, got %d", result.TotalNodes)
	}
}

func TestNLQ_EmptyGraph(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())

	result, err := kg.NaturalLanguageQuery("who consumes orders?")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes != 0 {
		t.Errorf("expected 0 on empty graph, got %d", result.TotalNodes)
	}
}

func TestNLQ_UnknownPatternFallbackSearch(t *testing.T) {
	t.Parallel()
	kg := buildTestGraph(t)
	// "orders" is a >3 char word that matches a node
	result, err := kg.NaturalLanguageQuery("please find orders details")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes == 0 {
		t.Error("expected fallback search to find 'orders'")
	}
}

// --- extractNodeName ---

func TestExtractNodeName_Quoted(t *testing.T) {
	t.Parallel()
	// extractNodeName uses Trim which strips chars from both ends
	got := extractNodeName(`(n:topic {name: "orders"})`)
	if got == "" {
		t.Error("expected non-empty for quoted name")
	}
}

func TestExtractNodeName_Unquoted(t *testing.T) {
	t.Parallel()
	got := extractNodeName(`(n:topic)`)
	if got != "" {
		t.Errorf("expected empty for no name filter, got %q", got)
	}
}

func TestExtractNodeName_NoNameField(t *testing.T) {
	t.Parallel()
	got := extractNodeName(`(n:topic {type: "avro"})`)
	if got != "" {
		t.Errorf("expected empty when no {name: field, got %q", got)
	}
}

func TestExtractNodeName_SingleQuotes(t *testing.T) {
	t.Parallel()
	got := extractNodeName(`(n:topic {name: 'payments'})`)
	if got == "" {
		t.Error("expected non-empty for single-quoted name")
	}
}

// --- extractEntityFromNL ---

func TestExtractEntityFromNL_WithPrefix(t *testing.T) {
	got := extractEntityFromNL("who consumes orders?", []string{"who consumes"})
	if got != "orders" {
		t.Errorf("expected orders, got %q", got)
	}
}

func TestExtractEntityFromNL_NoMatch(t *testing.T) {
	got := extractEntityFromNL("something else entirely", []string{"who consumes"})
	if got != "" {
		t.Errorf("expected empty, got %q", got)
	}
}

func TestExtractEntityFromNL_EmptyRest(t *testing.T) {
	got := extractEntityFromNL("who consumes", []string{"who consumes"})
	if got != "" {
		t.Errorf("expected empty for no entity after prefix, got %q", got)
	}
}
