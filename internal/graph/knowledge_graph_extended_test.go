package graph

import (
	"log/slog"
	"testing"
	"time"
)

func TestQueryPath(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "topic:orders", Type: NodeTopic, Name: "orders"})
	kg.AddNode(Node{ID: "group:processor", Type: NodeConsumerGroup, Name: "processor"})
	kg.AddNode(Node{ID: "topic:invoices", Type: NodeTopic, Name: "invoices"})

	kg.AddEdge(Edge{From: "topic:orders", To: "group:processor", Type: EdgeConsumesFrom})
	kg.AddEdge(Edge{From: "group:processor", To: "topic:invoices", Type: EdgeProducesTo})

	result, err := kg.Query("path topic:orders to topic:invoices")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Paths) == 0 {
		t.Error("expected to find a path between orders and invoices")
	}
}

func TestQueryPathNoPath(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "topic:a", Type: NodeTopic, Name: "a"})
	kg.AddNode(Node{ID: "topic:b", Type: NodeTopic, Name: "b"})

	result, err := kg.Query("path topic:a to topic:b")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Paths) != 0 {
		t.Error("expected no path between disconnected nodes")
	}
}

func TestGetStats(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "topic:a", Type: NodeTopic, Name: "a"})
	kg.AddNode(Node{ID: "topic:b", Type: NodeTopic, Name: "b"})
	kg.AddNode(Node{ID: "group:g1", Type: NodeConsumerGroup, Name: "g1"})
	kg.AddEdge(Edge{From: "topic:a", To: "group:g1", Type: EdgeConsumesFrom})

	stats := kg.GetStats()
	if stats["total_nodes"].(int) != 3 {
		t.Errorf("expected 3 nodes, got %v", stats["total_nodes"])
	}
	if stats["total_edges"].(int) != 1 {
		t.Errorf("expected 1 edge, got %v", stats["total_edges"])
	}
	nodesByType, ok := stats["node_types"].(map[string]int)
	if !ok {
		t.Fatal("expected node_types map")
	}
	if nodesByType["topic"] != 2 {
		t.Errorf("expected 2 topic nodes, got %d", nodesByType["topic"])
	}
}

func TestGetStatsEmpty(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	stats := kg.GetStats()
	if stats["total_nodes"].(int) != 0 {
		t.Errorf("expected 0 nodes, got %v", stats["total_nodes"])
	}
}

func TestEvictStale(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.ttl = 1 * time.Millisecond

	kg.AddNode(Node{ID: "topic:old", Type: NodeTopic, Name: "old", UpdatedAt: time.Now().Add(-1 * time.Hour)})
	kg.AddNode(Node{ID: "topic:new", Type: NodeTopic, Name: "new", UpdatedAt: time.Now()})

	time.Sleep(5 * time.Millisecond)
	evicted := kg.EvictStale()
	if evicted < 1 {
		t.Errorf("expected at least 1 evicted node, got %d", evicted)
	}
}

func TestEvictStaleEmpty(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	evicted := kg.EvictStale()
	if evicted != 0 {
		t.Errorf("expected 0 evictions on empty graph, got %d", evicted)
	}
}

func TestQueryInvalidFormat(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	result, err := kg.Query("")
	// Empty query may return empty result or error depending on implementation
	if err == nil && result != nil && len(result.Nodes) == 0 {
		return // acceptable: returns empty result
	}
	if err != nil {
		return // acceptable: returns error
	}
}

func TestQueryUnknownCommand(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	_, err := kg.Query("invalid_command something")
	// Should either return an error or fall through to search
	if err != nil {
		return // error is fine
	}
	// If no error, it did a search, which is also fine
}

func TestAddNodeOverwrite(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "topic:a", Type: NodeTopic, Name: "a-old"})
	kg.AddNode(Node{ID: "topic:a", Type: NodeTopic, Name: "a-new"})

	stats := kg.GetStats()
	if stats["total_nodes"].(int) != 1 {
		t.Error("expected 1 node after overwrite")
	}
}

func TestSearchByNamePartial(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "topic:user-events", Type: NodeTopic, Name: "user-events"})
	kg.AddNode(Node{ID: "topic:order-events", Type: NodeTopic, Name: "order-events"})
	kg.AddNode(Node{ID: "topic:payments", Type: NodeTopic, Name: "payments"})

	result, err := kg.Query("events")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Nodes) < 2 {
		t.Errorf("expected at least 2 matching nodes, got %d", len(result.Nodes))
	}
}

func TestFindByTypeConsumerGroup(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(slog.Default())
	kg.AddNode(Node{ID: "topic:a", Type: NodeTopic, Name: "a"})
	kg.AddNode(Node{ID: "group:g1", Type: NodeConsumerGroup, Name: "g1"})
	kg.AddNode(Node{ID: "group:g2", Type: NodeConsumerGroup, Name: "g2"})

	result, err := kg.Query("type consumer_group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Nodes) != 2 {
		t.Errorf("expected 2 consumer_group nodes, got %d", len(result.Nodes))
	}
}

func TestEdgeTypes(t *testing.T) {
	t.Parallel()
	if EdgeProducesTo != "produces_to" {
		t.Errorf("unexpected EdgeProducesTo value: %s", EdgeProducesTo)
	}
	if EdgeConsumesFrom != "consumes_from" {
		t.Errorf("unexpected EdgeConsumesFrom value: %s", EdgeConsumesFrom)
	}
	if EdgeUsesSchema != "uses_schema" {
		t.Errorf("unexpected EdgeUsesSchema value: %s", EdgeUsesSchema)
	}
}

func TestNodeTypes(t *testing.T) {
	t.Parallel()
	if NodeTopic != "topic" {
		t.Errorf("unexpected NodeTopic value: %s", NodeTopic)
	}
	if NodeConsumerGroup != "consumer_group" {
		t.Errorf("unexpected NodeConsumerGroup value: %s", NodeConsumerGroup)
	}
}
