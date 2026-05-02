package graph

import (
	"testing"
)

func TestAddNodeAndEdge(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(nil)
	kg.AddNode(Node{ID: "orders", Type: NodeTopic, Name: "orders"})
	kg.AddNode(Node{ID: "group:payment-svc", Type: NodeConsumerGroup, Name: "payment-svc"})
	kg.AddEdge(Edge{From: "orders", To: "group:payment-svc", Type: EdgeConsumesFrom})

	stats := kg.GetStats()
	if stats["total_nodes"].(int) != 2 {
		t.Errorf("expected 2 nodes, got %v", stats["total_nodes"])
	}
	if stats["total_edges"].(int) != 1 {
		t.Errorf("expected 1 edge, got %v", stats["total_edges"])
	}
}

func TestDiscoverFromTopology(t *testing.T) {
	t.Parallel()
	kg := NewKnowledgeGraph(nil)

	topics := []TopicInfo{
		{Name: "orders", Partitions: 6, ReplicationFactor: 3},
		{Name: "payments", Partitions: 3, ReplicationFactor: 3},
	}
	groups := []GroupInfo{
		{GroupID: "payment-svc", State: "Stable", MemberCount: 3, Topics: []string{"orders"}},
	}
	schemas := []SchemaLink{
		{Subject: "orders-value", SchemaType: "AVRO", Topic: "orders"},
	}

	kg.DiscoverFromTopology(topics, groups, schemas)

	stats := kg.GetStats()
	if stats["total_nodes"].(int) < 3 {
		t.Errorf("expected at least 3 nodes, got %v", stats["total_nodes"])
	}
}

func TestQueryDependsOn(t *testing.T) {
	t.Parallel()
	kg := setupTestGraph()
	result, err := kg.Query("depends_on orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Nodes) == 0 {
		t.Error("expected nodes in dependency result")
	}
}

func TestQueryImpact(t *testing.T) {
	t.Parallel()
	kg := setupTestGraph()
	result, err := kg.Query("impacts orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Nodes) == 0 {
		t.Error("expected nodes in impact result")
	}
}

func TestQueryNeighbors(t *testing.T) {
	t.Parallel()
	kg := setupTestGraph()
	result, err := kg.Query("neighbors orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Nodes) == 0 {
		t.Error("expected neighbors")
	}
}

func TestQueryAll(t *testing.T) {
	t.Parallel()
	kg := setupTestGraph()
	result, err := kg.Query("all")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalNodes == 0 {
		t.Error("expected some nodes")
	}
}

func TestQueryByType(t *testing.T) {
	t.Parallel()
	kg := setupTestGraph()
	result, err := kg.Query("type topic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, n := range result.Nodes {
		if n.Type != NodeTopic {
			t.Errorf("expected topic type, got %s", n.Type)
		}
	}
}

func TestSearchByName(t *testing.T) {
	t.Parallel()
	kg := setupTestGraph()
	result, err := kg.Query("order")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Nodes) == 0 {
		t.Error("expected to find nodes matching 'order'")
	}
}

func setupTestGraph() *KnowledgeGraph {
	kg := NewKnowledgeGraph(nil)
	kg.AddNode(Node{ID: "orders", Type: NodeTopic, Name: "orders"})
	kg.AddNode(Node{ID: "payments", Type: NodeTopic, Name: "payments"})
	kg.AddNode(Node{ID: "group:payment-svc", Type: NodeConsumerGroup, Name: "payment-svc"})
	kg.AddEdge(Edge{From: "orders", To: "group:payment-svc", Type: EdgeConsumesFrom})
	kg.AddEdge(Edge{From: "group:payment-svc", To: "payments", Type: EdgeProducesTo})
	return kg
}
