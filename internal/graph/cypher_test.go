package graph

import (
	"log/slog"
	"testing"
)

func buildTestGraph(t *testing.T) *KnowledgeGraph {
	t.Helper()
	kg := NewKnowledgeGraph(slog.Default())
	kg.DiscoverFromTopology(
		[]TopicInfo{
			{Name: "orders", Partitions: 6, ReplicationFactor: 3},
			{Name: "payments", Partitions: 3, ReplicationFactor: 3},
			{Name: "analytics", Partitions: 12, ReplicationFactor: 3},
		},
		[]GroupInfo{
			{GroupID: "payment-service", State: "Stable", MemberCount: 3, Topics: []string{"orders"}},
			{GroupID: "analytics-pipeline", State: "Stable", MemberCount: 2, Topics: []string{"orders", "payments"}},
		},
		[]SchemaLink{
			{Subject: "orders-value", SchemaType: "avro", Topic: "orders"},
		},
	)
	// Add a produce edge
	kg.AddEdge(Edge{From: "producer:order-api", To: "orders", Type: EdgeProducesTo})
	kg.AddNode(Node{ID: "producer:order-api", Type: NodeProducer, Name: "order-api"})
	return kg
}

func TestCypherMatchNodeType(t *testing.T) {
	kg := buildTestGraph(t)
	result, err := kg.ExecuteCypher("MATCH (n:topic) RETURN n")
	if err != nil {
		t.Fatalf("ExecuteCypher: %v", err)
	}
	if result.TotalNodes != 3 {
		t.Errorf("TotalNodes = %d, want 3", result.TotalNodes)
	}
}

func TestCypherMatchRelationship(t *testing.T) {
	kg := buildTestGraph(t)
	result, err := kg.ExecuteCypher("MATCH (n:topic)-[:consumes_from]->(m:consumer_group) RETURN n,m")
	if err != nil {
		t.Fatalf("ExecuteCypher: %v", err)
	}
	if result.TotalEdges == 0 {
		t.Error("expected edges for consumes_from relationship")
	}
}

func TestCypherShortestPath(t *testing.T) {
	kg := buildTestGraph(t)
	result, err := kg.ExecuteCypher(`SHORTEST_PATH (a)->(b) WHERE a.name = "orders" AND b.name = "analytics-pipeline"`)
	if err != nil {
		t.Fatalf("ExecuteCypher: %v", err)
	}
	if len(result.Paths) == 0 {
		t.Error("expected at least one path")
	}
}

func TestCypherAllPaths(t *testing.T) {
	kg := buildTestGraph(t)
	result, err := kg.ExecuteCypher(`ALL_PATHS (a)-[*..5]->(b) WHERE a = "orders" AND b = "group:analytics-pipeline"`)
	if err != nil {
		t.Fatalf("ExecuteCypher: %v", err)
	}
	if len(result.Paths) == 0 {
		t.Error("expected at least one path")
	}
}

func TestNaturalLanguageQuery(t *testing.T) {
	kg := buildTestGraph(t)

	tests := []struct {
		name     string
		question string
		wantMin  int
	}{
		{"list topics", "What topics are in the cluster?", 3},
		{"consumer groups", "Show me all consumer groups", 2},
		{"schemas", "What schemas exist?", 1},
		{"who consumes orders", "Who consumes orders?", 1},
		{"count", "How many entities exist?", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := kg.NaturalLanguageQuery(tt.question)
			if err != nil {
				t.Fatalf("NaturalLanguageQuery: %v", err)
			}
			if result.TotalNodes < tt.wantMin {
				t.Errorf("TotalNodes = %d, want >= %d", result.TotalNodes, tt.wantMin)
			}
		})
	}
}

func TestExtractNodeType(t *testing.T) {
	tests := []struct {
		input string
		want  NodeType
	}{
		{"(n:topic)", "topic"},
		{"(n:consumer_group)", "consumer_group"},
		{"(n)", ""},
	}
	for _, tt := range tests {
		got := extractNodeType(tt.input)
		if got != tt.want {
			t.Errorf("extractNodeType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestExtractPathEndpoints(t *testing.T) {
	from, to, err := extractPathEndpoints(`SHORTEST_PATH (a)->(b) WHERE a.name = "orders" AND b.name = "analytics"`)
	if err != nil {
		t.Fatalf("extractPathEndpoints: %v", err)
	}
	if from != "orders" {
		t.Errorf("from = %q, want orders", from)
	}
	if to != "analytics" {
		t.Errorf("to = %q, want analytics", to)
	}
}
