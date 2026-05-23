package crosslineage

import (
	"log/slog"
	"testing"
)

func TestRegisterNode(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	n := e.RegisterNode("orders", NodeTopic, map[string]string{"partitions": "6"})
	if n.ID != "topic:orders" {
		t.Errorf("expected id topic:orders, got %s", n.ID)
	}
}

func TestAddEdge(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("raw-events", NodeTopic, nil)
	e.RegisterNode("enriched-events", NodeTopic, nil)
	edge, err := e.AddEdge("topic:raw-events", "topic:enriched-events", "transform", "transforms", nil)
	if err != nil {
		t.Fatal(err)
	}
	if edge.SourceID != "topic:raw-events" {
		t.Errorf("unexpected source: %s", edge.SourceID)
	}
}

func TestAddEdge_MissingNode(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.AddEdge("topic:missing", "topic:also-missing", "", "produces", nil)
	if err == nil {
		t.Error("expected error for missing nodes")
	}
}

func TestGetGraph(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("topic-a", NodeTopic, nil)
	e.RegisterNode("topic-b", NodeTopic, nil)
	_, _ = e.AddEdge("topic:topic-a", "topic:topic-b", "", "produces", nil)

	graph := e.GetGraph()
	if graph.TotalNodes != 2 {
		t.Errorf("expected 2 nodes, got %d", graph.TotalNodes)
	}
	if graph.TotalEdges != 1 {
		t.Errorf("expected 1 edge, got %d", graph.TotalEdges)
	}
}

func TestTraceLineage(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("source", NodeTopic, nil)
	e.RegisterNode("processor", NodeStream, nil)
	e.RegisterNode("target", NodeTopic, nil)

	_, _ = e.AddEdge("topic:source", "stream_app:processor", "read", "consumes", nil)
	_, _ = e.AddEdge("stream_app:processor", "topic:target", "write", "produces", nil)

	lineage := e.TraceLineage("target", 5)
	if len(lineage.Upstream) == 0 {
		t.Error("expected upstream lineage")
	}
}

func TestRemoveNode(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.RegisterNode("to-remove", NodeTopic, nil)
	e.RegisterNode("other", NodeTopic, nil)
	_, _ = e.AddEdge("topic:to-remove", "topic:other", "", "produces", nil)

	if err := e.RemoveNode("topic:to-remove"); err != nil {
		t.Fatal(err)
	}

	graph := e.GetGraph()
	if graph.TotalNodes != 1 {
		t.Errorf("expected 1 node after removal, got %d", graph.TotalNodes)
	}
	if graph.TotalEdges != 0 {
		t.Errorf("expected 0 edges after removal, got %d", graph.TotalEdges)
	}
}
