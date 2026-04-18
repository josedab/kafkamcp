package flowdebug

import (
	"testing"
	"time"
)

func TestUpdateGraph(t *testing.T) {
	d := NewDebugger(nil)

	graph := &FlowGraph{
		ClusterID: "test",
		Nodes: []FlowNode{
			{ID: "orders", Type: "topic", Name: "orders", Throughput: 100, Partitions: 6, HealthStatus: "healthy"},
			{ID: "group:svc", Type: "consumer_group", Name: "svc", ConsumerLag: 500, HealthStatus: "healthy"},
		},
		Edges: []FlowEdge{
			{Source: "orders", Target: "group:svc", Throughput: 100, AvgLatencyMs: 50, Status: "active"},
		},
	}

	d.UpdateGraph(graph)

	current := d.GetCurrentGraph()
	if len(current.Nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(current.Nodes))
	}
}

func TestBottleneckDetection(t *testing.T) {
	d := NewDebugger(nil)

	graph := &FlowGraph{
		Nodes: []FlowNode{
			{ID: "orders", Type: "topic", Name: "orders", ConsumerLag: 200000, HealthStatus: "critical"},
			{ID: "errors", Type: "topic", Name: "errors", ErrorRate: 0.1, HealthStatus: "critical"},
		},
		Edges: []FlowEdge{
			{Source: "orders", Target: "svc", AvgLatencyMs: 10000},
		},
	}

	d.UpdateGraph(graph)
	bottlenecks := d.GetBottlenecks()

	if len(bottlenecks) < 3 {
		t.Errorf("expected at least 3 bottlenecks (lag, error, latency), got %d", len(bottlenecks))
	}

	// Check that high lag is critical
	for _, b := range bottlenecks {
		if b.Type == "high_lag" && b.Severity != "critical" {
			t.Errorf("200k lag should be critical, got %s", b.Severity)
		}
	}
}

func TestSubscribe(t *testing.T) {
	d := NewDebugger(nil)

	ch := d.Subscribe("client1")
	defer d.Unsubscribe("client1")

	go func() {
		d.UpdateGraph(&FlowGraph{
			Nodes: []FlowNode{{ID: "test", Type: "topic", Name: "test", HealthStatus: "healthy"}},
		})
	}()

	select {
	case msg := <-ch:
		if msg.Type != "flow_update" {
			t.Errorf("expected flow_update, got %s", msg.Type)
		}
	case <-time.After(1 * time.Second):
		t.Error("timed out waiting for update")
	}
}

func TestSnapshots(t *testing.T) {
	d := NewDebugger(nil)

	for i := 0; i < 5; i++ {
		d.UpdateGraph(&FlowGraph{
			Nodes: []FlowNode{{ID: "test", Type: "topic", Name: "test", HealthStatus: "healthy"}},
		})
	}

	snapshots := d.GetSnapshots(time.Time{}, time.Time{}, 0)
	if len(snapshots) != 5 {
		t.Errorf("expected 5 snapshots, got %d", len(snapshots))
	}

	// With limit
	limited := d.GetSnapshots(time.Time{}, time.Time{}, 3)
	if len(limited) != 3 {
		t.Errorf("expected 3 snapshots, got %d", len(limited))
	}
}

func TestSerializeUpdate(t *testing.T) {
	msg := WebSocketMessage{Type: "test", Payload: map[string]string{"key": "value"}}
	data, err := SerializeUpdate(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty serialization")
	}
}
