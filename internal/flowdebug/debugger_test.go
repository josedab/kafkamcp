package flowdebug

import (
	"testing"
	"time"
)

func TestUpdateGraph(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	msg := WebSocketMessage{Type: "test", Payload: map[string]string{"key": "value"}}
	data, err := SerializeUpdate(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty serialization")
	}
}

func TestSetGetBottleneckConfig_Roundtrip(t *testing.T) {
	t.Parallel()
	d := NewDebugger(nil)

	cfg := BottleneckConfig{
		LagWarning:        5000,
		LagCritical:       50000,
		ErrorRateWarning:  0.02,
		ErrorRateCritical: 0.10,
		LatencyWarningMs:  500,
		LatencyCriticalMs: 2000,
		ThroughputMinMsg:  5.0,
	}
	d.SetBottleneckConfig(cfg)

	got := d.GetBottleneckConfig()
	if got.LagWarning != 5000 {
		t.Errorf("expected LagWarning=5000, got %d", got.LagWarning)
	}
	if got.LagCritical != 50000 {
		t.Errorf("expected LagCritical=50000, got %d", got.LagCritical)
	}
	if got.ErrorRateWarning != 0.02 {
		t.Errorf("expected ErrorRateWarning=0.02, got %f", got.ErrorRateWarning)
	}
	if got.LatencyWarningMs != 500 {
		t.Errorf("expected LatencyWarningMs=500, got %f", got.LatencyWarningMs)
	}
	if got.ThroughputMinMsg != 5.0 {
		t.Errorf("expected ThroughputMinMsg=5.0, got %f", got.ThroughputMinMsg)
	}
}

func TestGetBottleneckConfig_Default(t *testing.T) {
	t.Parallel()
	d := NewDebugger(nil)
	cfg := d.GetBottleneckConfig()

	expected := DefaultBottleneckConfig()
	if cfg.LagWarning != expected.LagWarning {
		t.Errorf("expected default LagWarning=%d, got %d", expected.LagWarning, cfg.LagWarning)
	}
	if cfg.ErrorRateCritical != expected.ErrorRateCritical {
		t.Errorf("expected default ErrorRateCritical=%f, got %f", expected.ErrorRateCritical, cfg.ErrorRateCritical)
	}
}

func TestGetBottlenecks_NoData(t *testing.T) {
	t.Parallel()
	d := NewDebugger(nil)
	bottlenecks := d.GetBottlenecks()
	if len(bottlenecks) != 0 {
		t.Errorf("expected 0 bottlenecks for empty graph, got %d", len(bottlenecks))
	}
}

func TestGetBottlenecks_HealthyGraph(t *testing.T) {
	t.Parallel()
	d := NewDebugger(nil)
	d.UpdateGraph(&FlowGraph{
		Nodes: []FlowNode{
			{ID: "orders", Type: "topic", Name: "orders", ConsumerLag: 100, ErrorRate: 0, Throughput: 100, HealthStatus: "healthy"},
		},
		Edges: []FlowEdge{
			{Source: "orders", Target: "svc", AvgLatencyMs: 50},
		},
	})

	bottlenecks := d.GetBottlenecks()
	if len(bottlenecks) != 0 {
		t.Errorf("expected 0 bottlenecks for healthy graph, got %d: %+v", len(bottlenecks), bottlenecks)
	}
}

func TestGetBottlenecks_DetectedBottlenecks(t *testing.T) {
	t.Parallel()
	d := NewDebugger(nil)
	d.UpdateGraph(&FlowGraph{
		Nodes: []FlowNode{
			{ID: "slow-consumer", Type: "consumer_group", Name: "slow-svc", ConsumerLag: 150000, HealthStatus: "critical"},
		},
	})

	bottlenecks := d.GetBottlenecks()
	if len(bottlenecks) == 0 {
		t.Fatal("expected at least one bottleneck")
	}
	found := false
	for _, b := range bottlenecks {
		if b.Type == "high_lag" && b.Severity == "critical" {
			found = true
		}
	}
	if !found {
		t.Error("expected critical high_lag bottleneck")
	}
}

func TestSetBottleneckConfig_ZeroThresholds(t *testing.T) {
	t.Parallel()
	d := NewDebugger(nil)
	cfg := BottleneckConfig{} // all zeros
	d.SetBottleneckConfig(cfg)

	got := d.GetBottleneckConfig()
	if got.LagWarning != 0 || got.LagCritical != 0 {
		t.Error("expected zero thresholds to be stored")
	}

	// With zero thresholds, any lag > 0 should trigger detection
	d.UpdateGraph(&FlowGraph{
		Nodes: []FlowNode{
			{ID: "n1", Name: "test", ConsumerLag: 1},
		},
	})
	bottlenecks := d.GetBottlenecks()
	hasLag := false
	for _, b := range bottlenecks {
		if b.Type == "high_lag" {
			hasLag = true
		}
	}
	if !hasLag {
		t.Error("expected high_lag with zero threshold and lag=1")
	}
}

func TestSetBottleneckConfig_NegativeValues(t *testing.T) {
	t.Parallel()
	d := NewDebugger(nil)
	cfg := BottleneckConfig{
		LagWarning:        -100,
		ErrorRateWarning:  -0.5,
		LatencyWarningMs:  -1000,
		ThroughputMinMsg:  -10,
	}
	d.SetBottleneckConfig(cfg)

	got := d.GetBottleneckConfig()
	if got.LagWarning != -100 {
		t.Errorf("expected negative LagWarning stored as-is, got %d", got.LagWarning)
	}
}

func TestSerializeUpdate_StructureAndFields(t *testing.T) {
	t.Parallel()
	msg := WebSocketMessage{
		Type:    "bottleneck_alert",
		Payload: []Bottleneck{{NodeID: "n1", Type: "high_lag", Severity: "critical"}},
	}
	data, err := SerializeUpdate(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	str := string(data)
	if !contains(str, `"type":"bottleneck_alert"`) {
		t.Errorf("expected 'type' field in JSON, got %s", str)
	}
	if !contains(str, `"payload"`) {
		t.Errorf("expected 'payload' field in JSON, got %s", str)
	}
}

func TestSerializeUpdate_NilPayload(t *testing.T) {
	t.Parallel()
	msg := WebSocketMessage{Type: "flow_update", Payload: nil}
	data, err := SerializeUpdate(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	str := string(data)
	if !contains(str, `"payload":null`) {
		t.Errorf("expected null payload in JSON, got %s", str)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
