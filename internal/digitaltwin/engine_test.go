package digitaltwin

import (
	"log/slog"
	"testing"
)

func seedSnapshot(e *Engine) *ClusterSnapshot {
	return e.CaptureSnapshot("test-cluster",
		map[string]TopicState{
			"orders":   {Name: "orders", Partitions: 6, ReplicationFactor: 3, Configs: map[string]string{"retention.ms": "86400000"}},
			"payments": {Name: "payments", Partitions: 3, ReplicationFactor: 3, Configs: map[string]string{}},
		},
		map[string]ConsumerGroupState{
			"order-processor": {GroupID: "order-processor", State: "Stable", Members: 3, TotalLag: 100},
		},
		[]BrokerState{
			{ID: 1, Host: "broker-1", Port: 9092, Partitions: 5},
			{ID: 2, Host: "broker-2", Port: 9092, Partitions: 4},
			{ID: 3, Host: "broker-3", Port: 9092, Partitions: 5},
		},
	)
}

func TestCaptureAndCreateTwin(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	snap := seedSnapshot(e)

	twin, err := e.CreateTwin(snap.ID)
	if err != nil {
		t.Fatalf("CreateTwin failed: %v", err)
	}
	if len(twin.Topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(twin.Topics))
	}
}

func TestSimulateCreateTopic(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	snap := seedSnapshot(e)
	twin, _ := e.CreateTwin(snap.ID)

	result, err := e.SimulateOp(twin.ID, SimulationOp{
		Type:   "create_topic",
		Params: map[string]any{"name": "events", "partitions": 12, "replication_factor": 3},
	})
	if err != nil {
		t.Fatalf("SimulateOp failed: %v", err)
	}
	if !result.Success {
		t.Fatalf("expected success, got error: %s", result.Error)
	}

	got, _ := e.GetTwin(twin.ID)
	if _, exists := got.Topics["events"]; !exists {
		t.Error("events topic should exist in twin")
	}
}

func TestSimulateDeleteTopic(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	snap := seedSnapshot(e)
	twin, _ := e.CreateTwin(snap.ID)

	result, _ := e.SimulateOp(twin.ID, SimulationOp{
		Type:   "delete_topic",
		Params: map[string]any{"name": "payments"},
	})
	if !result.Success {
		t.Fatalf("expected success, got: %s", result.Error)
	}

	got, _ := e.GetTwin(twin.ID)
	if _, exists := got.Topics["payments"]; exists {
		t.Error("payments should be deleted")
	}
}

func TestSimulateAlterPartitions(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	snap := seedSnapshot(e)
	twin, _ := e.CreateTwin(snap.ID)

	// Increase partitions - should succeed
	result, _ := e.SimulateOp(twin.ID, SimulationOp{
		Type:   "alter_partitions",
		Params: map[string]any{"name": "orders", "partitions": 12},
	})
	if !result.Success {
		t.Fatalf("expected success: %s", result.Error)
	}

	// Decrease partitions - should fail
	result, _ = e.SimulateOp(twin.ID, SimulationOp{
		Type:   "alter_partitions",
		Params: map[string]any{"name": "orders", "partitions": 3},
	})
	if result.Success {
		t.Error("decreasing partitions should fail")
	}
}

func TestDiffAndMigration(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	snap := seedSnapshot(e)
	twin, _ := e.CreateTwin(snap.ID)

	// Make changes
	_, _ = e.SimulateOp(twin.ID, SimulationOp{Type: "create_topic", Params: map[string]any{"name": "events", "partitions": 6}})
	_, _ = e.SimulateOp(twin.ID, SimulationOp{Type: "delete_topic", Params: map[string]any{"name": "payments"}})
	_, _ = e.SimulateOp(twin.ID, SimulationOp{Type: "alter_partitions", Params: map[string]any{"name": "orders", "partitions": 12}})

	diff, err := e.Diff(twin.ID)
	if err != nil {
		t.Fatalf("Diff failed: %v", err)
	}
	if len(diff.AddedTopics) != 1 {
		t.Errorf("expected 1 added topic, got %d", len(diff.AddedTopics))
	}
	if len(diff.RemovedTopics) != 1 {
		t.Errorf("expected 1 removed topic, got %d", len(diff.RemovedTopics))
	}

	plan, err := e.GenerateMigrationPlan(twin.ID)
	if err != nil {
		t.Fatalf("GenerateMigrationPlan failed: %v", err)
	}
	if len(plan.Steps) == 0 {
		t.Error("expected migration steps")
	}
	if plan.Reversible {
		t.Error("plan with deletes should not be reversible")
	}
}

func TestRebalance(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	snap := seedSnapshot(e)
	twin, _ := e.CreateTwin(snap.ID)

	result, _ := e.SimulateOp(twin.ID, SimulationOp{Type: "rebalance"})
	if !result.Success {
		t.Fatalf("rebalance failed: %s", result.Error)
	}
}
