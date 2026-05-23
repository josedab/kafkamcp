package digitaltwin

import (
	"log/slog"
	"testing"
)

func TestPlayground_CreatePlayground(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	twin, err := e.CreatePlayground("test", map[string]int{"events": 6, "logs": 3}, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(twin.Topics) != 2 {
		t.Errorf("expected 2 topics, got %d", len(twin.Topics))
	}
	if len(twin.Brokers) != 3 {
		t.Errorf("expected 3 brokers, got %d", len(twin.Brokers))
	}
}

func TestPlayground_RunScenario(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	twin, _ := e.CreatePlayground("test", map[string]int{"events": 3}, 3)

	result, err := e.RunScenario(twin.ID, "scenario-scale-partitions", map[string]any{
		"topic": "events", "target_partitions": 6,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.AllPassed {
		t.Error("expected all passed")
	}
}

func TestPlayground_RunScenario_AddTopic(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	twin, _ := e.CreatePlayground("test", map[string]int{"existing": 3}, 3)

	result, err := e.RunScenario(twin.ID, "scenario-add-topic", map[string]any{"topic": "new-topic"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.AllPassed {
		t.Errorf("expected all passed, got operations: %+v", result.Operations)
	}
}

func TestPlayground_AnalyzeImpact_Delete(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	twin, _ := e.CreatePlayground("test", map[string]int{"events": 3}, 3)

	analysis, err := e.AnalyzeImpact(twin.ID, []SimulationOp{
		{Type: "delete_topic", Params: map[string]any{"name": "events"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !analysis.DataLossRisk {
		t.Error("expected data loss risk")
	}
	if analysis.RiskLevel != "critical" {
		t.Errorf("expected critical risk, got %s", analysis.RiskLevel)
	}
}

func TestPlayground_AnalyzeImpact_Rebalance(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	twin, _ := e.CreatePlayground("test", map[string]int{"events": 3}, 3)

	analysis, _ := e.AnalyzeImpact(twin.ID, []SimulationOp{
		{Type: "rebalance", Params: map[string]any{}},
	})
	if !analysis.DowntimeRisk {
		t.Error("expected downtime risk for rebalance")
	}
}

func TestPlayground_GetScenarios(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	all := e.GetScenarios("")
	if len(all) < 3 {
		t.Errorf("expected at least 3 scenarios, got %d", len(all))
	}

	scaling := e.GetScenarios("scaling")
	for _, s := range scaling {
		if s.Category != "scaling" {
			t.Errorf("expected scaling category, got %s", s.Category)
		}
	}
}

func TestPlayground_ResetTwin(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	twin, _ := e.CreatePlayground("test", map[string]int{"events": 3}, 3)

	// Modify the twin
	_, _ = e.SimulateOp(twin.ID, SimulationOp{
		Type: "create_topic", Params: map[string]any{"name": "new-topic", "partitions": 6},
	})
	if len(e.twins[twin.ID].Topics) != 2 {
		t.Fatal("expected 2 topics after create")
	}

	// Reset
	reset, err := e.ResetTwin(twin.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(reset.Topics) != 1 {
		t.Errorf("expected 1 topic after reset, got %d", len(reset.Topics))
	}
}
