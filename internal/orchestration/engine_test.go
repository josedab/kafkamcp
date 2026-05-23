package orchestration

import (
	"log/slog"
	"testing"
)

func newTestEngine() *Engine {
	return NewEngine(slog.Default())
}

func TestRegisterAgent(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	a, err := e.RegisterAgent("agent-1", "Monitor Agent", RoleMonitor, []string{"kafka_list_topics"})
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != "idle" {
		t.Errorf("expected idle, got %s", a.Status)
	}

	agents := e.ListAgents()
	if len(agents) != 1 {
		t.Errorf("expected 1 agent, got %d", len(agents))
	}
}

func TestCreateOrchestration(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	_, _ = e.RegisterAgent("agent-1", "Monitor", RoleMonitor, nil)
	_, _ = e.RegisterAgent("agent-2", "Operator", RoleOperator, nil)

	steps := []OrchestrationStep{
		{ID: "s1", Name: "Check health", Tool: "kafka_cluster_info", AssignTo: "agent-1"},
		{ID: "s2", Name: "Fix issues", Tool: "kafka_rebalance", AssignTo: "agent-2", DependsOn: []string{"s1"}},
	}

	orch, err := e.CreateOrchestration("incident-response", "Fix cluster issues", "coordinator", steps)
	if err != nil {
		t.Fatal(err)
	}
	if orch.Status != "pending" {
		t.Errorf("expected pending, got %s", orch.Status)
	}
	if len(orch.Results) != 2 {
		t.Errorf("expected 2 results, got %d", len(orch.Results))
	}
}

func TestCreateOrchestrationValidation(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	if _, err := e.CreateOrchestration("", "", "", nil); err == nil {
		t.Error("expected error for empty name")
	}
	if _, err := e.CreateOrchestration("test", "", "", nil); err == nil {
		t.Error("expected error for no steps")
	}

	// Duplicate step IDs
	steps := []OrchestrationStep{
		{ID: "s1", Name: "a", Tool: "t"},
		{ID: "s1", Name: "b", Tool: "t"},
	}
	if _, err := e.CreateOrchestration("test", "", "", steps); err == nil {
		t.Error("expected error for duplicate step IDs")
	}

	// Invalid dependency
	steps = []OrchestrationStep{
		{ID: "s1", Name: "a", Tool: "t", DependsOn: []string{"nonexistent"}},
	}
	if _, err := e.CreateOrchestration("test", "", "", steps); err == nil {
		t.Error("expected error for invalid dependency")
	}
}

func TestGetReadyStepsAndComplete(t *testing.T) {
	t.Parallel()
	e := newTestEngine()

	steps := []OrchestrationStep{
		{ID: "s1", Name: "Step 1", Tool: "tool_a"},
		{ID: "s2", Name: "Step 2", Tool: "tool_b", DependsOn: []string{"s1"}},
		{ID: "s3", Name: "Step 3", Tool: "tool_c", DependsOn: []string{"s1"}},
		{ID: "s4", Name: "Step 4", Tool: "tool_d", DependsOn: []string{"s2", "s3"}},
	}

	orch, _ := e.CreateOrchestration("test", "", "coord", steps)

	// Initially only s1 is ready
	ready, _ := e.GetReadySteps(orch.ID)
	if len(ready) != 1 || ready[0].ID != "s1" {
		t.Errorf("expected only s1 ready, got %d steps", len(ready))
	}

	// Complete s1 -> s2, s3 become ready
	_ = e.CompleteStep(orch.ID, "s1", "a1", "completed", map[string]any{"ok": true}, "")
	ready, _ = e.GetReadySteps(orch.ID)
	if len(ready) != 2 {
		t.Errorf("expected 2 ready steps, got %d", len(ready))
	}

	// Complete s2 and s3 -> s4 becomes ready
	_ = e.CompleteStep(orch.ID, "s2", "a1", "completed", nil, "")
	_ = e.CompleteStep(orch.ID, "s3", "a2", "completed", nil, "")
	ready, _ = e.GetReadySteps(orch.ID)
	if len(ready) != 1 || ready[0].ID != "s4" {
		t.Errorf("expected only s4 ready, got %v", ready)
	}

	// Complete s4 -> orchestration completes
	_ = e.CompleteStep(orch.ID, "s4", "a1", "completed", nil, "")
	o, _ := e.GetOrchestration(orch.ID)
	if o.Status != "completed" {
		t.Errorf("expected completed, got %s", o.Status)
	}
}

func TestHandoffs(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	_, _ = e.RegisterAgent("a1", "Agent 1", RoleMonitor, nil)
	_, _ = e.RegisterAgent("a2", "Agent 2", RoleOperator, nil)

	h, err := e.CreateHandoff("a1", "a2", "Please investigate this lag issue", map[string]any{"topic": "orders"})
	if err != nil {
		t.Fatal(err)
	}
	if h.Status != "pending" {
		t.Errorf("expected pending, got %s", h.Status)
	}

	if err := e.AcceptHandoff(h.ID, "a2"); err != nil {
		t.Fatal(err)
	}

	// Wrong agent
	if err := e.AcceptHandoff(h.ID, "a1"); err == nil {
		t.Error("expected error for wrong agent")
	}

	handoffs := e.ListHandoffs("a2")
	if len(handoffs) != 1 {
		t.Errorf("expected 1 handoff, got %d", len(handoffs))
	}
}

func TestSharedContext(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	steps := []OrchestrationStep{{ID: "s1", Name: "a", Tool: "t"}}
	orch, _ := e.CreateOrchestration("test", "", "c", steps)

	_ = e.UpdateSharedContext(orch.ID, "findings", "lag detected")
	ctx, _ := e.GetSharedContext(orch.ID)
	if ctx["findings"] != "lag detected" {
		t.Error("expected shared context value")
	}
}

func TestFindAgentByRole(t *testing.T) {
	t.Parallel()
	e := newTestEngine()
	_, _ = e.RegisterAgent("a1", "Mon 1", RoleMonitor, nil)
	_, _ = e.RegisterAgent("a2", "Mon 2", RoleMonitor, nil)
	_, _ = e.RegisterAgent("a3", "Op 1", RoleOperator, nil)

	monitors := e.FindAgentByRole(RoleMonitor)
	if len(monitors) != 2 {
		t.Errorf("expected 2 monitors, got %d", len(monitors))
	}
}
