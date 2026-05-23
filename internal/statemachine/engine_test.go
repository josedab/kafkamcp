package statemachine

import (
	"log/slog"
	"testing"
)

func TestParseDefinitionYAML(t *testing.T) {
	yaml := `
name: "Order Flow"
topic: "order-events"
initial_state: "created"
states:
  - name: created
  - name: processing
  - name: done
    terminal: true
transitions:
  - from: created
    to: processing
    event: process
  - from: processing
    to: done
    event: complete
`
	def, err := ParseDefinitionYAML(yaml)
	if err != nil {
		t.Fatalf("ParseDefinitionYAML: %v", err)
	}
	if def.Name != "Order Flow" {
		t.Errorf("Name = %q", def.Name)
	}
	if len(def.States) != 3 {
		t.Errorf("States = %d, want 3", len(def.States))
	}
}

func TestValidateDefinition(t *testing.T) {
	tests := []struct {
		name    string
		def     StateMachineDefinition
		wantErr bool
	}{
		{"valid", StateMachineDefinition{Name: "x", InitialState: "a", States: []StateDefinition{{Name: "a"}}, Transitions: []TransitionDef{}}, false},
		{"no name", StateMachineDefinition{InitialState: "a", States: []StateDefinition{{Name: "a"}}}, true},
		{"no states", StateMachineDefinition{Name: "x", InitialState: "a"}, true},
		{"bad initial", StateMachineDefinition{Name: "x", InitialState: "missing", States: []StateDefinition{{Name: "a"}}}, true},
		{"bad transition from", StateMachineDefinition{Name: "x", InitialState: "a", States: []StateDefinition{{Name: "a"}}, Transitions: []TransitionDef{{From: "b", To: "a", Event: "e"}}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateDefinition(&tt.def); (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestInstanceTransition(t *testing.T) {
	e := NewEngine(slog.Default())

	// Use builtin order flow
	inst, err := e.CreateInstance("sm_order_flow", nil)
	if err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}
	if inst.CurrentState != "created" {
		t.Errorf("CurrentState = %q, want created", inst.CurrentState)
	}

	// Valid transition
	inst, err = e.Transition(inst.ID, "validate", "agent-1")
	if err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if inst.CurrentState != "validated" {
		t.Errorf("CurrentState = %q, want validated", inst.CurrentState)
	}

	// Invalid transition
	_, err = e.Transition(inst.ID, "fulfill", "agent-1")
	if err == nil {
		t.Error("expected error for invalid transition")
	}
}

func TestTerminalState(t *testing.T) {
	e := NewEngine(slog.Default())
	inst, _ := e.CreateInstance("sm_order_flow", nil)
	_, _ = e.Transition(inst.ID, "cancel", "agent-1")

	_, err := e.Transition(inst.ID, "validate", "agent-1")
	if err == nil {
		t.Error("expected error for transition from terminal state")
	}
}

func TestValidTransitions(t *testing.T) {
	e := NewEngine(slog.Default())
	inst, _ := e.CreateInstance("sm_order_flow", nil)

	valid, err := e.ValidTransitions(inst.ID)
	if err != nil {
		t.Fatalf("ValidTransitions: %v", err)
	}
	if len(valid) != 2 { // validate, cancel
		t.Errorf("ValidTransitions = %d, want 2", len(valid))
	}
}

func TestSagaExecution(t *testing.T) {
	e := NewEngine(slog.Default())
	inst, _ := e.CreateInstance("sm_order_flow", nil)

	// Start saga
	inst, err := e.StartSaga(inst.ID)
	if err != nil {
		t.Fatalf("StartSaga: %v", err)
	}
	if inst.SagaState.Status != "running" {
		t.Errorf("SagaState.Status = %q, want running", inst.SagaState.Status)
	}

	// Complete first step
	inst, err = e.CompleteSagaStep(inst.ID, true, "")
	if err != nil {
		t.Fatalf("CompleteSagaStep: %v", err)
	}
	if len(inst.SagaState.CompletedSteps) != 1 {
		t.Errorf("CompletedSteps = %d, want 1", len(inst.SagaState.CompletedSteps))
	}

	// Fail second step
	inst, err = e.CompleteSagaStep(inst.ID, false, "payment failed")
	if err != nil {
		t.Fatalf("CompleteSagaStep: %v", err)
	}
	if inst.SagaState.Status != "compensating" {
		t.Errorf("SagaState.Status = %q, want compensating", inst.SagaState.Status)
	}
}

func TestListMachines(t *testing.T) {
	e := NewEngine(slog.Default())
	machines := e.ListMachines()
	if len(machines) == 0 {
		t.Error("expected at least one builtin machine")
	}
}

// ===================== CreateMachine tests =====================

func TestCreateMachine_FromYAML(t *testing.T) {
	e := NewEngine(slog.Default())
	yaml := `
name: "Test Machine"
topic: "test-events"
initial_state: "start"
states:
  - name: start
  - name: end
    terminal: true
transitions:
  - from: start
    to: end
    event: finish
`
	def, err := e.CreateMachine(yaml, "test-agent")
	if err != nil {
		t.Fatalf("CreateMachine: %v", err)
	}
	if def.Name != "Test Machine" {
		t.Errorf("Name = %q", def.Name)
	}
	if def.CreatedBy != "test-agent" {
		t.Errorf("CreatedBy = %q", def.CreatedBy)
	}
	if def.ID == "" {
		t.Error("expected non-empty ID")
	}
}

func TestCreateMachine_InvalidYAML(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.CreateMachine("{{{{invalid yaml", "agent")
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestCreateMachine_MissingRequiredFields(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.CreateMachine(`
states:
  - name: s1
initial_state: "s1"
`, "agent")
	if err == nil {
		t.Error("expected error for missing name")
	}
}

func TestCreateMachine_MissingStates(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.CreateMachine(`
name: "No States"
initial_state: "s1"
`, "agent")
	if err == nil {
		t.Error("expected error for missing states")
	}
}

func TestCreateMachine_MissingInitialState(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.CreateMachine(`
name: "No Initial"
states:
  - name: s1
`, "agent")
	if err == nil {
		t.Error("expected error for missing initial_state")
	}
}

func TestCreateMachine_InvalidInitialState(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.CreateMachine(`
name: "Bad Initial"
initial_state: "nonexistent"
states:
  - name: s1
`, "agent")
	if err == nil {
		t.Error("expected error for initial_state not in states")
	}
}

// ===================== GetMachine tests =====================

func TestGetMachine_Existing(t *testing.T) {
	e := NewEngine(slog.Default())
	m, err := e.GetMachine("sm_order_flow")
	if err != nil {
		t.Fatalf("GetMachine: %v", err)
	}
	if m.Name != "Order Processing" {
		t.Errorf("Name = %q", m.Name)
	}
}

func TestGetMachine_NonExistent(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.GetMachine("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent machine")
	}
}

// ===================== GetInstance tests =====================

func TestGetInstance_Existing(t *testing.T) {
	e := NewEngine(slog.Default())
	inst, _ := e.CreateInstance("sm_order_flow", map[string]any{"order_id": "123"})

	got, err := e.GetInstance(inst.ID)
	if err != nil {
		t.Fatalf("GetInstance: %v", err)
	}
	if got.ID != inst.ID {
		t.Errorf("ID = %q, want %q", got.ID, inst.ID)
	}
	if got.CurrentState != "created" {
		t.Errorf("CurrentState = %q, want created", got.CurrentState)
	}
	if len(got.History) == 0 {
		t.Error("expected non-empty history")
	}
	if got.Data["order_id"] != "123" {
		t.Error("expected data to contain order_id")
	}
}

func TestGetInstance_NonExistent(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.GetInstance("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent instance")
	}
}

// ===================== ListInstances tests =====================

func TestListInstances_FilterByMachineID(t *testing.T) {
	e := NewEngine(slog.Default())
	_, _ = e.CreateInstance("sm_order_flow", nil)
	_, _ = e.CreateInstance("sm_order_flow", nil)

	instances := e.ListInstances("sm_order_flow")
	if len(instances) != 2 {
		t.Errorf("expected 2 instances, got %d", len(instances))
	}
}

func TestListInstances_EmptyResults(t *testing.T) {
	e := NewEngine(slog.Default())
	instances := e.ListInstances("nonexistent")
	if len(instances) != 0 {
		t.Errorf("expected 0 instances, got %d", len(instances))
	}
}

func TestListInstances_AllInstances(t *testing.T) {
	e := NewEngine(slog.Default())
	_, _ = e.CreateInstance("sm_order_flow", nil)
	_, _ = e.CreateInstance("sm_order_flow", nil)

	// Empty machineID returns all
	instances := e.ListInstances("")
	if len(instances) < 2 {
		t.Errorf("expected at least 2 instances, got %d", len(instances))
	}
}

func TestListInstances_DifferentStates(t *testing.T) {
	e := NewEngine(slog.Default())
	inst1, _ := e.CreateInstance("sm_order_flow", nil)
	inst2, _ := e.CreateInstance("sm_order_flow", nil)

	// Transition one
	_, _ = e.Transition(inst1.ID, "validate", "agent-1")

	instances := e.ListInstances("sm_order_flow")
	states := make(map[string]int)
	for _, inst := range instances {
		states[inst.CurrentState]++
	}
	_ = inst2

	if states["created"] < 1 || states["validated"] < 1 {
		t.Errorf("expected different states, got: %v", states)
	}
}

// ===================== ParseDefinitionYAML extended tests =====================

func TestParseDefinitionYAML_MalformedYAML(t *testing.T) {
	_, err := ParseDefinitionYAML("{{{{not yaml")
	if err == nil {
		t.Error("expected error for malformed YAML")
	}
}

func TestParseDefinitionYAML_MissingStates(t *testing.T) {
	_, err := ParseDefinitionYAML(`
name: "Test"
initial_state: "s1"
`)
	if err == nil {
		t.Error("expected error for missing states")
	}
}

func TestParseDefinitionYAML_MissingInitialState(t *testing.T) {
	_, err := ParseDefinitionYAML(`
name: "Test"
states:
  - name: s1
`)
	if err == nil {
		t.Error("expected error for missing initial_state")
	}
}

func TestParseDefinitionYAML_TransitionToUnknownState(t *testing.T) {
	_, err := ParseDefinitionYAML(`
name: "Test"
initial_state: "s1"
states:
  - name: s1
transitions:
  - from: s1
    to: unknown
    event: go
`)
	if err == nil {
		t.Error("expected error for transition to unknown state")
	}
}

func TestParseDefinitionYAML_EmptyTransitionEvent(t *testing.T) {
	_, err := ParseDefinitionYAML(`
name: "Test"
initial_state: "s1"
states:
  - name: s1
  - name: s2
transitions:
  - from: s1
    to: s2
`)
	if err == nil {
		t.Error("expected error for empty transition event")
	}
}

// ===================== CreateInstance tests =====================

func TestCreateInstance_NonExistentMachine(t *testing.T) {
	e := NewEngine(slog.Default())
	_, err := e.CreateInstance("nonexistent", nil)
	if err == nil {
		t.Error("expected error for non-existent machine")
	}
}

func TestCreateInstance_WithData(t *testing.T) {
	e := NewEngine(slog.Default())
	inst, err := e.CreateInstance("sm_order_flow", map[string]any{"order_id": "abc"})
	if err != nil {
		t.Fatalf("CreateInstance: %v", err)
	}
	if inst.Data["order_id"] != "abc" {
		t.Error("expected data to be preserved")
	}
	if inst.CurrentState != "created" {
		t.Errorf("expected initial state, got %q", inst.CurrentState)
	}
}

// ===================== Saga extended tests =====================

func TestStartSaga_NoSagaDefined(t *testing.T) {
	e := NewEngine(slog.Default())

	// Create a machine without saga
	_, _ = e.CreateMachine(`
name: "No Saga"
initial_state: "s1"
states:
  - name: s1
`, "agent")

	machines := e.ListMachines()
	var noSagaID string
	for _, m := range machines {
		if m.Name == "No Saga" {
			noSagaID = m.ID
			break
		}
	}

	inst, _ := e.CreateInstance(noSagaID, nil)
	_, err := e.StartSaga(inst.ID)
	if err == nil {
		t.Error("expected error for machine without saga")
	}
}

func TestCompleteSagaStep_AllSteps(t *testing.T) {
	e := NewEngine(slog.Default())
	inst, _ := e.CreateInstance("sm_order_flow", nil)
	_, _ = e.StartSaga(inst.ID)

	// Complete all 3 steps
	for i := 0; i < 3; i++ {
		inst, err := e.CompleteSagaStep(inst.ID, true, "")
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if i == 2 && inst.SagaState.Status != "completed" {
			t.Errorf("expected completed after all steps, got %q", inst.SagaState.Status)
		}
	}
}

func TestCompleteSagaStep_NoActiveSaga(t *testing.T) {
	e := NewEngine(slog.Default())
	inst, _ := e.CreateInstance("sm_order_flow", nil)

	_, err := e.CompleteSagaStep(inst.ID, true, "")
	if err == nil {
		t.Error("expected error for no active saga")
	}
}
