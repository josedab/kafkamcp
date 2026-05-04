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
	e.Transition(inst.ID, "cancel", "agent-1")

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
