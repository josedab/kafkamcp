package statemachine

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// StateMachineDefinition defines a finite state machine in YAML.
type StateMachineDefinition struct {
	ID           string            `json:"id" yaml:"id"`
	Name         string            `json:"name" yaml:"name"`
	Description  string            `json:"description,omitempty" yaml:"description,omitempty"`
	Topic        string            `json:"topic" yaml:"topic"`
	InitialState string            `json:"initial_state" yaml:"initial_state"`
	States       []StateDefinition `json:"states" yaml:"states"`
	Transitions  []TransitionDef   `json:"transitions" yaml:"transitions"`
	Saga         *SagaDefinition   `json:"saga,omitempty" yaml:"saga,omitempty"`
	CreatedBy    string            `json:"created_by" yaml:"created_by"`
	CreatedAt    time.Time         `json:"created_at" yaml:"created_at"`
}

// StateDefinition defines a state in the FSM.
type StateDefinition struct {
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Terminal    bool   `json:"terminal,omitempty" yaml:"terminal,omitempty"`
	OnEnter     string `json:"on_enter,omitempty" yaml:"on_enter,omitempty"` // tool to execute
	OnExit      string `json:"on_exit,omitempty" yaml:"on_exit,omitempty"`   // tool to execute
}

// TransitionDef defines a valid state transition.
type TransitionDef struct {
	From     string            `json:"from" yaml:"from"`
	To       string            `json:"to" yaml:"to"`
	Event    string            `json:"event" yaml:"event"`
	Guard    string            `json:"guard,omitempty" yaml:"guard,omitempty"`   // condition
	Action   string            `json:"action,omitempty" yaml:"action,omitempty"` // tool to execute
	Metadata map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

// SagaDefinition defines saga compensation steps.
type SagaDefinition struct {
	Steps []SagaStep `json:"steps" yaml:"steps"`
}

// SagaStep defines a single step in a saga with compensation.
type SagaStep struct {
	Name         string `json:"name" yaml:"name"`
	Action       string `json:"action" yaml:"action"`
	Compensation string `json:"compensation" yaml:"compensation"`
	State        string `json:"state" yaml:"state"` // state to transition to on success
}

// Instance represents an active FSM instance.
type Instance struct {
	ID           string          `json:"id"`
	MachineID    string          `json:"machine_id"`
	MachineName  string          `json:"machine_name"`
	CurrentState string          `json:"current_state"`
	Data         map[string]any  `json:"data,omitempty"`
	History      []TransitionLog `json:"history"`
	SagaState    *SagaState      `json:"saga_state,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

// TransitionLog records a state transition.
type TransitionLog struct {
	From      string    `json:"from"`
	To        string    `json:"to"`
	Event     string    `json:"event"`
	Timestamp time.Time `json:"timestamp"`
	AgentID   string    `json:"agent_id,omitempty"`
}

// SagaState tracks saga execution progress.
type SagaState struct {
	Status         string   `json:"status"` // running, completed, compensating, failed
	CurrentStep    int      `json:"current_step"`
	CompletedSteps []string `json:"completed_steps"`
	FailedStep     string   `json:"failed_step,omitempty"`
	Error          string   `json:"error,omitempty"`
}

// Engine manages state machine definitions and instances.
type Engine struct {
	mu        sync.RWMutex
	machines  map[string]*StateMachineDefinition
	instances map[string]*Instance
	nextID    int64
	logger    *slog.Logger
}

// NewEngine creates a new state machine engine.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		machines:  make(map[string]*StateMachineDefinition),
		instances: make(map[string]*Instance),
		logger:    logger,
	}
	e.seedBuiltins()
	return e
}

func (e *Engine) seedBuiltins() {
	e.machines["sm_order_flow"] = &StateMachineDefinition{
		ID: "sm_order_flow", Name: "Order Processing",
		Description:  "Order lifecycle state machine",
		Topic:        "order-events",
		InitialState: "created",
		States: []StateDefinition{
			{Name: "created", Description: "Order created"},
			{Name: "validated", Description: "Order validated"},
			{Name: "payment_pending", Description: "Awaiting payment"},
			{Name: "paid", Description: "Payment received"},
			{Name: "fulfilled", Description: "Order fulfilled", Terminal: true},
			{Name: "canceled", Description: "Order canceled", Terminal: true},
		},
		Transitions: []TransitionDef{
			{From: "created", To: "validated", Event: "validate"},
			{From: "validated", To: "payment_pending", Event: "request_payment"},
			{From: "payment_pending", To: "paid", Event: "payment_received"},
			{From: "paid", To: "fulfilled", Event: "fulfill"},
			{From: "created", To: "canceled", Event: "cancel"},
			{From: "validated", To: "canceled", Event: "cancel"},
			{From: "payment_pending", To: "canceled", Event: "cancel"},
		},
		Saga: &SagaDefinition{
			Steps: []SagaStep{
				{Name: "reserve_inventory", Action: "kafka_produce", Compensation: "kafka_produce", State: "validated"},
				{Name: "charge_payment", Action: "kafka_produce", Compensation: "kafka_produce", State: "paid"},
				{Name: "ship_order", Action: "kafka_produce", Compensation: "kafka_produce", State: "fulfilled"},
			},
		},
		CreatedBy: "system", CreatedAt: time.Now(),
	}
}

// ParseDefinitionYAML parses a state machine definition from YAML.
func ParseDefinitionYAML(data string) (*StateMachineDefinition, error) {
	var def StateMachineDefinition
	if err := yaml.Unmarshal([]byte(data), &def); err != nil {
		return nil, fmt.Errorf("invalid state machine YAML: %w", err)
	}
	if err := validateDefinition(&def); err != nil {
		return nil, err
	}
	return &def, nil
}

func validateDefinition(def *StateMachineDefinition) error {
	if def.Name == "" {
		return fmt.Errorf("state machine name is required")
	}
	if len(def.States) == 0 {
		return fmt.Errorf("at least one state is required")
	}
	if def.InitialState == "" {
		return fmt.Errorf("initial_state is required")
	}

	stateSet := make(map[string]bool)
	for _, s := range def.States {
		if s.Name == "" {
			return fmt.Errorf("state name is required")
		}
		stateSet[s.Name] = true
	}

	if !stateSet[def.InitialState] {
		return fmt.Errorf("initial_state %q not found in states", def.InitialState)
	}

	for _, t := range def.Transitions {
		if !stateSet[t.From] {
			return fmt.Errorf("transition from unknown state %q", t.From)
		}
		if !stateSet[t.To] {
			return fmt.Errorf("transition to unknown state %q", t.To)
		}
		if t.Event == "" {
			return fmt.Errorf("transition event is required")
		}
	}

	return nil
}

// CreateMachine creates a new state machine definition.
func (e *Engine) CreateMachine(yamlDef string, createdBy string) (*StateMachineDefinition, error) {
	def, err := ParseDefinitionYAML(yamlDef)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	def.ID = fmt.Sprintf("sm_%d", e.nextID)
	def.CreatedBy = createdBy
	def.CreatedAt = time.Now()

	e.machines[def.ID] = def
	e.logger.Info("state machine created", "id", def.ID, "name", def.Name)
	return def, nil
}

// CreateInstance creates a new instance of a state machine.
func (e *Engine) CreateInstance(machineID string, data map[string]any) (*Instance, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	machine, ok := e.machines[machineID]
	if !ok {
		return nil, fmt.Errorf("state machine %q not found", machineID)
	}

	e.nextID++
	inst := &Instance{
		ID:           fmt.Sprintf("smi_%d", e.nextID),
		MachineID:    machineID,
		MachineName:  machine.Name,
		CurrentState: machine.InitialState,
		Data:         data,
		History: []TransitionLog{
			{From: "", To: machine.InitialState, Event: "init", Timestamp: time.Now()},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	e.instances[inst.ID] = inst
	return inst, nil
}

// Transition attempts a state transition on an instance.
func (e *Engine) Transition(instanceID, event, agentID string) (*Instance, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	inst, ok := e.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("instance %q not found", instanceID)
	}

	machine, ok := e.machines[inst.MachineID]
	if !ok {
		return nil, fmt.Errorf("machine %q not found", inst.MachineID)
	}

	// Check if current state is terminal
	for _, s := range machine.States {
		if s.Name == inst.CurrentState && s.Terminal {
			return nil, fmt.Errorf("cannot transition from terminal state %q", inst.CurrentState)
		}
	}

	// Find valid transition
	var transition *TransitionDef
	for i, t := range machine.Transitions {
		if t.From == inst.CurrentState && t.Event == event {
			transition = &machine.Transitions[i]
			break
		}
	}
	if transition == nil {
		return nil, fmt.Errorf("no valid transition from %q with event %q", inst.CurrentState, event)
	}

	// Execute transition
	from := inst.CurrentState
	inst.CurrentState = transition.To
	inst.UpdatedAt = time.Now()
	inst.History = append(inst.History, TransitionLog{
		From: from, To: transition.To, Event: event, Timestamp: time.Now(), AgentID: agentID,
	})

	return inst, nil
}

// ValidTransitions returns valid transitions for the current state of an instance.
func (e *Engine) ValidTransitions(instanceID string) ([]TransitionDef, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	inst, ok := e.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("instance %q not found", instanceID)
	}

	machine, ok := e.machines[inst.MachineID]
	if !ok {
		return nil, fmt.Errorf("machine %q not found", inst.MachineID)
	}

	var valid []TransitionDef
	for _, t := range machine.Transitions {
		if t.From == inst.CurrentState {
			valid = append(valid, t)
		}
	}
	return valid, nil
}

// StartSaga initiates a saga execution on an instance.
func (e *Engine) StartSaga(instanceID string) (*Instance, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	inst, ok := e.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("instance %q not found", instanceID)
	}

	machine, ok := e.machines[inst.MachineID]
	if !ok {
		return nil, fmt.Errorf("machine %q not found", inst.MachineID)
	}

	if machine.Saga == nil {
		return nil, fmt.Errorf("no saga defined for machine %q", inst.MachineID)
	}

	inst.SagaState = &SagaState{
		Status:      "running",
		CurrentStep: 0,
	}
	inst.UpdatedAt = time.Now()

	return inst, nil
}

// CompleteSagaStep marks a saga step as completed or failed.
func (e *Engine) CompleteSagaStep(instanceID string, success bool, errMsg string) (*Instance, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	inst, ok := e.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("instance %q not found", instanceID)
	}
	if inst.SagaState == nil {
		return nil, fmt.Errorf("no active saga for instance %q", instanceID)
	}

	machine := e.machines[inst.MachineID]
	if machine == nil || machine.Saga == nil {
		return nil, fmt.Errorf("machine or saga not found")
	}

	step := machine.Saga.Steps[inst.SagaState.CurrentStep]

	if success {
		inst.SagaState.CompletedSteps = append(inst.SagaState.CompletedSteps, step.Name)
		inst.SagaState.CurrentStep++

		// Transition to the step's target state
		if step.State != "" {
			inst.CurrentState = step.State
			inst.History = append(inst.History, TransitionLog{
				From: inst.CurrentState, To: step.State, Event: "saga_" + step.Name,
				Timestamp: time.Now(),
			})
		}

		if inst.SagaState.CurrentStep >= len(machine.Saga.Steps) {
			inst.SagaState.Status = "completed"
		}
	} else {
		inst.SagaState.Status = "compensating"
		inst.SagaState.FailedStep = step.Name
		inst.SagaState.Error = errMsg
	}

	inst.UpdatedAt = time.Now()
	return inst, nil
}

// GetMachine returns a state machine definition by ID.
func (e *Engine) GetMachine(id string) (*StateMachineDefinition, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	m, ok := e.machines[id]
	if !ok {
		return nil, fmt.Errorf("state machine %q not found", id)
	}
	return m, nil
}

// ListMachines returns all state machine definitions.
func (e *Engine) ListMachines() []StateMachineDefinition {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var result []StateMachineDefinition
	for _, m := range e.machines {
		result = append(result, *m)
	}
	return result
}

// GetInstance returns an instance by ID.
func (e *Engine) GetInstance(id string) (*Instance, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	inst, ok := e.instances[id]
	if !ok {
		return nil, fmt.Errorf("instance %q not found", id)
	}
	return inst, nil
}

// ListInstances returns all instances for a machine.
func (e *Engine) ListInstances(machineID string) []Instance {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var result []Instance
	for _, inst := range e.instances {
		if machineID == "" || inst.MachineID == machineID {
			result = append(result, *inst)
		}
	}
	return result
}
