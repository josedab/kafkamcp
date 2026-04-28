package orchestration

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// AgentRole defines what an agent specializes in.
type AgentRole string

const (
	RoleMonitor    AgentRole = "monitor"
	RoleOperator   AgentRole = "operator"
	RoleAnalyst    AgentRole = "analyst"
	RoleSecurity   AgentRole = "security"
	RolePipeline   AgentRole = "pipeline"
	RoleCoordinator AgentRole = "coordinator"
)

// OrchestratedAgent represents an agent participating in orchestration.
type OrchestratedAgent struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Role         AgentRole `json:"role"`
	Capabilities []string  `json:"capabilities"`
	Status       string    `json:"status"` // idle, busy, offline
	CurrentTask  string    `json:"current_task,omitempty"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	TasksCompleted int64   `json:"tasks_completed"`
}

// Orchestration represents a multi-agent orchestration session.
type Orchestration struct {
	ID          string                `json:"id"`
	Name        string                `json:"name"`
	Description string                `json:"description,omitempty"`
	Agents      []string              `json:"agents"`
	Steps       []OrchestrationStep   `json:"steps"`
	Context     map[string]any        `json:"context"`
	Status      string                `json:"status"` // pending, running, completed, failed
	CreatedBy   string                `json:"created_by"`
	CreatedAt   time.Time             `json:"created_at"`
	CompletedAt *time.Time            `json:"completed_at,omitempty"`
	Results     []StepResult          `json:"results,omitempty"`
}

// OrchestrationStep defines a step in a multi-agent orchestration.
type OrchestrationStep struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	AssignTo     string   `json:"assign_to,omitempty"`      // specific agent or role
	AssignRole   string   `json:"assign_role,omitempty"`    // role-based assignment
	Tool         string   `json:"tool"`
	Args         map[string]any `json:"args,omitempty"`
	DependsOn    []string `json:"depends_on,omitempty"`
	Condition    string   `json:"condition,omitempty"`
	Timeout      string   `json:"timeout,omitempty"`
	OnFailure    string   `json:"on_failure,omitempty"` // skip, abort, retry
	MaxRetries   int      `json:"max_retries,omitempty"`
}

// StepResult captures the outcome of an orchestration step.
type StepResult struct {
	StepID      string    `json:"step_id"`
	AgentID     string    `json:"agent_id"`
	Status      string    `json:"status"` // pending, running, completed, failed, skipped
	Output      any       `json:"output,omitempty"`
	Error       string    `json:"error,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
	Retries     int       `json:"retries"`
}

// Handoff represents a task handoff between agents.
type Handoff struct {
	ID          string         `json:"id"`
	FromAgent   string         `json:"from_agent"`
	ToAgent     string         `json:"to_agent"`
	Context     map[string]any `json:"context"`
	Message     string         `json:"message"`
	Status      string         `json:"status"` // pending, accepted, rejected, completed
	CreatedAt   time.Time      `json:"created_at"`
}

// Engine orchestrates multi-agent Kafka operations.
type Engine struct {
	mu             sync.RWMutex
	agents         map[string]*OrchestratedAgent
	orchestrations map[string]*Orchestration
	handoffs       map[string]*Handoff
	sharedContext  map[string]map[string]any // orchestrationID -> shared context
	logger         *slog.Logger
	nextID         int64
}

// NewEngine creates a new orchestration engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		agents:         make(map[string]*OrchestratedAgent),
		orchestrations: make(map[string]*Orchestration),
		handoffs:       make(map[string]*Handoff),
		sharedContext:  make(map[string]map[string]any),
		logger:         logger,
	}
}

// RegisterAgent registers an agent for orchestration.
func (e *Engine) RegisterAgent(id, name string, role AgentRole, capabilities []string) (*OrchestratedAgent, error) {
	if id == "" || name == "" {
		return nil, fmt.Errorf("agent id and name are required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	agent := &OrchestratedAgent{
		ID:           id,
		Name:         name,
		Role:         role,
		Capabilities: capabilities,
		Status:       "idle",
		LastHeartbeat: time.Now(),
	}
	e.agents[id] = agent
	e.logger.Info("agent registered for orchestration", "id", id, "role", role)
	return agent, nil
}

// ListAgents returns all registered agents.
func (e *Engine) ListAgents() []OrchestratedAgent {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]OrchestratedAgent, 0, len(e.agents))
	for _, a := range e.agents {
		result = append(result, *a)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// CreateOrchestration creates a new multi-agent orchestration.
func (e *Engine) CreateOrchestration(name, description, createdBy string, steps []OrchestrationStep) (*Orchestration, error) {
	if name == "" {
		return nil, fmt.Errorf("orchestration name is required")
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("at least one step is required")
	}

	// Validate steps
	stepIDs := make(map[string]bool)
	for _, s := range steps {
		if s.ID == "" || s.Name == "" || s.Tool == "" {
			return nil, fmt.Errorf("step id, name, and tool are required")
		}
		if stepIDs[s.ID] {
			return nil, fmt.Errorf("duplicate step ID %q", s.ID)
		}
		stepIDs[s.ID] = true
	}
	for _, s := range steps {
		for _, dep := range s.DependsOn {
			if !stepIDs[dep] {
				return nil, fmt.Errorf("step %q depends on unknown step %q", s.ID, dep)
			}
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("orch_%d", e.nextID)

	// Collect participating agents
	agentSet := make(map[string]bool)
	for _, s := range steps {
		if s.AssignTo != "" {
			agentSet[s.AssignTo] = true
		}
	}
	agents := make([]string, 0, len(agentSet))
	for a := range agentSet {
		agents = append(agents, a)
	}

	orch := &Orchestration{
		ID:          id,
		Name:        name,
		Description: description,
		Agents:      agents,
		Steps:       steps,
		Context:     make(map[string]any),
		Status:      "pending",
		CreatedBy:   createdBy,
		CreatedAt:   time.Now(),
	}

	// Initialize step results
	for _, s := range steps {
		orch.Results = append(orch.Results, StepResult{StepID: s.ID, Status: "pending"})
	}

	e.orchestrations[id] = orch
	e.sharedContext[id] = make(map[string]any)
	e.logger.Info("orchestration created", "id", id, "name", name, "steps", len(steps))
	return orch, nil
}

// GetReadySteps returns steps that are ready to execute (all dependencies satisfied).
func (e *Engine) GetReadySteps(orchID string) ([]OrchestrationStep, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	orch, ok := e.orchestrations[orchID]
	if !ok {
		return nil, fmt.Errorf("orchestration %q not found", orchID)
	}

	completedSteps := make(map[string]bool)
	for _, r := range orch.Results {
		if r.Status == "completed" || r.Status == "skipped" {
			completedSteps[r.StepID] = true
		}
	}

	var ready []OrchestrationStep
	for _, step := range orch.Steps {
		if completedSteps[step.ID] {
			continue
		}
		// Check if all dependencies are satisfied
		depsOk := true
		for _, dep := range step.DependsOn {
			if !completedSteps[dep] {
				depsOk = false
				break
			}
		}
		if depsOk {
			ready = append(ready, step)
		}
	}
	return ready, nil
}

// CompleteStep records the result of a step and auto-assigns agents.
func (e *Engine) CompleteStep(orchID, stepID, agentID, status string, output any, errMsg string) error {
	validStatuses := map[string]bool{"completed": true, "failed": true, "skipped": true}
	if !validStatuses[status] {
		return fmt.Errorf("invalid status %q", status)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	orch, ok := e.orchestrations[orchID]
	if !ok {
		return fmt.Errorf("orchestration %q not found", orchID)
	}

	for i, r := range orch.Results {
		if r.StepID == stepID {
			orch.Results[i].AgentID = agentID
			orch.Results[i].Status = status
			orch.Results[i].Output = output
			orch.Results[i].Error = errMsg
			orch.Results[i].CompletedAt = time.Now()

			if agent, ok := e.agents[agentID]; ok {
				agent.Status = "idle"
				agent.CurrentTask = ""
				agent.TasksCompleted++
			}
			break
		}
	}

	// Store output in shared context
	if output != nil {
		ctx := e.sharedContext[orchID]
		ctx[stepID] = output
	}

	// Update orchestration status
	if orch.Status == "pending" {
		orch.Status = "running"
	}

	allDone := true
	anyFailed := false
	for _, r := range orch.Results {
		if r.Status == "pending" || r.Status == "running" {
			allDone = false
		}
		if r.Status == "failed" {
			anyFailed = true
		}
	}
	if allDone {
		now := time.Now()
		orch.CompletedAt = &now
		if anyFailed {
			orch.Status = "failed"
		} else {
			orch.Status = "completed"
		}
	}

	return nil
}

// CreateHandoff creates a task handoff between agents.
func (e *Engine) CreateHandoff(fromAgent, toAgent, message string, context map[string]any) (*Handoff, error) {
	if fromAgent == "" || toAgent == "" {
		return nil, fmt.Errorf("from_agent and to_agent are required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("handoff_%d", e.nextID)

	handoff := &Handoff{
		ID:        id,
		FromAgent: fromAgent,
		ToAgent:   toAgent,
		Context:   context,
		Message:   message,
		Status:    "pending",
		CreatedAt: time.Now(),
	}
	e.handoffs[id] = handoff
	return handoff, nil
}

// AcceptHandoff marks a handoff as accepted.
func (e *Engine) AcceptHandoff(id, agentID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	h, ok := e.handoffs[id]
	if !ok {
		return fmt.Errorf("handoff %q not found", id)
	}
	if h.ToAgent != agentID {
		return fmt.Errorf("handoff %q is not assigned to agent %q", id, agentID)
	}
	h.Status = "accepted"
	return nil
}

// GetOrchestration returns an orchestration by ID.
func (e *Engine) GetOrchestration(id string) (*Orchestration, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	orch, ok := e.orchestrations[id]
	if !ok {
		return nil, fmt.Errorf("orchestration %q not found", id)
	}
	return orch, nil
}

// ListOrchestrations returns all orchestrations.
func (e *Engine) ListOrchestrations() []Orchestration {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]Orchestration, 0, len(e.orchestrations))
	for _, o := range e.orchestrations {
		result = append(result, *o)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	return result
}

// ListHandoffs returns handoffs for a specific agent.
func (e *Engine) ListHandoffs(agentID string) []Handoff {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Handoff
	for _, h := range e.handoffs {
		if agentID == "" || h.FromAgent == agentID || h.ToAgent == agentID {
			result = append(result, *h)
		}
	}
	return result
}

// GetSharedContext returns the shared context for an orchestration.
func (e *Engine) GetSharedContext(orchID string) (map[string]any, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	ctx, ok := e.sharedContext[orchID]
	if !ok {
		return nil, fmt.Errorf("orchestration %q not found", orchID)
	}
	return ctx, nil
}

// UpdateSharedContext updates a value in shared context.
func (e *Engine) UpdateSharedContext(orchID, key string, value any) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	ctx, ok := e.sharedContext[orchID]
	if !ok {
		return fmt.Errorf("orchestration %q not found", orchID)
	}
	ctx[key] = value
	return nil
}

// AgentHeartbeat updates an agent's heartbeat and status.
func (e *Engine) AgentHeartbeat(agentID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	agent, ok := e.agents[agentID]
	if !ok {
		return fmt.Errorf("agent %q not found", agentID)
	}
	agent.LastHeartbeat = time.Now()
	return nil
}

// FindAgentByRole returns agents matching a role.
func (e *Engine) FindAgentByRole(role AgentRole) []OrchestratedAgent {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []OrchestratedAgent
	for _, a := range e.agents {
		if a.Role == role {
			result = append(result, *a)
		}
	}
	return result
}
