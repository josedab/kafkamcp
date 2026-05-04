package dagworkflow

import (
	"fmt"
	"time"
)

// AgentAssignment assigns a DAG step to a specific agent.
type AgentAssignment struct {
	StepID    string `json:"step_id"`
	AgentID   string `json:"agent_id"`
	AgentRole string `json:"agent_role,omitempty"`
}

// HandoffRecord records a handoff between agents.
type HandoffRecord struct {
	FromAgent string    `json:"from_agent"`
	ToAgent   string    `json:"to_agent"`
	StepID    string    `json:"step_id"`
	Data      any       `json:"data,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// MultiAgentRun extends WorkflowRun with multi-agent coordination.
type MultiAgentRun struct {
	RunID       string            `json:"run_id"`
	Assignments []AgentAssignment `json:"assignments"`
	Handoffs    []HandoffRecord   `json:"handoffs"`
	AgentStatus map[string]string `json:"agent_status"` // agentID -> "idle", "working", "done"
}

// AssignSteps assigns agents to steps in a running workflow.
func (e *Engine) AssignSteps(runID string, assignments []AgentAssignment) (*MultiAgentRun, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	run, ok := e.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %q not found", runID)
	}

	// Validate all step IDs exist
	for _, a := range assignments {
		if _, ok := run.StepResults[a.StepID]; !ok {
			return nil, fmt.Errorf("step %q not found in run %q", a.StepID, runID)
		}
		if a.AgentID == "" {
			return nil, fmt.Errorf("agent_id is required for step %q", a.StepID)
		}
	}

	mar := e.getOrCreateMultiAgentRun(runID)
	mar.Assignments = assignments

	// Initialize agent status
	mar.AgentStatus = make(map[string]string)
	for _, a := range assignments {
		mar.AgentStatus[a.AgentID] = "idle"
	}

	return mar, nil
}

// RecordHandoff records a data handoff between two agents.
func (e *Engine) RecordHandoff(runID, fromAgent, toAgent, stepID string, data any) (*HandoffRecord, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.runs[runID]; !ok {
		return nil, fmt.Errorf("run %q not found", runID)
	}
	if fromAgent == "" || toAgent == "" {
		return nil, fmt.Errorf("from_agent and to_agent are required")
	}

	mar := e.getOrCreateMultiAgentRun(runID)
	handoff := HandoffRecord{
		FromAgent: fromAgent,
		ToAgent:   toAgent,
		StepID:    stepID,
		Data:      data,
		Timestamp: time.Now(),
	}
	mar.Handoffs = append(mar.Handoffs, handoff)

	// Update agent statuses
	mar.AgentStatus[fromAgent] = "idle"
	mar.AgentStatus[toAgent] = "working"

	return &handoff, nil
}

// GetAgentSteps returns the steps assigned to a specific agent.
func (e *Engine) GetAgentSteps(runID, agentID string) ([]DAGStep, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	run, ok := e.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %q not found", runID)
	}

	tmpl, ok := e.templates[run.TemplateID]
	if !ok {
		return nil, fmt.Errorf("template %q not found", run.TemplateID)
	}

	mar := e.multiAgentRuns[runID]
	if mar == nil {
		return nil, fmt.Errorf("no agent assignments for run %q", runID)
	}

	// Find step IDs assigned to this agent
	assignedSteps := make(map[string]bool)
	for _, a := range mar.Assignments {
		if a.AgentID == agentID {
			assignedSteps[a.StepID] = true
		}
	}

	var result []DAGStep
	for _, step := range tmpl.Steps {
		if assignedSteps[step.ID] {
			result = append(result, step)
		}
	}
	return result, nil
}

// GetMultiAgentStatus returns the multi-agent coordination status for a run.
func (e *Engine) GetMultiAgentStatus(runID string) (*MultiAgentRun, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	mar, ok := e.multiAgentRuns[runID]
	if !ok {
		return nil, fmt.Errorf("no multi-agent data for run %q", runID)
	}
	return mar, nil
}

// GetReadyAgentSteps returns steps that are ready to execute AND assigned to the given agent.
func (e *Engine) GetReadyAgentSteps(runID, agentID string) ([]DAGStep, error) {
	ready, err := e.GetReadySteps(runID)
	if err != nil {
		return nil, err
	}

	e.mu.RLock()
	mar := e.multiAgentRuns[runID]
	e.mu.RUnlock()

	if mar == nil {
		return ready, nil // No assignments, return all ready steps
	}

	assignedSteps := make(map[string]bool)
	for _, a := range mar.Assignments {
		if a.AgentID == agentID {
			assignedSteps[a.StepID] = true
		}
	}

	var result []DAGStep
	for _, step := range ready {
		if assignedSteps[step.ID] {
			result = append(result, step)
		}
	}
	return result, nil
}

func (e *Engine) getOrCreateMultiAgentRun(runID string) *MultiAgentRun {
	if e.multiAgentRuns == nil {
		e.multiAgentRuns = make(map[string]*MultiAgentRun)
	}
	mar, ok := e.multiAgentRuns[runID]
	if !ok {
		mar = &MultiAgentRun{
			RunID:       runID,
			AgentStatus: make(map[string]string),
		}
		e.multiAgentRuns[runID] = mar
	}
	return mar
}
