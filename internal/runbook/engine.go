package runbook

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Runbook defines a multi-step operational procedure.
type Runbook struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	Category    string     `json:"category,omitempty"`
	Steps       []Step     `json:"steps"`
	Params      []ParamDef `json:"params,omitempty"`
	Timeout     string     `json:"timeout,omitempty"`
	Rollback    []Step     `json:"rollback,omitempty"`
	CreatedBy   string     `json:"created_by"`
	CreatedAt   time.Time  `json:"created_at"`
	Enabled     bool       `json:"enabled"`
}

// Step defines a single action in a runbook.
type Step struct {
	Order       int               `json:"order"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Tool        string            `json:"tool"`
	Args        map[string]string `json:"args,omitempty"`
	Condition   string            `json:"condition,omitempty"`
	OnFailure   string            `json:"on_failure,omitempty"` // continue, abort, rollback
	Retries     int               `json:"retries,omitempty"`
	DelayBefore string            `json:"delay_before,omitempty"`
}

// ParamDef defines a parameter for runbook execution.
type ParamDef struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required"`
	Default     string `json:"default,omitempty"`
}

// Execution tracks a runbook run.
type Execution struct {
	ID          string            `json:"id"`
	RunbookID   string            `json:"runbook_id"`
	RunbookName string            `json:"runbook_name"`
	Params      map[string]string `json:"params,omitempty"`
	Steps       []StepResult      `json:"steps"`
	Status      string            `json:"status"` // pending, running, completed, failed, aborted, rolling_back
	StartedAt   time.Time         `json:"started_at"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	ExecutedBy  string            `json:"executed_by"`
	AuditLog    []AuditEntry      `json:"audit_log"`
}

// StepResult tracks a single step execution.
type StepResult struct {
	StepName  string    `json:"step_name"`
	Tool      string    `json:"tool"`
	Status    string    `json:"status"` // pending, running, completed, failed, skipped
	Output    any       `json:"output,omitempty"`
	Error     string    `json:"error,omitempty"`
	StartedAt time.Time `json:"started_at,omitempty"`
	Duration  string    `json:"duration,omitempty"`
	Attempt   int       `json:"attempt"`
}

// AuditEntry records a timestamped log of execution events.
type AuditEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Event     string    `json:"event"`
	Details   string    `json:"details,omitempty"`
}

// Engine manages runbooks and their execution.
type Engine struct {
	mu                sync.RWMutex
	runbooks          map[string]*Runbook
	executions        map[string]*Execution
	agenticRunbooks   map[string]*AgenticRunbook
	agenticExecutions map[string]*AgenticExecution
	logger            *slog.Logger
	nextID            int64
}

// NewEngine creates a new runbook engine with built-in runbooks.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		runbooks:          make(map[string]*Runbook),
		executions:        make(map[string]*Execution),
		agenticRunbooks:   make(map[string]*AgenticRunbook),
		agenticExecutions: make(map[string]*AgenticExecution),
		logger:            logger,
	}
	e.seedBuiltins()
	return e
}

func (e *Engine) seedBuiltins() {
	builtins := []Runbook{
		{
			ID: "rb_partition_reassign", Name: "Partition Reassignment",
			Description: "Reassign partitions across brokers for load balancing",
			Category:    "operations",
			Steps: []Step{
				{Order: 1, Name: "Verify cluster state", Tool: "kafka_health_score", Description: "Check cluster health before reassignment"},
				{Order: 2, Name: "List current assignments", Tool: "kafka_describe_topic", Description: "Capture current partition layout"},
				{Order: 3, Name: "Plan reassignment", Tool: "kafka_cluster_info", Args: map[string]string{"action": "reassignment_plan"}, Description: "Generate reassignment plan"},
				{Order: 4, Name: "Verify ISR state", Tool: "kafka_health_score", Description: "Verify all partitions are in sync"},
				{Order: 5, Name: "Log completion", Tool: "kafka_audit_trail", Description: "Record reassignment in audit log"},
			},
			Rollback: []Step{
				{Order: 1, Name: "Revert assignments", Tool: "kafka_cluster_info", Args: map[string]string{"action": "revert"}, Description: "Revert to previous assignments"},
			},
			Enabled: true, CreatedBy: "system", CreatedAt: time.Now(),
		},
		{
			ID: "rb_topic_migration", Name: "Topic Migration",
			Description: "Migrate a topic from one cluster to another",
			Category:    "migration",
			Params: []ParamDef{
				{Name: "source_topic", Required: true, Description: "Source topic name"},
				{Name: "target_cluster", Required: true, Description: "Target cluster name"},
			},
			Steps: []Step{
				{Order: 1, Name: "Describe source topic", Tool: "kafka_describe_topic", Description: "Capture source topic configuration"},
				{Order: 2, Name: "Create target topic", Tool: "kafka_create_topic", Description: "Create topic on target cluster", OnFailure: "abort"},
				{Order: 3, Name: "Verify target topic", Tool: "kafka_describe_topic", Description: "Verify target topic was created correctly"},
				{Order: 4, Name: "Start replication", Tool: "kafka_cluster_info", Description: "Set up cross-cluster replication"},
				{Order: 5, Name: "Verify sync", Tool: "kafka_health_score", Description: "Verify data is in sync"},
			},
			Enabled: true, CreatedBy: "system", CreatedAt: time.Now(),
		},
		{
			ID: "rb_rolling_restart_check", Name: "Rolling Restart Verification",
			Description: "Verify cluster health during rolling restart",
			Category:    "maintenance",
			Steps: []Step{
				{Order: 1, Name: "Pre-check health", Tool: "kafka_health_score", Description: "Check cluster health before restart"},
				{Order: 2, Name: "Check under-replicated", Tool: "kafka_list_topics", Description: "Check for under-replicated partitions"},
				{Order: 3, Name: "Check consumer groups", Tool: "kafka_list_consumer_groups", Description: "Verify all consumer groups are stable"},
				{Order: 4, Name: "Post-check health", Tool: "kafka_health_score", Description: "Verify health after restart"},
			},
			Enabled: true, CreatedBy: "system", CreatedAt: time.Now(),
		},
	}
	for i := range builtins {
		e.runbooks[builtins[i].ID] = &builtins[i]
	}
}

// CreateRunbook creates a custom runbook from a JSON definition.
func (e *Engine) CreateRunbook(definition string, createdBy string) (*Runbook, error) {
	var rb Runbook
	if err := json.Unmarshal([]byte(definition), &rb); err != nil {
		return nil, fmt.Errorf("invalid runbook JSON: %w", err)
	}

	if rb.Name == "" {
		return nil, fmt.Errorf("runbook name is required")
	}
	if len(rb.Steps) == 0 {
		return nil, fmt.Errorf("at least one step is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	rb.ID = fmt.Sprintf("rb_%d", e.nextID)
	rb.Enabled = true
	rb.CreatedBy = createdBy
	rb.CreatedAt = time.Now()

	for i := range rb.Steps {
		rb.Steps[i].Order = i + 1
	}

	e.runbooks[rb.ID] = &rb
	e.logger.Info("runbook created", "id", rb.ID, "name", rb.Name, "steps", len(rb.Steps))
	return &rb, nil
}

// Execute starts a runbook execution.
func (e *Engine) Execute(runbookID, executedBy string, params map[string]string) (*Execution, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	rb, ok := e.runbooks[runbookID]
	if !ok {
		return nil, fmt.Errorf("runbook %q not found", runbookID)
	}
	if !rb.Enabled {
		return nil, fmt.Errorf("runbook %q is disabled", runbookID)
	}

	// Validate required params
	for _, p := range rb.Params {
		if p.Required {
			if _, ok := params[p.Name]; !ok {
				if p.Default != "" {
					if params == nil {
						params = make(map[string]string)
					}
					params[p.Name] = p.Default
				} else {
					return nil, fmt.Errorf("required parameter %q is missing", p.Name)
				}
			}
		}
	}

	e.nextID++
	execID := fmt.Sprintf("exec_%d", e.nextID)

	exec := &Execution{
		ID:          execID,
		RunbookID:   runbookID,
		RunbookName: rb.Name,
		Params:      params,
		Status:      "running",
		StartedAt:   time.Now(),
		ExecutedBy:  executedBy,
		AuditLog: []AuditEntry{
			{Timestamp: time.Now(), Event: "execution_started", Details: fmt.Sprintf("Runbook %q started by %s", rb.Name, executedBy)},
		},
	}

	for _, step := range rb.Steps {
		exec.Steps = append(exec.Steps, StepResult{
			StepName: step.Name,
			Tool:     step.Tool,
			Status:   "pending",
		})
	}

	e.executions[execID] = exec
	return exec, nil
}

// CompleteStep marks a step as completed or failed.
func (e *Engine) CompleteStep(executionID string, stepIndex int, status string, output any, errMsg string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	exec, ok := e.executions[executionID]
	if !ok {
		return fmt.Errorf("execution %q not found", executionID)
	}
	if stepIndex < 0 || stepIndex >= len(exec.Steps) {
		return fmt.Errorf("invalid step index %d", stepIndex)
	}

	exec.Steps[stepIndex].Status = status
	exec.Steps[stepIndex].Output = output
	exec.Steps[stepIndex].Error = errMsg
	exec.Steps[stepIndex].Duration = time.Since(exec.StartedAt).String()

	exec.AuditLog = append(exec.AuditLog, AuditEntry{
		Timestamp: time.Now(),
		Event:     fmt.Sprintf("step_%s", status),
		Details:   fmt.Sprintf("Step %q %s", exec.Steps[stepIndex].StepName, status),
	})

	allDone := true
	anyFailed := false
	for _, s := range exec.Steps {
		if s.Status == "pending" || s.Status == "running" {
			allDone = false
		}
		if s.Status == "failed" {
			anyFailed = true
		}
	}
	if allDone {
		now := time.Now()
		exec.CompletedAt = &now
		if anyFailed {
			exec.Status = "failed"
		} else {
			exec.Status = "completed"
		}
		exec.AuditLog = append(exec.AuditLog, AuditEntry{
			Timestamp: now, Event: "execution_" + exec.Status, Details: fmt.Sprintf("Execution %s", exec.Status),
		})
	}

	return nil
}

// GetRunbook returns a runbook by ID.
func (e *Engine) GetRunbook(id string) (*Runbook, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rb, ok := e.runbooks[id]
	if !ok {
		return nil, fmt.Errorf("runbook %q not found", id)
	}
	return rb, nil
}

// ListRunbooks returns all runbooks.
func (e *Engine) ListRunbooks(category string) []Runbook {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Runbook
	for _, rb := range e.runbooks {
		if category == "" || rb.Category == category {
			result = append(result, *rb)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// GetExecution returns an execution by ID.
func (e *Engine) GetExecution(id string) (*Execution, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	exec, ok := e.executions[id]
	if !ok {
		return nil, fmt.Errorf("execution %q not found", id)
	}
	return exec, nil
}

// ListExecutions returns recent executions.
func (e *Engine) ListExecutions(runbookID string, limit int) []Execution {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var result []Execution
	for _, exec := range e.executions {
		if runbookID == "" || exec.RunbookID == runbookID {
			result = append(result, *exec)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].StartedAt.After(result[j].StartedAt) })
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

// DeleteRunbook removes a custom runbook.
func (e *Engine) DeleteRunbook(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.runbooks[id]; !ok {
		return fmt.Errorf("runbook %q not found", id)
	}
	delete(e.runbooks, id)
	return nil
}

// EnableRunbook enables or disables a runbook.
func (e *Engine) EnableRunbook(id string, enabled bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	rb, ok := e.runbooks[id]
	if !ok {
		return fmt.Errorf("runbook %q not found", id)
	}
	rb.Enabled = enabled
	return nil
}
