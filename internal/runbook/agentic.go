package runbook

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// AgenticRunbook extends Runbook with approval gates, pre/post conditions,
// blast-radius limits, and dry-run support.
type AgenticRunbook struct {
	ID             string            `json:"id" yaml:"id"`
	Name           string            `json:"name" yaml:"name"`
	Description    string            `json:"description,omitempty" yaml:"description,omitempty"`
	Category       string            `json:"category,omitempty" yaml:"category,omitempty"`
	Version        string            `json:"version,omitempty" yaml:"version,omitempty"`
	Tags           []string          `json:"tags,omitempty" yaml:"tags,omitempty"`
	Params         []ParamDef        `json:"params,omitempty" yaml:"params,omitempty"`
	PreConditions  []Condition       `json:"pre_conditions,omitempty" yaml:"pre_conditions,omitempty"`
	PostConditions []Condition       `json:"post_conditions,omitempty" yaml:"post_conditions,omitempty"`
	Steps          []AgenticStep     `json:"steps" yaml:"steps"`
	Rollback       []AgenticStep     `json:"rollback,omitempty" yaml:"rollback,omitempty"`
	BlastRadius    BlastRadiusConfig `json:"blast_radius,omitempty" yaml:"blast_radius,omitempty"`
	Timeout        string            `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	DryRunDefault  bool              `json:"dry_run_default,omitempty" yaml:"dry_run_default,omitempty"`
	CreatedBy      string            `json:"created_by" yaml:"created_by"`
	CreatedAt      time.Time         `json:"created_at" yaml:"created_at"`
	Enabled        bool              `json:"enabled" yaml:"enabled"`
}

// AgenticStep extends Step with approval gates and checkpoints.
type AgenticStep struct {
	Order        int               `json:"order" yaml:"order"`
	Name         string            `json:"name" yaml:"name"`
	Description  string            `json:"description,omitempty" yaml:"description,omitempty"`
	Tool         string            `json:"tool" yaml:"tool"`
	Args         map[string]string `json:"args,omitempty" yaml:"args,omitempty"`
	Condition    string            `json:"condition,omitempty" yaml:"condition,omitempty"`
	OnFailure    string            `json:"on_failure,omitempty" yaml:"on_failure,omitempty"`
	Retries      int               `json:"retries,omitempty" yaml:"retries,omitempty"`
	RetryDelay   string            `json:"retry_delay,omitempty" yaml:"retry_delay,omitempty"`
	DelayBefore  string            `json:"delay_before,omitempty" yaml:"delay_before,omitempty"`
	ApprovalGate *ApprovalGate     `json:"approval_gate,omitempty" yaml:"approval_gate,omitempty"`
	Checkpoint   bool              `json:"checkpoint,omitempty" yaml:"checkpoint,omitempty"`
	Idempotent   bool              `json:"idempotent,omitempty" yaml:"idempotent,omitempty"`
}

// Condition defines a pre/post condition check.
type Condition struct {
	Name        string `json:"name" yaml:"name"`
	Tool        string `json:"tool" yaml:"tool"`
	Check       string `json:"check" yaml:"check"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Required    bool   `json:"required" yaml:"required"`
}

// ApprovalGate defines a human-in-the-loop gate.
type ApprovalGate struct {
	Required   bool     `json:"required" yaml:"required"`
	Approvers  []string `json:"approvers,omitempty" yaml:"approvers,omitempty"`
	Timeout    string   `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Message    string   `json:"message,omitempty" yaml:"message,omitempty"`
	Escalation string   `json:"escalation,omitempty" yaml:"escalation,omitempty"`
}

// BlastRadiusConfig limits the scope of operations.
type BlastRadiusConfig struct {
	MaxTopics       int      `json:"max_topics,omitempty" yaml:"max_topics,omitempty"`
	MaxPartitions   int      `json:"max_partitions,omitempty" yaml:"max_partitions,omitempty"`
	MaxBrokers      int      `json:"max_brokers,omitempty" yaml:"max_brokers,omitempty"`
	AllowedClusters []string `json:"allowed_clusters,omitempty" yaml:"allowed_clusters,omitempty"`
	ExcludeTopics   []string `json:"exclude_topics,omitempty" yaml:"exclude_topics,omitempty"`
}

// AgenticExecution tracks an agentic runbook execution with checkpointing.
type AgenticExecution struct {
	ID              string              `json:"id"`
	RunbookID       string              `json:"runbook_id"`
	RunbookName     string              `json:"runbook_name"`
	Params          map[string]string   `json:"params,omitempty"`
	Steps           []AgenticStepResult `json:"steps"`
	Status          string              `json:"status"` // pending, running, paused, completed, failed, rolling_back, rolled_back, dry_run_complete
	DryRun          bool                `json:"dry_run"`
	Checkpoint      int                 `json:"checkpoint"`
	PendingApproval *PendingApproval    `json:"pending_approval,omitempty"`
	PreCondResults  []ConditionResult   `json:"pre_condition_results,omitempty"`
	PostCondResults []ConditionResult   `json:"post_condition_results,omitempty"`
	StartedAt       time.Time           `json:"started_at"`
	CompletedAt     *time.Time          `json:"completed_at,omitempty"`
	ExecutedBy      string              `json:"executed_by"`
	AuditLog        []AuditEntry        `json:"audit_log"`
}

// AgenticStepResult tracks a single agentic step execution.
type AgenticStepResult struct {
	StepName     string    `json:"step_name"`
	Tool         string    `json:"tool"`
	Status       string    `json:"status"` // pending, running, completed, failed, skipped, awaiting_approval, dry_run
	Output       any       `json:"output,omitempty"`
	Error        string    `json:"error,omitempty"`
	StartedAt    time.Time `json:"started_at,omitempty"`
	Duration     string    `json:"duration,omitempty"`
	Attempt      int       `json:"attempt"`
	DryRunResult string    `json:"dry_run_result,omitempty"`
	ApprovedBy   string    `json:"approved_by,omitempty"`
}

// PendingApproval represents a step awaiting human approval.
type PendingApproval struct {
	ExecutionID string    `json:"execution_id"`
	StepIndex   int       `json:"step_index"`
	StepName    string    `json:"step_name"`
	Message     string    `json:"message"`
	Approvers   []string  `json:"approvers,omitempty"`
	RequestedAt time.Time `json:"requested_at"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// ConditionResult tracks a pre/post condition check result.
type ConditionResult struct {
	Name    string `json:"name"`
	Passed  bool   `json:"passed"`
	Details string `json:"details,omitempty"`
}

// ParseAgenticRunbookYAML parses an agentic runbook from YAML.
func ParseAgenticRunbookYAML(data string) (*AgenticRunbook, error) {
	var rb AgenticRunbook
	if err := yaml.Unmarshal([]byte(data), &rb); err != nil {
		return nil, fmt.Errorf("invalid runbook YAML: %w", err)
	}
	if err := validateAgenticRunbook(&rb); err != nil {
		return nil, err
	}
	return &rb, nil
}

func validateAgenticRunbook(rb *AgenticRunbook) error {
	if rb.Name == "" {
		return fmt.Errorf("runbook name is required")
	}
	if len(rb.Steps) == 0 {
		return fmt.Errorf("at least one step is required")
	}
	for i, s := range rb.Steps {
		if s.Name == "" {
			return fmt.Errorf("step %d: name is required", i+1)
		}
		if s.Tool == "" {
			return fmt.Errorf("step %d (%s): tool is required", i+1, s.Name)
		}
		if s.OnFailure != "" && s.OnFailure != "continue" && s.OnFailure != "abort" && s.OnFailure != "rollback" {
			return fmt.Errorf("step %d (%s): invalid on_failure: %s", i+1, s.Name, s.OnFailure)
		}
	}
	if rb.BlastRadius.MaxTopics < 0 {
		return fmt.Errorf("blast_radius.max_topics must be non-negative")
	}
	return nil
}

// CreateAgenticRunbook creates a runbook from YAML definition.
func (e *Engine) CreateAgenticRunbook(yamlDef string, createdBy string) (*AgenticRunbook, error) {
	rb, err := ParseAgenticRunbookYAML(yamlDef)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	rb.ID = fmt.Sprintf("arb_%d", e.nextID)
	rb.Enabled = true
	rb.CreatedBy = createdBy
	rb.CreatedAt = time.Now()

	for i := range rb.Steps {
		rb.Steps[i].Order = i + 1
	}

	e.agenticRunbooks[rb.ID] = rb
	e.logger.Info("agentic runbook created", "id", rb.ID, "name", rb.Name, "steps", len(rb.Steps))
	return rb, nil
}

// ExecuteAgentic starts an agentic runbook execution with dry-run support.
func (e *Engine) ExecuteAgentic(runbookID, executedBy string, params map[string]string, dryRun bool) (*AgenticExecution, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	rb, ok := e.agenticRunbooks[runbookID]
	if !ok {
		return nil, fmt.Errorf("agentic runbook %q not found", runbookID)
	}
	if !rb.Enabled {
		return nil, fmt.Errorf("agentic runbook %q is disabled", runbookID)
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

	if dryRun || rb.DryRunDefault {
		dryRun = true
	}

	e.nextID++
	execID := fmt.Sprintf("aexec_%d", e.nextID)

	exec := &AgenticExecution{
		ID:          execID,
		RunbookID:   runbookID,
		RunbookName: rb.Name,
		Params:      params,
		DryRun:      dryRun,
		StartedAt:   time.Now(),
		ExecutedBy:  executedBy,
		AuditLog: []AuditEntry{
			{Timestamp: time.Now(), Event: "execution_started",
				Details: fmt.Sprintf("Agentic runbook %q started by %s (dry_run=%v)", rb.Name, executedBy, dryRun)},
		},
	}

	if dryRun {
		exec.Status = "dry_run_complete"
		for _, step := range rb.Steps {
			stepArgs := interpolateArgs(step.Args, params)
			exec.Steps = append(exec.Steps, AgenticStepResult{
				StepName:     step.Name,
				Tool:         step.Tool,
				Status:       "dry_run",
				DryRunResult: fmt.Sprintf("Would execute %s with args: %v", step.Tool, stepArgs),
			})
		}
		now := time.Now()
		exec.CompletedAt = &now
		exec.AuditLog = append(exec.AuditLog, AuditEntry{
			Timestamp: now, Event: "dry_run_complete",
			Details: fmt.Sprintf("Dry run completed with %d steps", len(rb.Steps)),
		})
	} else {
		exec.Status = "running"
		// Check for first approval gate
		for i, step := range rb.Steps {
			sr := AgenticStepResult{
				StepName: step.Name,
				Tool:     step.Tool,
				Status:   "pending",
			}
			if step.ApprovalGate != nil && step.ApprovalGate.Required {
				sr.Status = "awaiting_approval"
				timeout := 30 * time.Minute
				if step.ApprovalGate.Timeout != "" {
					if d, err := time.ParseDuration(step.ApprovalGate.Timeout); err == nil {
						timeout = d
					}
				}
				exec.PendingApproval = &PendingApproval{
					ExecutionID: execID,
					StepIndex:   i,
					StepName:    step.Name,
					Message:     step.ApprovalGate.Message,
					Approvers:   step.ApprovalGate.Approvers,
					RequestedAt: time.Now(),
					ExpiresAt:   time.Now().Add(timeout),
				}
				exec.Status = "paused"
			}
			exec.Steps = append(exec.Steps, sr)
			if exec.Status == "paused" {
				break
			}
		}
		// Fill remaining steps as pending
		for i := len(exec.Steps); i < len(rb.Steps); i++ {
			exec.Steps = append(exec.Steps, AgenticStepResult{
				StepName: rb.Steps[i].Name,
				Tool:     rb.Steps[i].Tool,
				Status:   "pending",
			})
		}
	}

	e.agenticExecutions[execID] = exec
	return exec, nil
}

// ApproveStep approves a pending approval gate.
func (e *Engine) ApproveStep(executionID string, approver string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	exec, ok := e.agenticExecutions[executionID]
	if !ok {
		return fmt.Errorf("execution %q not found", executionID)
	}
	if exec.PendingApproval == nil {
		return fmt.Errorf("no pending approval for execution %q", executionID)
	}
	if time.Now().After(exec.PendingApproval.ExpiresAt) {
		return fmt.Errorf("approval for execution %q has expired", executionID)
	}

	stepIdx := exec.PendingApproval.StepIndex
	exec.Steps[stepIdx].Status = "pending"
	exec.Steps[stepIdx].ApprovedBy = approver
	exec.PendingApproval = nil
	exec.Status = "running"

	exec.AuditLog = append(exec.AuditLog, AuditEntry{
		Timestamp: time.Now(),
		Event:     "step_approved",
		Details:   fmt.Sprintf("Step %q approved by %s", exec.Steps[stepIdx].StepName, approver),
	})

	return nil
}

// RejectStep rejects a pending approval gate and aborts the execution.
func (e *Engine) RejectStep(executionID string, rejector string, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	exec, ok := e.agenticExecutions[executionID]
	if !ok {
		return fmt.Errorf("execution %q not found", executionID)
	}
	if exec.PendingApproval == nil {
		return fmt.Errorf("no pending approval for execution %q", executionID)
	}

	stepIdx := exec.PendingApproval.StepIndex
	exec.Steps[stepIdx].Status = "failed"
	exec.Steps[stepIdx].Error = fmt.Sprintf("Rejected by %s: %s", rejector, reason)
	exec.PendingApproval = nil
	exec.Status = "failed"
	now := time.Now()
	exec.CompletedAt = &now

	exec.AuditLog = append(exec.AuditLog, AuditEntry{
		Timestamp: now, Event: "step_rejected",
		Details: fmt.Sprintf("Step %q rejected by %s: %s", exec.Steps[stepIdx].StepName, rejector, reason),
	})

	return nil
}

// RollbackExecution initiates rollback for a failed execution.
func (e *Engine) RollbackExecution(executionID string) (*AgenticExecution, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	exec, ok := e.agenticExecutions[executionID]
	if !ok {
		return nil, fmt.Errorf("execution %q not found", executionID)
	}
	if exec.Status != "failed" && exec.Status != "running" {
		return nil, fmt.Errorf("cannot rollback execution in status %q", exec.Status)
	}

	rb, ok := e.agenticRunbooks[exec.RunbookID]
	if !ok {
		return nil, fmt.Errorf("runbook %q not found", exec.RunbookID)
	}

	if len(rb.Rollback) == 0 {
		return nil, fmt.Errorf("no rollback steps defined for runbook %q", exec.RunbookID)
	}

	exec.Status = "rolling_back"
	exec.AuditLog = append(exec.AuditLog, AuditEntry{
		Timestamp: time.Now(), Event: "rollback_started",
		Details: fmt.Sprintf("Rolling back %d steps", len(rb.Rollback)),
	})

	return exec, nil
}

// CheckBlastRadius validates that the operation is within blast-radius limits.
func (e *Engine) CheckBlastRadius(runbookID string, topicCount, partitionCount, brokerCount int, cluster string) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	rb, ok := e.agenticRunbooks[runbookID]
	if !ok {
		return nil
	}

	br := rb.BlastRadius
	if br.MaxTopics > 0 && topicCount > br.MaxTopics {
		return fmt.Errorf("blast radius exceeded: %d topics > max %d", topicCount, br.MaxTopics)
	}
	if br.MaxPartitions > 0 && partitionCount > br.MaxPartitions {
		return fmt.Errorf("blast radius exceeded: %d partitions > max %d", partitionCount, br.MaxPartitions)
	}
	if br.MaxBrokers > 0 && brokerCount > br.MaxBrokers {
		return fmt.Errorf("blast radius exceeded: %d brokers > max %d", brokerCount, br.MaxBrokers)
	}
	if len(br.AllowedClusters) > 0 && cluster != "" {
		allowed := false
		for _, c := range br.AllowedClusters {
			if c == cluster {
				allowed = true
				break
			}
		}
		if !allowed {
			return fmt.Errorf("cluster %q is not in allowed clusters", cluster)
		}
	}

	return nil
}

// GetAgenticRunbook returns an agentic runbook by ID.
func (e *Engine) GetAgenticRunbook(id string) (*AgenticRunbook, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	rb, ok := e.agenticRunbooks[id]
	if !ok {
		return nil, fmt.Errorf("agentic runbook %q not found", id)
	}
	return rb, nil
}

// ListAgenticRunbooks returns all agentic runbooks.
func (e *Engine) ListAgenticRunbooks(category string) []AgenticRunbook {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var result []AgenticRunbook
	for _, rb := range e.agenticRunbooks {
		if category == "" || rb.Category == category {
			result = append(result, *rb)
		}
	}
	return result
}

// GetAgenticExecution returns an agentic execution by ID.
func (e *Engine) GetAgenticExecution(id string) (*AgenticExecution, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	exec, ok := e.agenticExecutions[id]
	if !ok {
		return nil, fmt.Errorf("agentic execution %q not found", id)
	}
	return exec, nil
}

// ListAgenticExecutions returns recent agentic executions.
func (e *Engine) ListAgenticExecutions(limit int) []AgenticExecution {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if limit <= 0 {
		limit = 50
	}
	var result []AgenticExecution
	for _, exec := range e.agenticExecutions {
		result = append(result, *exec)
	}
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

// ResumeExecution resumes a paused execution from its checkpoint.
func (e *Engine) ResumeExecution(executionID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	exec, ok := e.agenticExecutions[executionID]
	if !ok {
		return fmt.Errorf("execution %q not found", executionID)
	}
	if exec.Status != "paused" {
		return fmt.Errorf("cannot resume execution in status %q", exec.Status)
	}
	exec.Status = "running"
	exec.AuditLog = append(exec.AuditLog, AuditEntry{
		Timestamp: time.Now(), Event: "execution_resumed",
		Details: fmt.Sprintf("Execution resumed from checkpoint %d", exec.Checkpoint),
	})
	return nil
}

func interpolateArgs(args map[string]string, params map[string]string) map[string]string {
	if args == nil {
		return nil
	}
	result := make(map[string]string, len(args))
	for k, v := range args {
		for pk, pv := range params {
			v = strings.ReplaceAll(v, fmt.Sprintf("${%s}", pk), pv)
		}
		result[k] = v
	}
	return result
}
