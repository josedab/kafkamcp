package playbook

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// TriggerType defines what anomaly pattern activates a playbook.
type TriggerType string

// TriggerHighLag and related values define constants for this package.
const (
	TriggerHighLag         TriggerType = "high_consumer_lag"
	TriggerBrokerDown      TriggerType = "broker_down"
	TriggerPartitionSkew   TriggerType = "partition_skew"
	TriggerThroughputDrop  TriggerType = "throughput_drop"
	TriggerThroughputSpike TriggerType = "throughput_spike"
	TriggerUnderReplicated TriggerType = "under_replicated"
	TriggerDiskUsage       TriggerType = "disk_usage_high"
	TriggerSchemaError     TriggerType = "schema_error"
	TriggerCustom          TriggerType = "custom"
)

// Playbook defines a set of remediation steps triggered by anomaly patterns.
type Playbook struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Trigger     TriggerConfig     `json:"trigger"`
	Steps       []RemediationStep `json:"steps"`
	Cooldown    string            `json:"cooldown,omitempty"` // min time between executions
	MaxRetries  int               `json:"max_retries,omitempty"`
	Enabled     bool              `json:"enabled"`
	CreatedBy   string            `json:"created_by"`
	CreatedAt   time.Time         `json:"created_at"`
}

// TriggerConfig defines when a playbook should be activated.
type TriggerConfig struct {
	Type       TriggerType       `json:"type"`
	Threshold  float64           `json:"threshold,omitempty"`
	Duration   string            `json:"duration,omitempty"`
	Topics     []string          `json:"topics,omitempty"`
	Conditions map[string]string `json:"conditions,omitempty"`
}

// RemediationStep is a single action in a playbook.
type RemediationStep struct {
	Order       int               `json:"order"`
	Name        string            `json:"name"`
	Tool        string            `json:"tool"`
	Args        map[string]string `json:"args,omitempty"`
	Condition   string            `json:"condition,omitempty"`  // skip condition
	OnFailure   string            `json:"on_failure,omitempty"` // continue, abort, skip
	Description string            `json:"description,omitempty"`
}

// PlaybookExecution tracks a playbook run.
type PlaybookExecution struct {
	ID          string          `json:"id"`
	PlaybookID  string          `json:"playbook_id"`
	TriggerInfo map[string]any  `json:"trigger_info"`
	Steps       []StepExecution `json:"steps"`
	Status      string          `json:"status"` // running, completed, failed, aborted
	StartedAt   time.Time       `json:"started_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	ExecutedBy  string          `json:"executed_by"`
}

// StepExecution tracks a single step's execution.
type StepExecution struct {
	StepName  string    `json:"step_name"`
	Tool      string    `json:"tool"`
	Status    string    `json:"status"` // pending, running, completed, failed, skipped
	Output    any       `json:"output,omitempty"`
	Error     string    `json:"error,omitempty"`
	StartedAt time.Time `json:"started_at,omitempty"`
	Duration  string    `json:"duration,omitempty"`
}

// Engine manages playbooks and their execution.
type Engine struct {
	mu         sync.RWMutex
	playbooks  map[string]*Playbook
	executions map[string]*PlaybookExecution
	lastRun    map[string]time.Time // playbook ID -> last execution time
	logger     *slog.Logger
	nextID     int64
}

// NewEngine creates a new playbook engine with built-in playbooks.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		playbooks:  make(map[string]*Playbook),
		executions: make(map[string]*PlaybookExecution),
		lastRun:    make(map[string]time.Time),
		logger:     logger,
	}
	e.seedBuiltins()
	return e
}

func (e *Engine) seedBuiltins() {
	builtins := []Playbook{
		{
			ID: "pb_high_lag", Name: "High Consumer Lag Response",
			Description: "Automatically investigates and remediates high consumer lag",
			Trigger:     TriggerConfig{Type: TriggerHighLag, Threshold: 10000},
			Steps: []RemediationStep{
				{Order: 1, Name: "Diagnose lag", Tool: "kafka_consumer_lag_analyze", Description: "Analyze consumer lag root cause"},
				{Order: 2, Name: "Check consumer health", Tool: "kafka_consumer_groups", Description: "Verify consumer group status"},
				{Order: 3, Name: "Scale consumers", Tool: "kafka_cluster_info", Args: map[string]string{"action": "scale"}, OnFailure: "continue", Description: "Attempt to scale consumer instances"},
				{Order: 4, Name: "Alert team", Tool: "kafka_audit_trail", Description: "Log alert for team review"},
			},
			Cooldown: "5m", MaxRetries: 2, Enabled: true, CreatedBy: "system", CreatedAt: time.Now(),
		},
		{
			ID: "pb_under_replicated", Name: "Under-Replicated Partitions Response",
			Description: "Responds to under-replicated partition alerts",
			Trigger:     TriggerConfig{Type: TriggerUnderReplicated, Threshold: 1},
			Steps: []RemediationStep{
				{Order: 1, Name: "Check broker status", Tool: "kafka_cluster_info", Description: "Verify all brokers are online"},
				{Order: 2, Name: "Identify affected topics", Tool: "kafka_list_topics", Description: "List topics with under-replicated partitions"},
				{Order: 3, Name: "Trigger reassignment", Tool: "kafka_cluster_info", Args: map[string]string{"action": "reassign"}, OnFailure: "abort", Description: "Initiate partition reassignment"},
			},
			Cooldown: "10m", MaxRetries: 1, Enabled: true, CreatedBy: "system", CreatedAt: time.Now(),
		},
		{
			ID: "pb_throughput_spike", Name: "Throughput Spike Response",
			Description: "Investigates sudden throughput increases",
			Trigger:     TriggerConfig{Type: TriggerThroughputSpike, Threshold: 200},
			Steps: []RemediationStep{
				{Order: 1, Name: "Capture metrics", Tool: "kafka_cluster_info", Description: "Capture current throughput metrics"},
				{Order: 2, Name: "Identify source", Tool: "kafka_list_topics", Description: "Identify topics with unusual traffic"},
				{Order: 3, Name: "Check rate limits", Tool: "kafka_rate_limit_status", Description: "Verify rate limits are appropriate"},
			},
			Cooldown: "15m", MaxRetries: 1, Enabled: true, CreatedBy: "system", CreatedAt: time.Now(),
		},
	}
	for i := range builtins {
		e.playbooks[builtins[i].ID] = &builtins[i]
	}
}

// CreatePlaybook creates a custom playbook.
func (e *Engine) CreatePlaybook(name, description, createdBy string, trigger TriggerConfig, steps []RemediationStep) (*Playbook, error) {
	if name == "" {
		return nil, fmt.Errorf("playbook name is required")
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("at least one step is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("pb_%d", e.nextID)

	for i := range steps {
		steps[i].Order = i + 1
	}

	pb := &Playbook{
		ID:          id,
		Name:        name,
		Description: description,
		Trigger:     trigger,
		Steps:       steps,
		Enabled:     true,
		CreatedBy:   createdBy,
		CreatedAt:   time.Now(),
	}

	e.playbooks[id] = pb
	e.logger.Info("playbook created", "id", id, "name", name, "steps", len(steps))
	return pb, nil
}

// Execute starts a playbook execution.
func (e *Engine) Execute(playbookID, executedBy string, triggerInfo map[string]any) (*PlaybookExecution, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	pb, ok := e.playbooks[playbookID]
	if !ok {
		return nil, fmt.Errorf("playbook %q not found", playbookID)
	}
	if !pb.Enabled {
		return nil, fmt.Errorf("playbook %q is disabled", playbookID)
	}

	// Check cooldown
	if pb.Cooldown != "" {
		if last, ok := e.lastRun[playbookID]; ok {
			cd, err := time.ParseDuration(pb.Cooldown)
			if err == nil && time.Since(last) < cd {
				return nil, fmt.Errorf("playbook %q is in cooldown (last run: %s)", playbookID, last.Format(time.RFC3339))
			}
		}
	}

	e.nextID++
	id := fmt.Sprintf("exec_%d", e.nextID)

	exec := &PlaybookExecution{
		ID:          id,
		PlaybookID:  playbookID,
		TriggerInfo: triggerInfo,
		Status:      "running",
		StartedAt:   time.Now(),
		ExecutedBy:  executedBy,
	}

	for _, step := range pb.Steps {
		exec.Steps = append(exec.Steps, StepExecution{
			StepName: step.Name,
			Tool:     step.Tool,
			Status:   "pending",
		})
	}

	e.executions[id] = exec
	e.lastRun[playbookID] = time.Now()
	return exec, nil
}

// CompleteStep completes a step in an execution.
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

	// Check if all steps are done
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
	}

	return nil
}

// MatchTrigger finds playbooks matching a given trigger type and threshold.
func (e *Engine) MatchTrigger(triggerType TriggerType, value float64) []Playbook {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var matches []Playbook
	for _, pb := range e.playbooks {
		if pb.Enabled && pb.Trigger.Type == triggerType {
			if pb.Trigger.Threshold <= 0 || value >= pb.Trigger.Threshold {
				matches = append(matches, *pb)
			}
		}
	}
	return matches
}

// GetPlaybook returns a playbook by ID.
func (e *Engine) GetPlaybook(id string) (*Playbook, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	pb, ok := e.playbooks[id]
	if !ok {
		return nil, fmt.Errorf("playbook %q not found", id)
	}
	return pb, nil
}

// ListPlaybooks returns all playbooks.
func (e *Engine) ListPlaybooks() []Playbook {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]Playbook, 0, len(e.playbooks))
	for _, pb := range e.playbooks {
		result = append(result, *pb)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// GetExecution returns an execution by ID.
func (e *Engine) GetExecution(id string) (*PlaybookExecution, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	exec, ok := e.executions[id]
	if !ok {
		return nil, fmt.Errorf("execution %q not found", id)
	}
	return exec, nil
}

// ListExecutions returns recent executions.
func (e *Engine) ListExecutions(playbookID string, limit int) []PlaybookExecution {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var result []PlaybookExecution
	for _, exec := range e.executions {
		if playbookID == "" || exec.PlaybookID == playbookID {
			result = append(result, *exec)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].StartedAt.After(result[j].StartedAt) })
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

// EnablePlaybook enables or disables a playbook.
func (e *Engine) EnablePlaybook(id string, enabled bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	pb, ok := e.playbooks[id]
	if !ok {
		return fmt.Errorf("playbook %q not found", id)
	}
	pb.Enabled = enabled
	return nil
}

// DeletePlaybook removes a playbook.
func (e *Engine) DeletePlaybook(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.playbooks[id]; !ok {
		return fmt.Errorf("playbook %q not found", id)
	}
	delete(e.playbooks, id)
	return nil
}
