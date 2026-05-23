// Package dagworkflow provides a DAG-based workflow orchestrator for multi-step
// Kafka operations with conditional branching, parallel fan-out, and error recovery.
package dagworkflow

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

// WorkflowTemplate defines a reusable, versioned workflow as a DAG of steps.
type WorkflowTemplate struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Version     int         `json:"version"`
	Parameters  []Parameter `json:"parameters,omitempty"`
	Steps       []DAGStep   `json:"steps"`
	CreatedBy   string      `json:"created_by"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
	Tags        []string    `json:"tags,omitempty"`
}

// Parameter defines an input parameter for a workflow template.
type Parameter struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Type        string `json:"type"` // string, number, boolean
	Required    bool   `json:"required"`
	Default     any    `json:"default,omitempty"`
}

// DAGStep is a single node in the workflow DAG.
type DAGStep struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Tool       string            `json:"tool"`
	Args       map[string]string `json:"args,omitempty"`
	DependsOn  []string          `json:"depends_on,omitempty"`
	Condition  string            `json:"condition,omitempty"`
	OnError    string            `json:"on_error,omitempty"` // stop, continue, retry
	MaxRetries int               `json:"max_retries,omitempty"`
	TimeoutSec int               `json:"timeout_sec,omitempty"`
}

// WorkflowRun tracks a running workflow execution.
type WorkflowRun struct {
	ID           string                `json:"id"`
	TemplateID   string                `json:"template_id"`
	TemplateName string                `json:"template_name"`
	Version      int                   `json:"version"`
	Status       string                `json:"status"` // pending, running, completed, failed, canceled
	Parameters   map[string]any        `json:"parameters"`
	StepResults  map[string]StepResult `json:"step_results"`
	StartedAt    time.Time             `json:"started_at"`
	CompletedAt  *time.Time            `json:"completed_at,omitempty"`
	Error        string                `json:"error,omitempty"`
	CreatedBy    string                `json:"created_by"`
}

// StepResult records the outcome of a single DAG step.
type StepResult struct {
	StepID    string    `json:"step_id"`
	StepName  string    `json:"step_name"`
	Tool      string    `json:"tool"`
	Status    string    `json:"status"` // pending, running, completed, failed, skipped
	Output    any       `json:"output,omitempty"`
	Error     string    `json:"error,omitempty"`
	Retries   int       `json:"retries"`
	StartedAt time.Time `json:"started_at,omitempty"`
	Duration  string    `json:"duration,omitempty"`
}

// Engine manages DAG workflow templates and executions.
type Engine struct {
	mu             sync.RWMutex
	templates      map[string]*WorkflowTemplate   // key: template ID
	versions       map[string][]*WorkflowTemplate // key: name -> version history
	runs           map[string]*WorkflowRun
	multiAgentRuns map[string]*MultiAgentRun
	logger         *slog.Logger
	nextID         int64
}

// NewEngine creates a new DAG workflow engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		templates:      make(map[string]*WorkflowTemplate),
		versions:       make(map[string][]*WorkflowTemplate),
		runs:           make(map[string]*WorkflowRun),
		multiAgentRuns: make(map[string]*MultiAgentRun),
		logger:         logger,
	}
}

// CreateWorkflow creates a new workflow template and validates the DAG structure.
func (e *Engine) CreateWorkflow(name, description, createdBy string, params []Parameter, steps []DAGStep, tags []string) (*WorkflowTemplate, error) {
	if name == "" {
		return nil, fmt.Errorf("workflow name is required")
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("workflow must have at least one step")
	}

	// Validate DAG: check for cycles and missing dependencies
	if err := validateDAG(steps); err != nil {
		return nil, fmt.Errorf("invalid DAG: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Determine version
	version := 1
	if existing, ok := e.versions[name]; ok {
		version = len(existing) + 1
	}

	e.nextID++
	id := fmt.Sprintf("wf_%d_v%d", e.nextID, version)

	tmpl := &WorkflowTemplate{
		ID:          id,
		Name:        name,
		Description: description,
		Version:     version,
		Parameters:  params,
		Steps:       steps,
		CreatedBy:   createdBy,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Tags:        tags,
	}

	e.templates[id] = tmpl
	e.versions[name] = append(e.versions[name], tmpl)

	e.logger.Info("workflow created", "id", id, "name", name, "version", version, "steps", len(steps))
	return tmpl, nil
}

// Close is a no-op placeholder for future goroutine cleanup.
func (e *Engine) Close() {}

// GetWorkflow returns a workflow template by ID.
func (e *Engine) GetWorkflow(id string) (*WorkflowTemplate, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	tmpl, ok := e.templates[id]
	if !ok {
		return nil, fmt.Errorf("workflow %q not found", id)
	}
	return tmpl, nil
}

// ListWorkflows returns all workflow templates.
func (e *Engine) ListWorkflows() []WorkflowTemplate {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []WorkflowTemplate
	// Return only latest version of each named workflow
	seen := make(map[string]bool)
	for _, tmpl := range e.templates {
		key := tmpl.Name
		if seen[key] {
			continue
		}
		versions := e.versions[tmpl.Name]
		latest := versions[len(versions)-1]
		result = append(result, *latest)
		seen[key] = true
	}
	return result
}

// GetVersions returns all versions of a named workflow.
func (e *Engine) GetVersions(name string) ([]WorkflowTemplate, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	versions, ok := e.versions[name]
	if !ok {
		return nil, fmt.Errorf("workflow %q not found", name)
	}

	result := make([]WorkflowTemplate, len(versions))
	for i, v := range versions {
		result[i] = *v
	}
	return result, nil
}

// StartRun creates a new workflow execution.
func (e *Engine) StartRun(templateID, createdBy string, params map[string]any) (*WorkflowRun, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	tmpl, ok := e.templates[templateID]
	if !ok {
		return nil, fmt.Errorf("workflow template %q not found", templateID)
	}

	// Validate required parameters
	if params == nil {
		params = make(map[string]any)
	}
	for _, p := range tmpl.Parameters {
		if _, ok := params[p.Name]; !ok {
			if p.Required {
				if p.Default != nil {
					params[p.Name] = p.Default
				} else {
					return nil, fmt.Errorf("required parameter %q not provided", p.Name)
				}
			} else if p.Default != nil {
				params[p.Name] = p.Default
			}
		}
	}

	e.nextID++
	runID := fmt.Sprintf("run_%d", e.nextID)

	// Initialize step results
	stepResults := make(map[string]StepResult, len(tmpl.Steps))
	for _, step := range tmpl.Steps {
		stepResults[step.ID] = StepResult{
			StepID:   step.ID,
			StepName: step.Name,
			Tool:     step.Tool,
			Status:   "pending",
		}
	}

	run := &WorkflowRun{
		ID:           runID,
		TemplateID:   templateID,
		TemplateName: tmpl.Name,
		Version:      tmpl.Version,
		Status:       "running",
		Parameters:   params,
		StepResults:  stepResults,
		StartedAt:    time.Now(),
		CreatedBy:    createdBy,
	}

	e.runs[runID] = run
	e.logger.Info("workflow run started", "run_id", runID, "template", tmpl.Name)
	return run, nil
}

// GetReadySteps returns steps whose dependencies are all completed.
func (e *Engine) GetReadySteps(runID string) ([]DAGStep, error) {
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

	var ready []DAGStep
	for _, step := range tmpl.Steps {
		result := run.StepResults[step.ID]
		if result.Status != "pending" {
			continue
		}

		// Check if condition is met
		if step.Condition != "" {
			resolved := interpolate(step.Condition, run.Parameters)
			if resolved == "" || resolved == step.Condition {
				continue
			}
		}

		// Check all dependencies are completed
		allDepsComplete := true
		for _, dep := range step.DependsOn {
			depResult, exists := run.StepResults[dep]
			if !exists || (depResult.Status != "completed" && depResult.Status != "skipped") {
				allDepsComplete = false
				break
			}
		}

		if allDepsComplete {
			ready = append(ready, step)
		}
	}
	return ready, nil
}

// CompleteStep records the result of a step execution.
func (e *Engine) CompleteStep(runID, stepID, status string, output any, errMsg string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	run, ok := e.runs[runID]
	if !ok {
		return fmt.Errorf("run %q not found", runID)
	}

	result, ok := run.StepResults[stepID]
	if !ok {
		return fmt.Errorf("step %q not found in run %q", stepID, runID)
	}

	result.Status = status
	result.Output = output
	result.Error = errMsg
	if !result.StartedAt.IsZero() {
		result.Duration = time.Since(result.StartedAt).String()
	}
	run.StepResults[stepID] = result

	// Check if all steps are done
	allDone := true
	anyFailed := false
	for _, r := range run.StepResults {
		if r.Status == "pending" || r.Status == "running" {
			allDone = false
		}
		if r.Status == "failed" {
			anyFailed = true
		}
	}

	if allDone {
		now := time.Now()
		run.CompletedAt = &now
		if anyFailed {
			run.Status = "failed"
		} else {
			run.Status = "completed"
		}
	}

	return nil
}

// ResolveStepArgs interpolates parameters into step arguments.
func (e *Engine) ResolveStepArgs(step DAGStep, params map[string]any) map[string]any {
	resolved := make(map[string]any, len(step.Args))
	for k, v := range step.Args {
		resolved[k] = interpolate(v, params)
	}
	return resolved
}

// GetRun returns a workflow run by ID.
func (e *Engine) GetRun(runID string) (*WorkflowRun, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	run, ok := e.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %q not found", runID)
	}
	return run, nil
}

// ListRuns returns workflow runs, optionally filtered by template.
func (e *Engine) ListRuns(templateID string, limit int) []WorkflowRun {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var result []WorkflowRun
	for _, r := range e.runs {
		if templateID == "" || r.TemplateID == templateID {
			result = append(result, *r)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].StartedAt.After(result[j].StartedAt)
	})

	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

// CancelRun cancels a running workflow.
func (e *Engine) CancelRun(runID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	run, ok := e.runs[runID]
	if !ok {
		return fmt.Errorf("run %q not found", runID)
	}
	if run.Status != "running" {
		return fmt.Errorf("run %q is not running (status: %s)", runID, run.Status)
	}

	now := time.Now()
	run.Status = "canceled"
	run.CompletedAt = &now
	return nil
}

// validateDAG checks that the step graph is a valid DAG (no cycles, all deps exist).
func validateDAG(steps []DAGStep) error {
	stepIDs := make(map[string]bool, len(steps))
	for _, s := range steps {
		if s.ID == "" {
			return fmt.Errorf("step must have an ID")
		}
		if s.Tool == "" {
			return fmt.Errorf("step %q must have a tool", s.ID)
		}
		if stepIDs[s.ID] {
			return fmt.Errorf("duplicate step ID %q", s.ID)
		}
		stepIDs[s.ID] = true
	}

	// Check dependencies exist
	for _, s := range steps {
		for _, dep := range s.DependsOn {
			if !stepIDs[dep] {
				return fmt.Errorf("step %q depends on unknown step %q", s.ID, dep)
			}
		}
	}

	// Topological sort to detect cycles
	inDegree := make(map[string]int)
	for _, s := range steps {
		if _, ok := inDegree[s.ID]; !ok {
			inDegree[s.ID] = 0
		}
		for _, dep := range s.DependsOn {
			inDegree[s.ID]++
			_ = dep
		}
	}

	queue := make([]string, 0)
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	visited := 0
	adj := make(map[string][]string)
	for _, s := range steps {
		for _, dep := range s.DependsOn {
			adj[dep] = append(adj[dep], s.ID)
		}
	}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range adj[node] {
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	if visited != len(steps) {
		return fmt.Errorf("workflow contains a cycle")
	}

	return nil
}

func interpolate(template string, params map[string]any) string {
	result := template
	for k, v := range params {
		var strVal string
		switch val := v.(type) {
		case string:
			strVal = val
		case json.Number:
			strVal = val.String()
		default:
			strVal = fmt.Sprintf("%v", val)
		}
		result = strings.ReplaceAll(result, "${"+k+"}", strVal)
	}
	return result
}
