package toolcompose

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Composition defines a composite tool built from atomic MCP tools.
type Composition struct {
	ID          string             `json:"id" yaml:"id"`
	Name        string             `json:"name" yaml:"name"`
	Description string             `json:"description,omitempty" yaml:"description,omitempty"`
	Category    string             `json:"category,omitempty" yaml:"category,omitempty"`
	Version     string             `json:"version,omitempty" yaml:"version,omitempty"`
	Tags        []string           `json:"tags,omitempty" yaml:"tags,omitempty"`
	Parameters  []CompositionParam `json:"parameters,omitempty" yaml:"parameters,omitempty"`
	Steps       []CompositionStep  `json:"steps" yaml:"steps"`
	CreatedBy   string             `json:"created_by" yaml:"created_by"`
	CreatedAt   time.Time          `json:"created_at" yaml:"created_at"`
	Shared      bool               `json:"shared" yaml:"shared"`
}

// CompositionParam defines an input parameter for the composition.
type CompositionParam struct {
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
	Type        string `json:"type" yaml:"type"` // string, number, boolean
	Required    bool   `json:"required" yaml:"required"`
	Default     any    `json:"default,omitempty" yaml:"default,omitempty"`
}

// CompositionStep defines a single step in a composition.
type CompositionStep struct {
	ID         string            `json:"id" yaml:"id"`
	Name       string            `json:"name" yaml:"name"`
	Tool       string            `json:"tool" yaml:"tool"`
	Args       map[string]string `json:"args,omitempty" yaml:"args,omitempty"`
	DependsOn  []string          `json:"depends_on,omitempty" yaml:"depends_on,omitempty"`
	Condition  string            `json:"condition,omitempty" yaml:"condition,omitempty"`
	OnError    string            `json:"on_error,omitempty" yaml:"on_error,omitempty"` // continue, stop, retry
	Retries    int               `json:"retries,omitempty" yaml:"retries,omitempty"`
	RetryDelay string            `json:"retry_delay,omitempty" yaml:"retry_delay,omitempty"`
	Parallel   bool              `json:"parallel,omitempty" yaml:"parallel,omitempty"`
	Output     string            `json:"output,omitempty" yaml:"output,omitempty"` // variable name for output
}

// CompositionExecution tracks a composition run.
type CompositionExecution struct {
	ID            string          `json:"id"`
	CompositionID string          `json:"composition_id"`
	Name          string          `json:"name"`
	Status        string          `json:"status"` // running, completed, failed
	Parameters    map[string]any  `json:"parameters,omitempty"`
	Steps         []StepExecution `json:"steps"`
	Outputs       map[string]any  `json:"outputs,omitempty"`
	StartedAt     time.Time       `json:"started_at"`
	CompletedAt   *time.Time      `json:"completed_at,omitempty"`
	ExecutedBy    string          `json:"executed_by"`
}

// StepExecution tracks a single step's execution.
type StepExecution struct {
	StepID   string `json:"step_id"`
	StepName string `json:"step_name"`
	Tool     string `json:"tool"`
	Status   string `json:"status"` // pending, running, completed, failed, skipped
	Output   any    `json:"output,omitempty"`
	Error    string `json:"error,omitempty"`
	Duration string `json:"duration,omitempty"`
	Attempt  int    `json:"attempt"`
}

// Engine manages tool compositions.
type Engine struct {
	mu           sync.RWMutex
	compositions map[string]*Composition
	executions   map[string]*CompositionExecution
	nextID       int64
	logger       *slog.Logger
}

// NewEngine creates a new composition engine.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		compositions: make(map[string]*Composition),
		executions:   make(map[string]*CompositionExecution),
		logger:       logger,
	}
	e.seedBuiltins()
	return e
}

func (e *Engine) seedBuiltins() {
	builtins := []Composition{
		{
			ID: "compose_deploy_schema", Name: "deploy-schema-and-migrate",
			Description: "Validate, register schema, update consumers, and verify compatibility",
			Category:    "deployment",
			Parameters: []CompositionParam{
				{Name: "subject", Required: true, Type: "string"},
				{Name: "schema", Required: true, Type: "string"},
				{Name: "topic", Required: true, Type: "string"},
			},
			Steps: []CompositionStep{
				{ID: "validate", Name: "Validate Schema", Tool: "kafka_schema", Args: map[string]string{"action": "validate", "subject": "${subject}", "schema": "${schema}"}},
				{ID: "register", Name: "Register Schema", Tool: "kafka_schema", Args: map[string]string{"action": "register", "subject": "${subject}", "schema": "${schema}"}, DependsOn: []string{"validate"}},
				{ID: "verify", Name: "Verify Compatibility", Tool: "kafka_schema", Args: map[string]string{"action": "compatibility", "subject": "${subject}"}, DependsOn: []string{"register"}},
			},
			Shared: true, CreatedBy: "system", CreatedAt: time.Now(),
		},
		{
			ID: "compose_topic_health", Name: "full-topic-health-check",
			Description: "Comprehensive topic health check: describe, check lag, verify schema, analyze quality",
			Category:    "operations",
			Parameters: []CompositionParam{
				{Name: "topic", Required: true, Type: "string"},
			},
			Steps: []CompositionStep{
				{ID: "describe", Name: "Describe Topic", Tool: "kafka_describe_topic", Args: map[string]string{"topic": "${topic}"}},
				{ID: "lag", Name: "Check Consumer Lag", Tool: "kafka_consumer_lag", Args: map[string]string{"topic": "${topic}"}, DependsOn: []string{"describe"}, Parallel: true},
				{ID: "schema", Name: "Check Schema", Tool: "kafka_schema", Args: map[string]string{"action": "get", "subject": "${topic}-value"}, DependsOn: []string{"describe"}, Parallel: true},
				{ID: "health", Name: "Health Score", Tool: "kafka_health_score", DependsOn: []string{"lag", "schema"}},
			},
			Shared: true, CreatedBy: "system", CreatedAt: time.Now(),
		},
	}
	for i := range builtins {
		e.compositions[builtins[i].ID] = &builtins[i]
	}
}

// ParseCompositionYAML parses a composition from YAML.
func ParseCompositionYAML(data string) (*Composition, error) {
	var comp Composition
	if err := yaml.Unmarshal([]byte(data), &comp); err != nil {
		return nil, fmt.Errorf("invalid composition YAML: %w", err)
	}
	if err := ValidateComposition(&comp); err != nil {
		return nil, err
	}
	return &comp, nil
}

// ValidateComposition checks a composition for errors including circular dependencies.
func ValidateComposition(comp *Composition) error {
	if comp.Name == "" {
		return fmt.Errorf("composition name is required")
	}
	if len(comp.Steps) == 0 {
		return fmt.Errorf("at least one step is required")
	}

	// Build step ID set
	stepIDs := make(map[string]bool)
	for _, s := range comp.Steps {
		if s.ID == "" {
			return fmt.Errorf("step ID is required")
		}
		if s.Tool == "" {
			return fmt.Errorf("step %q: tool is required", s.ID)
		}
		if stepIDs[s.ID] {
			return fmt.Errorf("duplicate step ID: %s", s.ID)
		}
		stepIDs[s.ID] = true
	}

	// Validate dependencies exist
	for _, s := range comp.Steps {
		for _, dep := range s.DependsOn {
			if !stepIDs[dep] {
				return fmt.Errorf("step %q depends on unknown step %q", s.ID, dep)
			}
			if dep == s.ID {
				return fmt.Errorf("step %q cannot depend on itself", s.ID)
			}
		}
	}

	// DAG cycle detection via topological sort
	if err := detectCycles(comp.Steps); err != nil {
		return err
	}

	return nil
}

// detectCycles uses Kahn's algorithm to detect circular dependencies.
func detectCycles(steps []CompositionStep) error {
	inDegree := make(map[string]int)
	graph := make(map[string][]string)

	for _, s := range steps {
		if _, ok := inDegree[s.ID]; !ok {
			inDegree[s.ID] = 0
		}
		for _, dep := range s.DependsOn {
			graph[dep] = append(graph[dep], s.ID)
			inDegree[s.ID]++
		}
	}

	queue := make([]string, 0)
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	visited := 0
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		visited++

		for _, next := range graph[node] {
			inDegree[next]--
			if inDegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}

	if visited != len(steps) {
		return fmt.Errorf("circular dependency detected in composition steps")
	}
	return nil
}

// Create creates a new composition from YAML.
func (e *Engine) Create(yamlDef string, createdBy string) (*Composition, error) {
	comp, err := ParseCompositionYAML(yamlDef)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	comp.ID = fmt.Sprintf("comp_%d", e.nextID)
	comp.CreatedBy = createdBy
	comp.CreatedAt = time.Now()

	e.compositions[comp.ID] = comp
	e.logger.Info("composition created", "id", comp.ID, "name", comp.Name, "steps", len(comp.Steps))
	return comp, nil
}

// Execute starts a composition execution.
func (e *Engine) Execute(compID, executedBy string, params map[string]any) (*CompositionExecution, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	comp, ok := e.compositions[compID]
	if !ok {
		return nil, fmt.Errorf("composition %q not found", compID)
	}

	// Validate required params
	for _, p := range comp.Parameters {
		if p.Required {
			if _, ok := params[p.Name]; !ok {
				if p.Default != nil {
					if params == nil {
						params = make(map[string]any)
					}
					params[p.Name] = p.Default
				} else {
					return nil, fmt.Errorf("required parameter %q is missing", p.Name)
				}
			}
		}
	}

	e.nextID++
	exec := &CompositionExecution{
		ID:            fmt.Sprintf("cexec_%d", e.nextID),
		CompositionID: compID,
		Name:          comp.Name,
		Status:        "running",
		Parameters:    params,
		Outputs:       make(map[string]any),
		StartedAt:     time.Now(),
		ExecutedBy:    executedBy,
	}

	for _, step := range comp.Steps {
		args := interpolateStepArgs(step.Args, params)
		exec.Steps = append(exec.Steps, StepExecution{
			StepID:   step.ID,
			StepName: step.Name,
			Tool:     step.Tool,
			Status:   "pending",
		})
		// Store interpolated args in outputs for reference
		exec.Outputs[step.ID+"_args"] = args
	}

	e.executions[exec.ID] = exec
	return exec, nil
}

// Get returns a composition by ID.
func (e *Engine) Get(id string) (*Composition, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	comp, ok := e.compositions[id]
	if !ok {
		return nil, fmt.Errorf("composition %q not found", id)
	}
	return comp, nil
}

// List returns all compositions.
func (e *Engine) List(category string) []Composition {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var result []Composition
	for _, c := range e.compositions {
		if category == "" || c.Category == category {
			result = append(result, *c)
		}
	}
	return result
}

// GetExecution returns an execution by ID.
func (e *Engine) GetExecution(id string) (*CompositionExecution, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	exec, ok := e.executions[id]
	if !ok {
		return nil, fmt.Errorf("execution %q not found", id)
	}
	return exec, nil
}

// Delete removes a composition.
func (e *Engine) Delete(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.compositions[id]; !ok {
		return fmt.Errorf("composition %q not found", id)
	}
	delete(e.compositions, id)
	return nil
}

// Search finds compositions by name or tag.
func (e *Engine) Search(query string) []Composition {
	e.mu.RLock()
	defer e.mu.RUnlock()
	queryLower := strings.ToLower(query)
	var result []Composition
	for _, c := range e.compositions {
		if strings.Contains(strings.ToLower(c.Name), queryLower) ||
			strings.Contains(strings.ToLower(c.Description), queryLower) {
			result = append(result, *c)
			continue
		}
		for _, tag := range c.Tags {
			if strings.Contains(strings.ToLower(tag), queryLower) {
				result = append(result, *c)
				break
			}
		}
	}
	return result
}

func interpolateStepArgs(args map[string]string, params map[string]any) map[string]string {
	if args == nil {
		return nil
	}
	result := make(map[string]string, len(args))
	for k, v := range args {
		for pk, pv := range params {
			v = strings.ReplaceAll(v, fmt.Sprintf("${%s}", pk), fmt.Sprintf("%v", pv))
		}
		result[k] = v
	}
	return result
}
