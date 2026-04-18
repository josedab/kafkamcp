package workflow

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// Template represents a reusable workflow template.
type Template struct {
	Name        string            `json:"name" yaml:"name"`
	Description string            `json:"description" yaml:"description"`
	Category    string            `json:"category" yaml:"category"`
	Parameters  []Parameter       `json:"parameters" yaml:"parameters"`
	Steps       []Step            `json:"steps" yaml:"steps"`
	Tags        []string          `json:"tags,omitempty" yaml:"tags,omitempty"`
}

// Parameter defines a template input parameter.
type Parameter struct {
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description" yaml:"description"`
	Type        string `json:"type" yaml:"type"` // string, number, boolean
	Required    bool   `json:"required" yaml:"required"`
	Default     any    `json:"default,omitempty" yaml:"default,omitempty"`
}

// Step is a single workflow step.
type Step struct {
	ID        string            `json:"id" yaml:"id"`
	Name      string            `json:"name" yaml:"name"`
	Tool      string            `json:"tool" yaml:"tool"`
	Args      map[string]string `json:"args" yaml:"args"` // supports ${variable} interpolation
	Condition string            `json:"condition,omitempty" yaml:"condition,omitempty"`
	OnError   string            `json:"on_error,omitempty" yaml:"on_error,omitempty"` // continue, stop, skip_to
}

// Execution tracks a running workflow.
type Execution struct {
	ID           string         `json:"id"`
	TemplateName string         `json:"template_name"`
	Status       string         `json:"status"` // running, completed, failed, cancelled
	CurrentStep  int            `json:"current_step"`
	TotalSteps   int            `json:"total_steps"`
	Parameters   map[string]any `json:"parameters"`
	Results      []StepResult   `json:"results"`
	StartedAt    time.Time      `json:"started_at"`
	CompletedAt  *time.Time     `json:"completed_at,omitempty"`
	Error        string         `json:"error,omitempty"`
}

// StepResult records the outcome of a single step.
type StepResult struct {
	StepID    string    `json:"step_id"`
	StepName  string    `json:"step_name"`
	Tool      string    `json:"tool"`
	Status    string    `json:"status"` // success, failed, skipped
	Output    any       `json:"output,omitempty"`
	Error     string    `json:"error,omitempty"`
	Duration  string    `json:"duration"`
	Timestamp time.Time `json:"timestamp"`
}

// Engine manages workflow templates and executions.
type Engine struct {
	templates  map[string]Template
	executions map[string]*Execution
	mu         sync.RWMutex
	logger     *slog.Logger
}

// NewEngine creates a workflow engine with built-in templates.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		templates:  make(map[string]Template),
		executions: make(map[string]*Execution),
		logger:     logger,
	}
	e.registerBuiltinTemplates()
	return e
}

func (e *Engine) registerBuiltinTemplates() {
	builtins := []Template{
		{
			Name:        "dlq-investigation",
			Description: "Investigate dead-letter queue: analyze errors, identify patterns, suggest fixes",
			Category:    "operations",
			Parameters: []Parameter{
				{Name: "topic", Description: "DLQ topic name", Type: "string", Required: true},
				{Name: "limit", Description: "Max messages to analyze", Type: "number", Default: 100},
			},
			Steps: []Step{
				{ID: "analyze", Name: "Analyze DLQ messages", Tool: "kafka_analyze_dlq", Args: map[string]string{"topic": "${topic}", "limit": "${limit}"}},
				{ID: "describe", Name: "Get DLQ topic details", Tool: "kafka_describe_topic", Args: map[string]string{"topic": "${topic}"}},
				{ID: "recommend", Name: "Get optimization recommendations", Tool: "kafka_recommend", Args: map[string]string{"topic": "${topic}"}},
			},
			Tags: []string{"dlq", "debugging", "operations"},
		},
		{
			Name:        "capacity-planning",
			Description: "Analyze topic capacity: throughput, storage, partition utilization, growth projections",
			Category:    "planning",
			Parameters: []Parameter{
				{Name: "topic", Description: "Topic to analyze", Type: "string", Required: true},
			},
			Steps: []Step{
				{ID: "describe", Name: "Get topic metadata", Tool: "kafka_describe_topic", Args: map[string]string{"topic": "${topic}"}},
				{ID: "aggregate", Name: "Measure throughput", Tool: "kafka_aggregate", Args: map[string]string{"topic": "${topic}", "function": "count", "window_ms": "300000"}},
				{ID: "recommend", Name: "Get recommendations", Tool: "kafka_recommend", Args: map[string]string{"topic": "${topic}"}},
				{ID: "cost", Name: "Analyze costs", Tool: "kafka_cost_analyze", Args: map[string]string{"topic": "${topic}"}},
			},
			Tags: []string{"capacity", "planning", "cost"},
		},
		{
			Name:        "incident-response",
			Description: "Rapid incident response: check consumer lag, errors, throughput anomalies",
			Category:    "operations",
			Parameters: []Parameter{
				{Name: "topic", Description: "Affected topic", Type: "string", Required: true},
				{Name: "group_id", Description: "Consumer group to check", Type: "string", Required: false},
			},
			Steps: []Step{
				{ID: "describe", Name: "Check topic health", Tool: "kafka_describe_topic", Args: map[string]string{"topic": "${topic}"}},
				{ID: "lag", Name: "Check consumer lag", Tool: "kafka_describe_consumer_group", Args: map[string]string{"group_id": "${group_id}"}, Condition: "${group_id}"},
				{ID: "anomalies", Name: "Check for anomalies", Tool: "kafka_anomalies", Args: map[string]string{"metric": "consumer_lag"}},
				{ID: "recent", Name: "Sample recent messages", Tool: "kafka_consume", Args: map[string]string{"topic": "${topic}", "offset": "latest", "limit": "5"}},
			},
			Tags: []string{"incident", "operations", "monitoring"},
		},
		{
			Name:        "schema-migration",
			Description: "Guide schema evolution: check compatibility, diff versions, suggest migration path",
			Category:    "development",
			Parameters: []Parameter{
				{Name: "subject", Description: "Schema subject name", Type: "string", Required: true},
				{Name: "change", Description: "Description of desired change", Type: "string", Required: true},
			},
			Steps: []Step{
				{ID: "current", Name: "Get current schema", Tool: "kafka_get_schema", Args: map[string]string{"subject": "${subject}"}},
				{ID: "suggest", Name: "Suggest evolution strategy", Tool: "kafka_suggest_schema_evolution", Args: map[string]string{"subject": "${subject}", "change": "${change}"}},
			},
			Tags: []string{"schema", "migration", "development"},
		},
		{
			Name:        "topic-health-check",
			Description: "Comprehensive topic health check: partitions, replicas, lag, throughput",
			Category:    "monitoring",
			Parameters: []Parameter{
				{Name: "topic", Description: "Topic to check", Type: "string", Required: true},
			},
			Steps: []Step{
				{ID: "describe", Name: "Get topic details", Tool: "kafka_describe_topic", Args: map[string]string{"topic": "${topic}"}},
				{ID: "throughput", Name: "Check throughput", Tool: "kafka_aggregate", Args: map[string]string{"topic": "${topic}", "function": "count", "window_ms": "60000"}},
				{ID: "recommend", Name: "Get recommendations", Tool: "kafka_recommend", Args: map[string]string{"topic": "${topic}"}},
			},
			Tags: []string{"health", "monitoring"},
		},
		{
			Name:        "consumer-group-audit",
			Description: "Audit consumer group: lag, member balance, partition assignment, stale consumers",
			Category:    "operations",
			Parameters: []Parameter{
				{Name: "group_id", Description: "Consumer group ID", Type: "string", Required: true},
			},
			Steps: []Step{
				{ID: "describe", Name: "Describe consumer group", Tool: "kafka_describe_consumer_group", Args: map[string]string{"group_id": "${group_id}"}},
				{ID: "anomalies", Name: "Check lag anomalies", Tool: "kafka_anomalies", Args: map[string]string{"metric": "consumer_lag"}},
			},
			Tags: []string{"consumer", "audit", "operations"},
		},
		{
			Name:        "cluster-overview",
			Description: "Get a full overview of the Kafka cluster: brokers, topics, groups, schemas",
			Category:    "discovery",
			Parameters:  []Parameter{},
			Steps: []Step{
				{ID: "topics", Name: "List all topics", Tool: "kafka_list_topics", Args: map[string]string{}},
				{ID: "groups", Name: "List consumer groups", Tool: "kafka_list_consumer_groups", Args: map[string]string{}},
				{ID: "schemas", Name: "List schemas", Tool: "kafka_list_schemas", Args: map[string]string{}},
			},
			Tags: []string{"cluster", "discovery", "overview"},
		},
		{
			Name:        "data-quality-check",
			Description: "Check data quality: sample messages, validate schema, check for null/empty fields",
			Category:    "quality",
			Parameters: []Parameter{
				{Name: "topic", Description: "Topic to check", Type: "string", Required: true},
				{Name: "sample_size", Description: "Number of messages to sample", Type: "number", Default: 20},
			},
			Steps: []Step{
				{ID: "sample", Name: "Sample messages", Tool: "kafka_consume", Args: map[string]string{"topic": "${topic}", "offset": "latest", "limit": "${sample_size}"}},
				{ID: "describe", Name: "Check topic config", Tool: "kafka_describe_topic", Args: map[string]string{"topic": "${topic}"}},
			},
			Tags: []string{"quality", "validation", "data"},
		},
		{
			Name:        "cross-cluster-compare",
			Description: "Compare topic configurations across clusters for config drift detection",
			Category:    "operations",
			Parameters: []Parameter{
				{Name: "topic", Description: "Topic to compare", Type: "string", Required: true},
				{Name: "source_cluster", Description: "Source cluster name", Type: "string", Required: true},
				{Name: "target_cluster", Description: "Target cluster name", Type: "string", Required: true},
			},
			Steps: []Step{
				{ID: "drift", Name: "Compare configs", Tool: "kafka_config_drift", Args: map[string]string{"topic": "${topic}", "source_cluster": "${source_cluster}", "target_cluster": "${target_cluster}"}},
			},
			Tags: []string{"drift", "multi-cluster", "operations"},
		},
		{
			Name:        "message-trace",
			Description: "Trace a message across topics using correlation ID for end-to-end debugging",
			Category:    "debugging",
			Parameters: []Parameter{
				{Name: "correlation_id", Description: "Correlation/trace ID to follow", Type: "string", Required: true},
				{Name: "header_name", Description: "Header containing the correlation ID", Type: "string", Default: "correlation-id"},
			},
			Steps: []Step{
				{ID: "trace", Name: "Trace message flow", Tool: "kafka_trace_message", Args: map[string]string{"correlation_id": "${correlation_id}", "header_name": "${header_name}"}},
			},
			Tags: []string{"tracing", "debugging", "correlation"},
		},
	}

	for _, t := range builtins {
		e.templates[t.Name] = t
	}
}

// ListTemplates returns all available templates, optionally filtered by category.
func (e *Engine) ListTemplates(category string) []Template {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Template
	for _, t := range e.templates {
		if category == "" || t.Category == category {
			result = append(result, t)
		}
	}
	return result
}

// GetTemplate returns a template by name.
func (e *Engine) GetTemplate(name string) (*Template, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	t, ok := e.templates[name]
	if !ok {
		return nil, fmt.Errorf("workflow template %q not found", name)
	}
	return &t, nil
}

// RegisterTemplate adds a custom template.
func (e *Engine) RegisterTemplate(t Template) error {
	if t.Name == "" {
		return fmt.Errorf("template name is required")
	}
	if len(t.Steps) == 0 {
		return fmt.Errorf("template must have at least one step")
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.templates[t.Name] = t
	return nil
}

// StartExecution creates an execution plan from a template with resolved parameters.
func (e *Engine) StartExecution(templateName string, params map[string]any) (*Execution, error) {
	tmpl, err := e.GetTemplate(templateName)
	if err != nil {
		return nil, err
	}

	// Validate required parameters
	for _, p := range tmpl.Parameters {
		if p.Required {
			if _, ok := params[p.Name]; !ok {
				if p.Default != nil {
					params[p.Name] = p.Default
				} else {
					return nil, fmt.Errorf("required parameter %q not provided", p.Name)
				}
			}
		} else if _, ok := params[p.Name]; !ok && p.Default != nil {
			params[p.Name] = p.Default
		}
	}

	exec := &Execution{
		ID:           fmt.Sprintf("exec-%s-%d", templateName, time.Now().UnixMilli()),
		TemplateName: templateName,
		Status:       "running",
		CurrentStep:  0,
		TotalSteps:   len(tmpl.Steps),
		Parameters:   params,
		StartedAt:    time.Now(),
	}

	e.mu.Lock()
	e.executions[exec.ID] = exec
	e.mu.Unlock()

	return exec, nil
}

// ResolveStepArgs interpolates parameters into step arguments.
func (e *Engine) ResolveStepArgs(step Step, params map[string]any) map[string]any {
	resolved := make(map[string]any, len(step.Args))
	for k, v := range step.Args {
		resolved[k] = interpolate(v, params)
	}
	return resolved
}

// CompleteStep records the result of a step.
func (e *Engine) CompleteStep(execID string, result StepResult) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	exec, ok := e.executions[execID]
	if !ok {
		return fmt.Errorf("execution %q not found", execID)
	}

	exec.Results = append(exec.Results, result)
	exec.CurrentStep++

	if result.Status == "failed" && e.getStepOnError(exec) == "stop" {
		exec.Status = "failed"
		exec.Error = result.Error
		now := time.Now()
		exec.CompletedAt = &now
	} else if exec.CurrentStep >= exec.TotalSteps {
		exec.Status = "completed"
		now := time.Now()
		exec.CompletedAt = &now
	}

	return nil
}

func (e *Engine) getStepOnError(exec *Execution) string {
	tmpl, err := e.GetTemplate(exec.TemplateName)
	if err != nil || exec.CurrentStep-1 < 0 || exec.CurrentStep-1 >= len(tmpl.Steps) {
		return "stop"
	}
	onErr := tmpl.Steps[exec.CurrentStep-1].OnError
	if onErr == "" {
		return "continue"
	}
	return onErr
}

// GetExecution returns an execution by ID.
func (e *Engine) GetExecution(execID string) (*Execution, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	exec, ok := e.executions[execID]
	if !ok {
		return nil, fmt.Errorf("execution %q not found", execID)
	}
	return exec, nil
}

// GetNextStep returns the next step to execute, if any.
func (e *Engine) GetNextStep(execID string) (*Step, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	exec, ok := e.executions[execID]
	if !ok {
		return nil, fmt.Errorf("execution %q not found", execID)
	}

	tmpl, ok := e.templates[exec.TemplateName]
	if !ok {
		return nil, fmt.Errorf("template %q not found", exec.TemplateName)
	}

	if exec.CurrentStep >= len(tmpl.Steps) {
		return nil, nil // all steps completed
	}

	step := tmpl.Steps[exec.CurrentStep]

	// Check condition
	if step.Condition != "" {
		resolved := interpolate(step.Condition, exec.Parameters)
		if resolved == "" || resolved == step.Condition {
			// Condition not met - skip this step
			return nil, nil
		}
	}

	return &step, nil
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
