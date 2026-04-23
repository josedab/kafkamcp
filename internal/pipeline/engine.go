// Package pipeline provides agent-defined filter/map/enrich micro-pipelines
// for lightweight stream processing on Kafka messages via MCP tools.
package pipeline

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Engine manages streaming function pipelines.
type Engine struct {
	mu        sync.RWMutex
	pipelines map[string]*Pipeline
	runs      map[string]*PipelineRun
	logger    *slog.Logger
	nextID    int64
}

// Pipeline defines a streaming function pipeline.
type Pipeline struct {
	ID          string     `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	SourceTopic string     `json:"source_topic"`
	SinkTopic   string     `json:"sink_topic,omitempty"`
	Steps       []Step     `json:"steps"`
	CreatedBy   string     `json:"created_by"`
	CreatedAt   time.Time  `json:"created_at"`
	Status      string     `json:"status"` // active, paused, stopped
}

// Step is a single processing step in a pipeline.
type Step struct {
	Type       string            `json:"type"` // filter, map, enrich, transform, aggregate
	Name       string            `json:"name"`
	Config     map[string]string `json:"config"`
	Order      int               `json:"order"`
}

// PipelineRun tracks the execution of a pipeline on a batch of messages.
type PipelineRun struct {
	ID           string    `json:"id"`
	PipelineID   string    `json:"pipeline_id"`
	InputCount   int       `json:"input_count"`
	OutputCount  int       `json:"output_count"`
	FilteredOut  int       `json:"filtered_out"`
	ErrorCount   int       `json:"error_count"`
	Duration     string    `json:"duration"`
	StartedAt    time.Time `json:"started_at"`
	CompletedAt  time.Time `json:"completed_at"`
	Status       string    `json:"status"` // running, completed, failed
}

// PipelineMessage is a message flowing through the pipeline.
type PipelineMessage struct {
	Key       string            `json:"key,omitempty"`
	Value     string            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Partition int32             `json:"partition,omitempty"`
	Offset    int64             `json:"offset,omitempty"`
}

// NewEngine creates a new pipeline engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		pipelines: make(map[string]*Pipeline),
		runs:      make(map[string]*PipelineRun),
		logger:    logger,
	}
}

// CreatePipeline registers a new pipeline definition.
func (e *Engine) CreatePipeline(name, sourceTopic, sinkTopic, createdBy string, steps []Step) (*Pipeline, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if name == "" || sourceTopic == "" {
		return nil, fmt.Errorf("name and source_topic are required")
	}
	if len(steps) == 0 {
		return nil, fmt.Errorf("at least one step is required")
	}

	for i := range steps {
		if steps[i].Type == "" {
			return nil, fmt.Errorf("step %d: type is required", i)
		}
		validTypes := map[string]bool{
			"filter": true, "map": true, "enrich": true,
			"transform": true, "aggregate": true,
		}
		if !validTypes[steps[i].Type] {
			return nil, fmt.Errorf("step %d: invalid type %q", i, steps[i].Type)
		}
		steps[i].Order = i + 1
	}

	e.nextID++
	id := fmt.Sprintf("pipe_%d", e.nextID)

	pipeline := &Pipeline{
		ID:          id,
		Name:        name,
		SourceTopic: sourceTopic,
		SinkTopic:   sinkTopic,
		Steps:       steps,
		CreatedBy:   createdBy,
		CreatedAt:   time.Now(),
		Status:      "active",
	}

	e.pipelines[id] = pipeline
	e.logger.Info("pipeline created", "id", id, "name", name, "steps", len(steps))
	return pipeline, nil
}

// Execute runs a pipeline on a batch of messages.
func (e *Engine) Execute(pipelineID string, messages []PipelineMessage) (*PipelineRun, []PipelineMessage, error) {
	e.mu.RLock()
	pipeline, ok := e.pipelines[pipelineID]
	e.mu.RUnlock()

	if !ok {
		return nil, nil, fmt.Errorf("pipeline %q not found", pipelineID)
	}
	if pipeline.Status != "active" {
		return nil, nil, fmt.Errorf("pipeline %q is %s", pipelineID, pipeline.Status)
	}

	start := time.Now()
	e.nextID++
	runID := fmt.Sprintf("run_%d", e.nextID)

	run := &PipelineRun{
		ID:         runID,
		PipelineID: pipelineID,
		InputCount: len(messages),
		StartedAt:  start,
		Status:     "running",
	}

	current := make([]PipelineMessage, len(messages))
	copy(current, messages)

	for _, step := range pipeline.Steps {
		var next []PipelineMessage
		for _, msg := range current {
			result, keep, err := executeStep(step, msg)
			if err != nil {
				run.ErrorCount++
				continue
			}
			if keep {
				next = append(next, result)
			} else {
				run.FilteredOut++
			}
		}
		current = next
	}

	run.OutputCount = len(current)
	run.CompletedAt = time.Now()
	run.Duration = time.Since(start).String()
	run.Status = "completed"

	e.mu.Lock()
	e.runs[runID] = run
	e.mu.Unlock()

	return run, current, nil
}

func executeStep(step Step, msg PipelineMessage) (PipelineMessage, bool, error) {
	switch step.Type {
	case "filter":
		return executeFilter(step, msg)
	case "map":
		return executeMap(step, msg)
	case "enrich":
		return executeEnrich(step, msg)
	case "transform":
		return executeTransform(step, msg)
	case "aggregate":
		return msg, true, nil // Pass through; aggregation handled at batch level
	default:
		return msg, true, nil
	}
}

func executeFilter(step Step, msg PipelineMessage) (PipelineMessage, bool, error) {
	field := step.Config["field"]
	op := step.Config["operator"]
	value := step.Config["value"]

	var target string
	switch field {
	case "key":
		target = msg.Key
	case "value":
		target = msg.Value
	default:
		if msg.Headers != nil {
			target = msg.Headers[field]
		}
	}

	match := false
	switch op {
	case "equals", "eq", "==":
		match = target == value
	case "not_equals", "ne", "!=":
		match = target != value
	case "contains":
		match = strings.Contains(target, value)
	case "not_contains":
		match = !strings.Contains(target, value)
	case "starts_with":
		match = strings.HasPrefix(target, value)
	case "ends_with":
		match = strings.HasSuffix(target, value)
	case "regex", "matches":
		if re, err := regexp.Compile(value); err == nil {
			match = re.MatchString(target)
		}
	case "exists":
		match = target != ""
	case "not_exists":
		match = target == ""
	default:
		match = strings.Contains(target, value) // default to contains
	}

	return msg, match, nil
}

func executeMap(step Step, msg PipelineMessage) (PipelineMessage, bool, error) {
	expr := step.Config["expression"]
	target := step.Config["target"]

	switch expr {
	case "uppercase":
		if target == "key" {
			msg.Key = strings.ToUpper(msg.Key)
		} else {
			msg.Value = strings.ToUpper(msg.Value)
		}
	case "lowercase":
		if target == "key" {
			msg.Key = strings.ToLower(msg.Key)
		} else {
			msg.Value = strings.ToLower(msg.Value)
		}
	case "trim":
		if target == "key" {
			msg.Key = strings.TrimSpace(msg.Key)
		} else {
			msg.Value = strings.TrimSpace(msg.Value)
		}
	case "prefix":
		prefix := step.Config["value"]
		if target == "key" {
			msg.Key = prefix + msg.Key
		} else {
			msg.Value = prefix + msg.Value
		}
	case "replace":
		old := step.Config["old"]
		new := step.Config["new"]
		if target == "key" {
			msg.Key = strings.ReplaceAll(msg.Key, old, new)
		} else {
			msg.Value = strings.ReplaceAll(msg.Value, old, new)
		}
	}

	return msg, true, nil
}

func executeEnrich(step Step, msg PipelineMessage) (PipelineMessage, bool, error) {
	if msg.Headers == nil {
		msg.Headers = make(map[string]string)
	}

	// Add metadata headers
	for k, v := range step.Config {
		if strings.HasPrefix(k, "header_") {
			headerName := strings.TrimPrefix(k, "header_")
			msg.Headers[headerName] = v
		}
	}

	// Add pipeline metadata
	msg.Headers["x-pipeline-processed"] = "true"
	msg.Headers["x-pipeline-step"] = step.Name
	msg.Headers["x-pipeline-timestamp"] = time.Now().UTC().Format(time.RFC3339)

	// JSON enrichment: try to parse value as JSON and add fields
	if addFields := step.Config["add_fields"]; addFields != "" {
		var fields map[string]any
		if err := json.Unmarshal([]byte(addFields), &fields); err == nil {
			var valueMap map[string]any
			if err := json.Unmarshal([]byte(msg.Value), &valueMap); err == nil {
				for k, v := range fields {
					valueMap[k] = v
				}
				if enriched, err := json.Marshal(valueMap); err == nil {
					msg.Value = string(enriched)
				}
			}
		}
	}

	return msg, true, nil
}

func executeTransform(step Step, msg PipelineMessage) (PipelineMessage, bool, error) {
	format := step.Config["format"]

	switch format {
	case "json_to_csv":
		var data map[string]any
		if err := json.Unmarshal([]byte(msg.Value), &data); err == nil {
			var values []string
			for _, v := range data {
				values = append(values, fmt.Sprintf("%v", v))
			}
			msg.Value = strings.Join(values, ",")
		}
	case "wrap_json":
		wrapper := step.Config["wrapper"]
		if wrapper == "" {
			wrapper = "data"
		}
		msg.Value = fmt.Sprintf(`{%q:%s}`, wrapper, msg.Value)
	case "extract_field":
		field := step.Config["field"]
		var data map[string]any
		if err := json.Unmarshal([]byte(msg.Value), &data); err == nil {
			if val, ok := data[field]; ok {
				if b, err := json.Marshal(val); err == nil {
					msg.Value = string(b)
				}
			}
		}
	}

	return msg, true, nil
}

// ListPipelines returns all pipelines.
func (e *Engine) ListPipelines() []Pipeline {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Pipeline
	for _, p := range e.pipelines {
		result = append(result, *p)
	}
	return result
}

// GetPipeline returns a pipeline by ID.
func (e *Engine) GetPipeline(id string) (*Pipeline, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	p, ok := e.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}
	return p, nil
}

// DeletePipeline removes a pipeline.
func (e *Engine) DeletePipeline(id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.pipelines[id]; !ok {
		return fmt.Errorf("pipeline %q not found", id)
	}
	delete(e.pipelines, id)
	return nil
}

// GetRun returns a pipeline run by ID.
func (e *Engine) GetRun(id string) (*PipelineRun, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	r, ok := e.runs[id]
	if !ok {
		return nil, fmt.Errorf("run %q not found", id)
	}
	return r, nil
}

// ListRuns returns recent pipeline runs.
func (e *Engine) ListRuns(pipelineID string, limit int) []PipelineRun {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var result []PipelineRun
	for _, r := range e.runs {
		if pipelineID == "" || r.PipelineID == pipelineID {
			result = append(result, *r)
		}
	}

	if len(result) > limit {
		result = result[len(result)-limit:]
	}
	return result
}
