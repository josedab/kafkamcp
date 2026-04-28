package nlpipeline

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

// PipelineSpec is a generated pipeline specification from natural language.
type PipelineSpec struct {
	ID           string              `json:"id"`
	Name         string              `json:"name"`
	Description  string              `json:"description"`
	OriginalNL   string              `json:"original_nl"`
	SourceTopic  string              `json:"source_topic"`
	SinkTopic    string              `json:"sink_topic,omitempty"`
	Steps        []PipelineStepSpec  `json:"steps"`
	ConnectConfig *ConnectConfig     `json:"connect_config,omitempty"`
	Status       string              `json:"status"` // draft, validated, deployed, stopped
	CreatedBy    string              `json:"created_by"`
	CreatedAt    time.Time           `json:"created_at"`
}

// PipelineStepSpec is a single step in a generated pipeline.
type PipelineStepSpec struct {
	Type       string            `json:"type"` // filter, map, transform, enrich, aggregate, join, sink
	Name       string            `json:"name"`
	Config     map[string]string `json:"config"`
	Order      int               `json:"order"`
}

// ConnectConfig represents a generated Kafka Connect configuration.
type ConnectConfig struct {
	Name           string            `json:"name"`
	ConnectorClass string            `json:"connector_class"`
	TasksMax       int               `json:"tasks_max"`
	Properties     map[string]string `json:"properties"`
}

// ParseResult is the result of parsing a natural language pipeline description.
type ParseResult struct {
	Intent       string   `json:"intent"`       // filter, transform, aggregate, route, sync, etl
	SourceTopic  string   `json:"source_topic"`
	SinkTopic    string   `json:"sink_topic,omitempty"`
	Filters      []string `json:"filters,omitempty"`
	Transforms   []string `json:"transforms,omitempty"`
	Aggregations []string `json:"aggregations,omitempty"`
	Confidence   float64  `json:"confidence"`
	Suggestions  []string `json:"suggestions,omitempty"`
}

// Builder translates natural language into pipeline specifications.
type Builder struct {
	mu        sync.RWMutex
	pipelines map[string]*PipelineSpec
	logger    *slog.Logger
	nextID    int64
}

// NewBuilder creates a new NL pipeline builder.
func NewBuilder(logger *slog.Logger) *Builder {
	return &Builder{
		pipelines: make(map[string]*PipelineSpec),
		logger:    logger,
	}
}

// Parse analyzes a natural language description and extracts pipeline intent.
func (b *Builder) Parse(description string) *ParseResult {
	desc := strings.ToLower(description)
	result := &ParseResult{Confidence: 0.5}

	// Detect intent
	switch {
	case containsAny(desc, "filter", "where", "only", "exclude", "remove"):
		result.Intent = "filter"
		result.Confidence = 0.8
	case containsAny(desc, "transform", "convert", "map", "change", "modify"):
		result.Intent = "transform"
		result.Confidence = 0.8
	case containsAny(desc, "aggregate", "count", "sum", "average", "group by", "window"):
		result.Intent = "aggregate"
		result.Confidence = 0.8
	case containsAny(desc, "route", "split", "branch", "send to", "forward"):
		result.Intent = "route"
		result.Confidence = 0.7
	case containsAny(desc, "sync", "replicate", "mirror", "copy"):
		result.Intent = "sync"
		result.Confidence = 0.7
	case containsAny(desc, "etl", "extract", "load", "pipeline"):
		result.Intent = "etl"
		result.Confidence = 0.7
	default:
		result.Intent = "transform"
		result.Confidence = 0.4
	}

	// Extract topic names
	result.SourceTopic = extractTopic(desc, "from")
	result.SinkTopic = extractTopic(desc, "to")
	if result.SinkTopic == "" {
		result.SinkTopic = extractTopic(desc, "into")
	}

	// Extract filter conditions
	if containsAny(desc, "where", "filter", "only") {
		result.Filters = extractConditions(desc)
	}

	// Extract transformations
	if containsAny(desc, "transform", "convert", "uppercase", "lowercase", "rename") {
		result.Transforms = extractTransforms(desc)
	}

	// Extract aggregations
	if containsAny(desc, "count", "sum", "average", "avg", "min", "max") {
		result.Aggregations = extractAggregations(desc)
	}

	// Generate suggestions
	if result.SourceTopic == "" {
		result.Suggestions = append(result.Suggestions, "Specify a source topic: 'from <topic_name>'")
	}
	if result.Intent == "transform" && len(result.Transforms) == 0 {
		result.Suggestions = append(result.Suggestions, "Specify transformations: 'convert to uppercase', 'rename field X to Y'")
	}

	return result
}

// Generate creates a pipeline specification from a natural language description.
func (b *Builder) Generate(description, createdBy string) (*PipelineSpec, error) {
	if description == "" {
		return nil, fmt.Errorf("description is required")
	}

	parsed := b.Parse(description)

	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextID++
	id := fmt.Sprintf("nlp_%d", b.nextID)

	spec := &PipelineSpec{
		ID:          id,
		Name:        generateName(parsed),
		Description: description,
		OriginalNL:  description,
		SourceTopic: parsed.SourceTopic,
		SinkTopic:   parsed.SinkTopic,
		Status:      "draft",
		CreatedBy:   createdBy,
		CreatedAt:   time.Now(),
	}

	// Generate steps based on parsed intent
	order := 1
	for _, f := range parsed.Filters {
		spec.Steps = append(spec.Steps, PipelineStepSpec{
			Type: "filter", Name: "filter_" + fmt.Sprint(order),
			Config: map[string]string{"expression": f}, Order: order,
		})
		order++
	}
	for _, t := range parsed.Transforms {
		spec.Steps = append(spec.Steps, PipelineStepSpec{
			Type: "transform", Name: "transform_" + fmt.Sprint(order),
			Config: map[string]string{"operation": t}, Order: order,
		})
		order++
	}
	for _, a := range parsed.Aggregations {
		spec.Steps = append(spec.Steps, PipelineStepSpec{
			Type: "aggregate", Name: "aggregate_" + fmt.Sprint(order),
			Config: map[string]string{"function": a}, Order: order,
		})
		order++
	}

	// If no steps were generated, create default based on intent
	if len(spec.Steps) == 0 {
		spec.Steps = append(spec.Steps, PipelineStepSpec{
			Type: parsed.Intent, Name: parsed.Intent + "_1",
			Config: map[string]string{"description": description}, Order: 1,
		})
	}

	// Generate Connect config for sync/etl intents
	if parsed.Intent == "sync" || parsed.Intent == "etl" {
		spec.ConnectConfig = generateConnectConfig(spec)
	}

	b.pipelines[id] = spec
	b.logger.Info("NL pipeline generated", "id", id, "intent", parsed.Intent, "steps", len(spec.Steps))
	return spec, nil
}

// Validate checks if a pipeline spec is valid and deployable.
func (b *Builder) Validate(id string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	spec, ok := b.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}

	var issues []string
	if spec.SourceTopic == "" {
		issues = append(issues, "Source topic is not specified")
	}
	if len(spec.Steps) == 0 {
		issues = append(issues, "No processing steps defined")
	}
	for _, step := range spec.Steps {
		if step.Type == "" {
			issues = append(issues, fmt.Sprintf("Step %q has no type", step.Name))
		}
	}

	if len(issues) == 0 {
		spec.Status = "validated"
	}
	return issues, nil
}

// Deploy marks a pipeline as deployed.
func (b *Builder) Deploy(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	spec, ok := b.pipelines[id]
	if !ok {
		return fmt.Errorf("pipeline %q not found", id)
	}
	if spec.Status != "validated" && spec.Status != "draft" {
		return fmt.Errorf("pipeline must be in draft or validated status to deploy")
	}
	spec.Status = "deployed"
	return nil
}

// GetPipeline returns a pipeline by ID.
func (b *Builder) GetPipeline(id string) (*PipelineSpec, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	spec, ok := b.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}
	return spec, nil
}

// ListPipelines returns all pipelines.
func (b *Builder) ListPipelines() []PipelineSpec {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]PipelineSpec, 0, len(b.pipelines))
	for _, p := range b.pipelines {
		result = append(result, *p)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	return result
}

// DeletePipeline removes a pipeline.
func (b *Builder) DeletePipeline(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.pipelines[id]; !ok {
		return fmt.Errorf("pipeline %q not found", id)
	}
	delete(b.pipelines, id)
	return nil
}

func containsAny(s string, words ...string) bool {
	for _, w := range words {
		if strings.Contains(s, w) {
			return true
		}
	}
	return false
}

func extractTopic(desc, preposition string) string {
	idx := strings.Index(desc, preposition+" ")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(desc[idx+len(preposition)+1:])
	// Take the next word (topic name)
	parts := strings.Fields(rest)
	if len(parts) > 0 {
		return strings.Trim(parts[0], "'\"`,.")
	}
	return ""
}

func extractConditions(desc string) []string {
	var conditions []string
	markers := []string{"where ", "filter by ", "only "}
	for _, m := range markers {
		idx := strings.Index(desc, m)
		if idx >= 0 {
			rest := desc[idx+len(m):]
			// Take until next keyword or end
			endIdx := strings.IndexAny(rest, ".;")
			if endIdx > 0 {
				rest = rest[:endIdx]
			}
			conditions = append(conditions, strings.TrimSpace(rest))
		}
	}
	return conditions
}

func extractTransforms(desc string) []string {
	var transforms []string
	if strings.Contains(desc, "uppercase") {
		transforms = append(transforms, "uppercase")
	}
	if strings.Contains(desc, "lowercase") {
		transforms = append(transforms, "lowercase")
	}
	if strings.Contains(desc, "rename") {
		transforms = append(transforms, "rename")
	}
	if strings.Contains(desc, "flatten") {
		transforms = append(transforms, "flatten")
	}
	if len(transforms) == 0 {
		transforms = append(transforms, "passthrough")
	}
	return transforms
}

func extractAggregations(desc string) []string {
	var aggs []string
	for _, a := range []string{"count", "sum", "average", "avg", "min", "max"} {
		if strings.Contains(desc, a) {
			aggs = append(aggs, a)
		}
	}
	return aggs
}

func generateName(parsed *ParseResult) string {
	parts := []string{parsed.Intent}
	if parsed.SourceTopic != "" {
		parts = append(parts, parsed.SourceTopic)
	}
	if parsed.SinkTopic != "" {
		parts = append(parts, "to", parsed.SinkTopic)
	}
	return strings.Join(parts, "-")
}

func generateConnectConfig(spec *PipelineSpec) *ConnectConfig {
	return &ConnectConfig{
		Name:           spec.Name + "-connector",
		ConnectorClass: "org.apache.kafka.connect.mirror.MirrorSourceConnector",
		TasksMax:       1,
		Properties: map[string]string{
			"topics":             spec.SourceTopic,
			"source.cluster":    "source",
			"target.cluster":    "target",
			"replication.factor": "1",
		},
	}
}
