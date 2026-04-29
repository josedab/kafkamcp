package topictemplate

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// TopicTemplate defines a declarative topic configuration.
type TopicTemplate struct {
	APIVersion string          `json:"api_version" yaml:"api_version"`
	Kind       string          `json:"kind" yaml:"kind"`
	Metadata   TemplateMetadata `json:"metadata" yaml:"metadata"`
	Spec       TopicSpec       `json:"spec" yaml:"spec"`
}

// TemplateMetadata contains template identification.
type TemplateMetadata struct {
	Name        string            `json:"name" yaml:"name"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
	CreatedBy   string            `json:"created_by,omitempty" yaml:"created_by,omitempty"`
	CreatedAt   time.Time         `json:"created_at,omitempty" yaml:"created_at,omitempty"`
}

// TopicSpec defines the desired topic configuration.
type TopicSpec struct {
	TopicName         string            `json:"topic_name" yaml:"topic_name"`
	Partitions        int32             `json:"partitions" yaml:"partitions"`
	ReplicationFactor int16             `json:"replication_factor" yaml:"replication_factor"`
	Configs           map[string]string `json:"configs,omitempty" yaml:"configs,omitempty"`
}

// DiffResult represents differences between template and actual state.
type DiffResult struct {
	TemplateName string       `json:"template_name"`
	TopicName    string       `json:"topic_name"`
	TopicExists  bool         `json:"topic_exists"`
	Changes      []DiffChange `json:"changes"`
	InSync       bool         `json:"in_sync"`
}

// DiffChange is a single configuration difference.
type DiffChange struct {
	Field    string `json:"field"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Action   string `json:"action"` // create, update, no_change
}

// PlanResult represents the execution plan for applying a template.
type PlanResult struct {
	TemplateName string       `json:"template_name"`
	TopicName    string       `json:"topic_name"`
	Action       string       `json:"action"` // create, update, no_change
	Changes      []DiffChange `json:"changes"`
	DryRun       bool         `json:"dry_run"`
}

// ApplyResult represents the result of applying a template.
type ApplyResult struct {
	TemplateName string       `json:"template_name"`
	TopicName    string       `json:"topic_name"`
	Action       string       `json:"action"`
	Changes      []DiffChange `json:"applied_changes"`
	Status       string       `json:"status"`
	Message      string       `json:"message,omitempty"`
}

// Engine manages topic templates.
type Engine struct {
	mu        sync.RWMutex
	templates map[string]*TopicTemplate
	logger    *slog.Logger
	nextID    int64
}

// NewEngine creates a new topic template engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		templates: make(map[string]*TopicTemplate),
		logger:    logger,
	}
}

// ParseTemplate parses a template from YAML or JSON string.
func (e *Engine) ParseTemplate(content, format string) (*TopicTemplate, error) {
	var tmpl TopicTemplate
	switch format {
	case "yaml", "yml":
		if err := yaml.Unmarshal([]byte(content), &tmpl); err != nil {
			return nil, fmt.Errorf("invalid YAML: %w", err)
		}
	case "json", "":
		if err := json.Unmarshal([]byte(content), &tmpl); err != nil {
			return nil, fmt.Errorf("invalid JSON: %w", err)
		}
	default:
		return nil, fmt.Errorf("unsupported format %q (use yaml or json)", format)
	}

	if err := e.validateTemplate(&tmpl); err != nil {
		return nil, err
	}
	return &tmpl, nil
}

func (e *Engine) validateTemplate(tmpl *TopicTemplate) error {
	if tmpl.Spec.TopicName == "" {
		return fmt.Errorf("spec.topic_name is required")
	}
	if tmpl.Spec.Partitions <= 0 {
		return fmt.Errorf("spec.partitions must be > 0")
	}
	if tmpl.Spec.ReplicationFactor <= 0 {
		return fmt.Errorf("spec.replication_factor must be > 0")
	}
	if tmpl.Metadata.Name == "" {
		tmpl.Metadata.Name = tmpl.Spec.TopicName
	}
	if tmpl.APIVersion == "" {
		tmpl.APIVersion = "v1"
	}
	if tmpl.Kind == "" {
		tmpl.Kind = "TopicTemplate"
	}
	return nil
}

// SaveTemplate stores a template.
func (e *Engine) SaveTemplate(tmpl *TopicTemplate, createdBy string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	tmpl.Metadata.CreatedBy = createdBy
	tmpl.Metadata.CreatedAt = time.Now()
	e.templates[tmpl.Metadata.Name] = tmpl
	e.logger.Info("template saved", "name", tmpl.Metadata.Name, "topic", tmpl.Spec.TopicName)
	return nil
}

// GetTemplate retrieves a template by name.
func (e *Engine) GetTemplate(name string) (*TopicTemplate, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	tmpl, ok := e.templates[name]
	if !ok {
		return nil, fmt.Errorf("template %q not found", name)
	}
	return tmpl, nil
}

// ListTemplates returns all stored templates.
func (e *Engine) ListTemplates() []TopicTemplate {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]TopicTemplate, 0, len(e.templates))
	for _, t := range e.templates {
		result = append(result, *t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Metadata.Name < result[j].Metadata.Name })
	return result
}

// DeleteTemplate removes a template.
func (e *Engine) DeleteTemplate(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.templates[name]; !ok {
		return fmt.Errorf("template %q not found", name)
	}
	delete(e.templates, name)
	return nil
}

// ComputeDiff compares a template against actual topic state.
func (e *Engine) ComputeDiff(tmpl *TopicTemplate, actualExists bool, actualPartitions int32, actualReplFactor int16, actualConfigs map[string]string) *DiffResult {
	diff := &DiffResult{
		TemplateName: tmpl.Metadata.Name,
		TopicName:    tmpl.Spec.TopicName,
		TopicExists:  actualExists,
		InSync:       true,
	}

	if !actualExists {
		diff.InSync = false
		diff.Changes = append(diff.Changes, DiffChange{
			Field: "topic", Expected: tmpl.Spec.TopicName, Actual: "<does not exist>", Action: "create",
		})
		return diff
	}

	if tmpl.Spec.Partitions != actualPartitions {
		diff.InSync = false
		diff.Changes = append(diff.Changes, DiffChange{
			Field: "partitions", Expected: fmt.Sprintf("%d", tmpl.Spec.Partitions),
			Actual: fmt.Sprintf("%d", actualPartitions), Action: "update",
		})
	}

	if tmpl.Spec.ReplicationFactor != actualReplFactor {
		diff.InSync = false
		diff.Changes = append(diff.Changes, DiffChange{
			Field: "replication_factor", Expected: fmt.Sprintf("%d", tmpl.Spec.ReplicationFactor),
			Actual: fmt.Sprintf("%d", actualReplFactor), Action: "update",
		})
	}

	for key, expected := range tmpl.Spec.Configs {
		actual, exists := actualConfigs[key]
		if !exists {
			diff.InSync = false
			diff.Changes = append(diff.Changes, DiffChange{
				Field: "config." + key, Expected: expected, Actual: "<not set>", Action: "update",
			})
		} else if actual != expected {
			diff.InSync = false
			diff.Changes = append(diff.Changes, DiffChange{
				Field: "config." + key, Expected: expected, Actual: actual, Action: "update",
			})
		}
	}

	if len(diff.Changes) == 0 {
		diff.Changes = append(diff.Changes, DiffChange{
			Field: "*", Expected: "", Actual: "", Action: "no_change",
		})
	}

	return diff
}

// ComputePlan generates a plan from a diff result.
func (e *Engine) ComputePlan(diff *DiffResult) *PlanResult {
	action := "no_change"
	if !diff.TopicExists {
		action = "create"
	} else if !diff.InSync {
		action = "update"
	}

	return &PlanResult{
		TemplateName: diff.TemplateName,
		TopicName:    diff.TopicName,
		Action:       action,
		Changes:      diff.Changes,
		DryRun:       true,
	}
}

// ExportTemplate exports a template to YAML or JSON.
func (e *Engine) ExportTemplate(tmpl *TopicTemplate, format string) (string, error) {
	switch format {
	case "yaml", "yml":
		data, err := yaml.Marshal(tmpl)
		if err != nil {
			return "", fmt.Errorf("marshal YAML: %w", err)
		}
		return string(data), nil
	case "json", "":
		data, err := json.MarshalIndent(tmpl, "", "  ")
		if err != nil {
			return "", fmt.Errorf("marshal JSON: %w", err)
		}
		return string(data), nil
	default:
		return "", fmt.Errorf("unsupported format %q", format)
	}
}
