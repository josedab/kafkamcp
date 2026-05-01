// Package schemaplayground provides an interactive environment for drafting,
// testing, and validating schema changes before committing them.
package schemaplayground

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// DraftStatus tracks the lifecycle of a schema draft.
type DraftStatus string

const (
	StatusDraft     DraftStatus = "draft"
	StatusTesting   DraftStatus = "testing"
	StatusValidated DraftStatus = "validated"
	StatusPromoted  DraftStatus = "promoted"
	StatusRejected  DraftStatus = "rejected"
)

// SchemaType identifies the schema format.
type SchemaType string

const (
	SchemaAvro     SchemaType = "AVRO"
	SchemaProtobuf SchemaType = "PROTOBUF"
	SchemaJSON     SchemaType = "JSON"
)

// Draft represents a schema being worked on in the playground.
type Draft struct {
	ID           string      `json:"id"`
	Subject      string      `json:"subject"`
	SchemaType   SchemaType  `json:"schema_type"`
	Schema       string      `json:"schema"`
	Status       DraftStatus `json:"status"`
	Version      int         `json:"version"`
	CreatedBy    string      `json:"created_by"`
	CreatedAt    time.Time   `json:"created_at"`
	UpdatedAt    time.Time   `json:"updated_at"`
	TestResults  []TestResult `json:"test_results,omitempty"`
	Validations  []Validation `json:"validations,omitempty"`
}

// TestResult from testing a sample message against a schema.
type TestResult struct {
	MessageJSON string    `json:"message_json"`
	Valid       bool      `json:"valid"`
	Errors      []string  `json:"errors,omitempty"`
	FieldMap    []FieldMapping `json:"field_map,omitempty"`
	TestedAt    time.Time `json:"tested_at"`
}

// FieldMapping shows how a field in a message maps to the schema.
type FieldMapping struct {
	Field      string `json:"field"`
	SchemaType string `json:"schema_type"`
	Present    bool   `json:"present"`
	Required   bool   `json:"required"`
	DataLoss   bool   `json:"data_loss"`
	Note       string `json:"note,omitempty"`
}

// Validation represents a compatibility check result.
type Validation struct {
	Against      string   `json:"against"`
	Compatible   bool     `json:"compatible"`
	CompatLevel  string   `json:"compatibility_level"`
	Issues       []string `json:"issues,omitempty"`
	ValidatedAt  time.Time `json:"validated_at"`
}

// EvolutionImpact describes the impact of schema evolution.
type EvolutionImpact struct {
	DraftID       string       `json:"draft_id"`
	Subject       string       `json:"subject"`
	CurrentVersion int         `json:"current_version"`
	NewVersion    int          `json:"new_version"`
	AddedFields   []FieldDiff  `json:"added_fields,omitempty"`
	RemovedFields []FieldDiff  `json:"removed_fields,omitempty"`
	ModifiedFields []FieldDiff `json:"modified_fields,omitempty"`
	BreakingChange bool        `json:"breaking_change"`
	DataLossRisk   bool        `json:"data_loss_risk"`
	Recommendations []string   `json:"recommendations,omitempty"`
}

// FieldDiff describes a field change between schema versions.
type FieldDiff struct {
	Name     string `json:"name"`
	OldType  string `json:"old_type,omitempty"`
	NewType  string `json:"new_type,omitempty"`
	Required bool   `json:"required"`
	HasDefault bool `json:"has_default"`
}

// Engine manages the schema playground.
type Engine struct {
	mu     sync.RWMutex
	drafts map[string]*Draft
	logger *slog.Logger
}

// NewEngine creates a new schema playground engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		drafts: make(map[string]*Draft),
		logger: logger,
	}
}

// CreateDraft starts a new schema draft.
func (e *Engine) CreateDraft(subject string, schemaType SchemaType, schema, createdBy string) (*Draft, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if subject == "" || schema == "" {
		return nil, fmt.Errorf("subject and schema are required")
	}
	if schemaType == "" {
		schemaType = SchemaAvro
	}

	id := randid.New("draft")
	draft := &Draft{
		ID:         id,
		Subject:    subject,
		SchemaType: schemaType,
		Schema:     schema,
		Status:     StatusDraft,
		Version:    1,
		CreatedBy:  createdBy,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	e.drafts[id] = draft
	return draft, nil
}

// UpdateDraft modifies a draft schema.
func (e *Engine) UpdateDraft(draftID, schema string) (*Draft, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	d, ok := e.drafts[draftID]
	if !ok {
		return nil, fmt.Errorf("draft %q not found", draftID)
	}
	if d.Status == StatusPromoted {
		return nil, fmt.Errorf("cannot modify promoted draft")
	}

	d.Schema = schema
	d.Version++
	d.Status = StatusDraft
	d.UpdatedAt = time.Now()
	return d, nil
}

// TestMessage tests a sample message against a draft schema.
func (e *Engine) TestMessage(draftID, messageJSON string) (*TestResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	d, ok := e.drafts[draftID]
	if !ok {
		return nil, fmt.Errorf("draft %q not found", draftID)
	}

	result := &TestResult{
		MessageJSON: messageJSON,
		TestedAt:    time.Now(),
	}

	// Parse the message
	var msg map[string]any
	if err := json.Unmarshal([]byte(messageJSON), &msg); err != nil {
		result.Valid = false
		result.Errors = []string{fmt.Sprintf("invalid JSON: %v", err)}
		d.TestResults = append(d.TestResults, *result)
		d.Status = StatusTesting
		d.UpdatedAt = time.Now()
		return result, nil
	}

	// Extract fields from schema and validate
	schemaFields := extractSchemaFields(d.Schema, d.SchemaType)
	var fieldMaps []FieldMapping
	var errors []string

	for _, sf := range schemaFields {
		_, present := msg[sf.Name]
		fm := FieldMapping{
			Field:      sf.Name,
			SchemaType: sf.Type,
			Present:    present,
			Required:   sf.Required,
		}
		if sf.Required && !present {
			fm.Note = "required field missing"
			errors = append(errors, fmt.Sprintf("required field %q is missing", sf.Name))
		}
		fieldMaps = append(fieldMaps, fm)
	}

	// Check for extra fields in message not in schema
	for key := range msg {
		found := false
		for _, sf := range schemaFields {
			if sf.Name == key {
				found = true
				break
			}
		}
		if !found {
			fieldMaps = append(fieldMaps, FieldMapping{
				Field:    key,
				Present:  true,
				DataLoss: true,
				Note:     "field not in schema - will be dropped on serialization",
			})
		}
	}

	result.Valid = len(errors) == 0
	result.Errors = errors
	result.FieldMap = fieldMaps

	d.TestResults = append(d.TestResults, *result)
	d.Status = StatusTesting
	d.UpdatedAt = time.Now()
	return result, nil
}

// ValidateCompatibility checks compatibility against another schema.
func (e *Engine) ValidateCompatibility(draftID, againstSchema, compatLevel string) (*Validation, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	d, ok := e.drafts[draftID]
	if !ok {
		return nil, fmt.Errorf("draft %q not found", draftID)
	}

	if compatLevel == "" {
		compatLevel = "BACKWARD"
	}

	validation := &Validation{
		Against:     "provided_schema",
		CompatLevel: compatLevel,
		ValidatedAt: time.Now(),
	}

	// Simulate compatibility check
	draftFields := extractSchemaFields(d.Schema, d.SchemaType)
	againstFields := extractSchemaFields(againstSchema, d.SchemaType)

	againstMap := make(map[string]schemaField)
	for _, f := range againstFields {
		againstMap[f.Name] = f
	}

	var issues []string
	for _, af := range againstFields {
		found := false
		for _, df := range draftFields {
			if df.Name == af.Name {
				found = true
				if df.Type != af.Type {
					issues = append(issues, fmt.Sprintf("field %q type changed from %s to %s", af.Name, af.Type, df.Type))
				}
				break
			}
		}
		if !found && af.Required {
			switch compatLevel {
			case "BACKWARD":
				issues = append(issues, fmt.Sprintf("required field %q removed - not backward compatible", af.Name))
			case "FORWARD":
				// Removing fields is OK for forward compat
			case "FULL":
				issues = append(issues, fmt.Sprintf("required field %q removed - not fully compatible", af.Name))
			}
		}
	}

	validation.Compatible = len(issues) == 0
	validation.Issues = issues

	d.Validations = append(d.Validations, *validation)
	if validation.Compatible {
		d.Status = StatusValidated
	}
	d.UpdatedAt = time.Now()

	return validation, nil
}

// SimulateEvolution analyzes the impact of evolving from one schema to another.
func (e *Engine) SimulateEvolution(draftID, currentSchema string) (*EvolutionImpact, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	d, ok := e.drafts[draftID]
	if !ok {
		return nil, fmt.Errorf("draft %q not found", draftID)
	}

	currentFields := extractSchemaFields(currentSchema, d.SchemaType)
	newFields := extractSchemaFields(d.Schema, d.SchemaType)

	currentMap := make(map[string]schemaField)
	for _, f := range currentFields {
		currentMap[f.Name] = f
	}
	newMap := make(map[string]schemaField)
	for _, f := range newFields {
		newMap[f.Name] = f
	}

	impact := &EvolutionImpact{
		DraftID:        draftID,
		Subject:        d.Subject,
		CurrentVersion: d.Version - 1,
		NewVersion:     d.Version,
	}

	// Find added fields
	for _, nf := range newFields {
		if _, exists := currentMap[nf.Name]; !exists {
			impact.AddedFields = append(impact.AddedFields, FieldDiff{
				Name:       nf.Name,
				NewType:    nf.Type,
				Required:   nf.Required,
				HasDefault: nf.HasDefault,
			})
			if nf.Required && !nf.HasDefault {
				impact.BreakingChange = true
			}
		}
	}

	// Find removed fields
	for _, cf := range currentFields {
		if _, exists := newMap[cf.Name]; !exists {
			impact.RemovedFields = append(impact.RemovedFields, FieldDiff{
				Name:     cf.Name,
				OldType:  cf.Type,
				Required: cf.Required,
			})
			impact.DataLossRisk = true
		}
	}

	// Find modified fields
	for _, nf := range newFields {
		if cf, exists := currentMap[nf.Name]; exists && cf.Type != nf.Type {
			impact.ModifiedFields = append(impact.ModifiedFields, FieldDiff{
				Name:    nf.Name,
				OldType: cf.Type,
				NewType: nf.Type,
			})
			impact.BreakingChange = true
		}
	}

	// Generate recommendations
	if impact.BreakingChange {
		impact.Recommendations = append(impact.Recommendations, "This is a breaking change. Consider adding new fields as optional with defaults.")
	}
	if impact.DataLossRisk {
		impact.Recommendations = append(impact.Recommendations, "Removing fields may cause data loss. Deprecate fields before removing them.")
	}
	if !impact.BreakingChange && !impact.DataLossRisk {
		impact.Recommendations = append(impact.Recommendations, "This evolution is safe for both producers and consumers.")
	}

	return impact, nil
}

// PromoteDraft marks a draft as ready for promotion to the registry.
func (e *Engine) PromoteDraft(draftID string) (*Draft, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	d, ok := e.drafts[draftID]
	if !ok {
		return nil, fmt.Errorf("draft %q not found", draftID)
	}
	if d.Status != StatusValidated && d.Status != StatusTesting {
		return nil, fmt.Errorf("draft must be validated or tested before promotion (current: %s)", d.Status)
	}

	d.Status = StatusPromoted
	d.UpdatedAt = time.Now()
	return d, nil
}

// GetDraft retrieves a draft.
func (e *Engine) GetDraft(draftID string) (*Draft, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	d, ok := e.drafts[draftID]
	if !ok {
		return nil, fmt.Errorf("draft %q not found", draftID)
	}
	return d, nil
}

// ListDrafts returns all drafts optionally filtered by status.
func (e *Engine) ListDrafts(status string) []*Draft {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []*Draft
	for _, d := range e.drafts {
		if status == "" || string(d.Status) == status {
			result = append(result, d)
		}
	}
	return result
}

// schemaField is a simplified representation of a field in any schema type.
type schemaField struct {
	Name       string
	Type       string
	Required   bool
	HasDefault bool
}

// extractSchemaFields does a best-effort parse of schema fields.
func extractSchemaFields(schema string, schemaType SchemaType) []schemaField {
	switch schemaType {
	case SchemaAvro:
		return extractAvroFields(schema)
	case SchemaJSON:
		return extractJSONSchemaFields(schema)
	default:
		return extractAvroFields(schema)
	}
}

func extractAvroFields(schema string) []schemaField {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		return nil
	}

	fields, ok := parsed["fields"].([]any)
	if !ok {
		return nil
	}

	var result []schemaField
	for _, f := range fields {
		fm, ok := f.(map[string]any)
		if !ok {
			continue
		}
		sf := schemaField{
			Name: fmt.Sprint(fm["name"]),
		}

		// Determine type
		switch t := fm["type"].(type) {
		case string:
			sf.Type = t
			sf.Required = true
		case []any:
			// Union type - check for null
			types := make([]string, 0)
			for _, ut := range t {
				if s, ok := ut.(string); ok {
					types = append(types, s)
				}
			}
			sf.Type = strings.Join(types, "|")
			for _, tt := range types {
				if tt == "null" {
					sf.Required = false
					break
				}
			}
			if sf.Required {
				sf.Required = true
			}
		}

		_, sf.HasDefault = fm["default"]
		result = append(result, sf)
	}
	return result
}

func extractJSONSchemaFields(schema string) []schemaField {
	var parsed map[string]any
	if err := json.Unmarshal([]byte(schema), &parsed); err != nil {
		return nil
	}

	props, ok := parsed["properties"].(map[string]any)
	if !ok {
		return nil
	}

	requiredFields := make(map[string]bool)
	if req, ok := parsed["required"].([]any); ok {
		for _, r := range req {
			if s, ok := r.(string); ok {
				requiredFields[s] = true
			}
		}
	}

	var result []schemaField
	for name, prop := range props {
		sf := schemaField{
			Name:     name,
			Required: requiredFields[name],
		}
		if pm, ok := prop.(map[string]any); ok {
			if t, ok := pm["type"].(string); ok {
				sf.Type = t
			}
			_, sf.HasDefault = pm["default"]
		}
		result = append(result, sf)
	}
	return result
}
