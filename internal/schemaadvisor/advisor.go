package schemaadvisor

import (
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"
)

// CompatibilityLevel represents schema compatibility mode.
type CompatibilityLevel string

const (
	CompatBackward     CompatibilityLevel = "BACKWARD"
	CompatForward      CompatibilityLevel = "FORWARD"
	CompatFull         CompatibilityLevel = "FULL"
	CompatNone         CompatibilityLevel = "NONE"
	CompatTransitive   CompatibilityLevel = "BACKWARD_TRANSITIVE"
)

// SchemaVersion represents a version in schema history.
type SchemaVersion struct {
	Version     int               `json:"version"`
	Schema      string            `json:"schema"`
	SchemaType  string            `json:"schema_type"` // AVRO, PROTOBUF, JSON
	Subject     string            `json:"subject"`
	Timestamp   time.Time         `json:"timestamp"`
	Fields      []FieldInfo       `json:"fields,omitempty"`
}

// FieldInfo describes a field in a schema.
type FieldInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required"`
	Default  any    `json:"default,omitempty"`
	Doc      string `json:"doc,omitempty"`
}

// CompatibilityPrediction is the result of predicting compatibility issues.
type CompatibilityPrediction struct {
	Subject          string             `json:"subject"`
	CurrentVersion   int                `json:"current_version"`
	Compatibility    CompatibilityLevel `json:"compatibility_level"`
	IsCompatible     bool               `json:"is_compatible"`
	BreakingChanges  []BreakingChange   `json:"breaking_changes,omitempty"`
	Warnings         []string           `json:"warnings,omitempty"`
	Suggestions      []string           `json:"suggestions,omitempty"`
	RiskLevel        string             `json:"risk_level"` // low, medium, high, critical
	AffectedConsumers int              `json:"affected_consumers"`
}

// BreakingChange describes a specific compatibility issue.
type BreakingChange struct {
	Field       string `json:"field"`
	ChangeType  string `json:"change_type"` // removed, type_changed, required_added, enum_shrunk
	Description string `json:"description"`
	Severity    string `json:"severity"` // warning, error, critical
	Mitigation  string `json:"mitigation"`
}

// MigrationPlan describes how to evolve a schema from one version to another.
type MigrationPlan struct {
	Subject      string           `json:"subject"`
	FromVersion  int              `json:"from_version"`
	ToVersion    int              `json:"to_version"`
	Steps        []MigrationStep  `json:"steps"`
	EstimatedRisk string          `json:"estimated_risk"`
	Rollback     string           `json:"rollback_strategy"`
	Prerequisites []string        `json:"prerequisites,omitempty"`
}

// MigrationStep is a single step in a migration plan.
type MigrationStep struct {
	Order       int    `json:"order"`
	Action      string `json:"action"`
	Description string `json:"description"`
	Schema      string `json:"schema,omitempty"`
	Reversible  bool   `json:"reversible"`
}

// EvolutionAdvice provides recommendations based on schema history.
type EvolutionAdvice struct {
	Subject        string            `json:"subject"`
	SchemaType     string            `json:"schema_type"`
	VersionCount   int               `json:"version_count"`
	Patterns       []EvolutionPattern `json:"patterns"`
	BestPractices  []string          `json:"best_practices"`
	Antipatterns   []string          `json:"antipatterns,omitempty"`
	HealthScore    float64           `json:"health_score"` // 0-100
}

// EvolutionPattern describes a detected pattern in schema evolution.
type EvolutionPattern struct {
	Pattern     string `json:"pattern"`
	Frequency   int    `json:"frequency"`
	Impact      string `json:"impact"` // positive, neutral, negative
	Description string `json:"description"`
}

// Advisor analyzes schema registry history and provides evolution recommendations.
type Advisor struct {
	mu       sync.RWMutex
	history  map[string][]SchemaVersion // subject -> versions
	advice   map[string]*EvolutionAdvice
	logger   *slog.Logger
}

// NewAdvisor creates a new schema evolution advisor.
func NewAdvisor(logger *slog.Logger) *Advisor {
	return &Advisor{
		history: make(map[string][]SchemaVersion),
		advice:  make(map[string]*EvolutionAdvice),
		logger:  logger,
	}
}

// RecordVersion records a schema version in history for analysis.
func (a *Advisor) RecordVersion(subject string, version int, schema, schemaType string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	sv := SchemaVersion{
		Version:    version,
		Schema:     schema,
		SchemaType: schemaType,
		Subject:    subject,
		Timestamp:  time.Now(),
		Fields:     extractFieldsFromSchema(schema, schemaType),
	}

	history := a.history[subject]
	// Avoid duplicates
	for _, existing := range history {
		if existing.Version == version {
			return
		}
	}
	history = append(history, sv)
	sort.Slice(history, func(i, j int) bool { return history[i].Version < history[j].Version })
	a.history[subject] = history
}

// PredictCompatibility predicts compatibility issues for a proposed schema change.
func (a *Advisor) PredictCompatibility(subject, newSchema, schemaType string, compatLevel CompatibilityLevel) *CompatibilityPrediction {
	a.mu.RLock()
	defer a.mu.RUnlock()

	history := a.history[subject]
	prediction := &CompatibilityPrediction{
		Subject:       subject,
		Compatibility: compatLevel,
		IsCompatible:  true,
		RiskLevel:     "low",
	}

	if len(history) == 0 {
		prediction.Suggestions = append(prediction.Suggestions, "First version — no compatibility constraints")
		return prediction
	}

	currentVersion := history[len(history)-1]
	prediction.CurrentVersion = currentVersion.Version

	newFields := extractFieldsFromSchema(newSchema, schemaType)
	oldFields := currentVersion.Fields

	// Build field maps
	oldMap := make(map[string]FieldInfo)
	for _, f := range oldFields {
		oldMap[f.Name] = f
	}
	newMap := make(map[string]FieldInfo)
	for _, f := range newFields {
		newMap[f.Name] = f
	}

	// Detect breaking changes based on compatibility level
	for name, oldField := range oldMap {
		if _, exists := newMap[name]; !exists {
			bc := BreakingChange{
				Field:      name,
				ChangeType: "removed",
				Severity:   "error",
				Description: fmt.Sprintf("Field %q was removed", name),
				Mitigation: fmt.Sprintf("Add field %q back with a default value, or use NONE compatibility", name),
			}
			if compatLevel == CompatBackward || compatLevel == CompatFull || compatLevel == CompatTransitive {
				prediction.IsCompatible = false
				prediction.BreakingChanges = append(prediction.BreakingChanges, bc)
			} else {
				bc.Severity = "warning"
				prediction.Warnings = append(prediction.Warnings, fmt.Sprintf("Field %q removed — may break old consumers", name))
			}
		} else if newMap[name].Type != oldField.Type {
			prediction.BreakingChanges = append(prediction.BreakingChanges, BreakingChange{
				Field:       name,
				ChangeType:  "type_changed",
				Severity:    "critical",
				Description: fmt.Sprintf("Field %q type changed from %s to %s", name, oldField.Type, newMap[name].Type),
				Mitigation:  "Add a new field with the desired type and deprecate the old one",
			})
			prediction.IsCompatible = false
		}
	}

	for name, newField := range newMap {
		if _, exists := oldMap[name]; !exists {
			if newField.Required && newField.Default == nil {
				if compatLevel == CompatForward || compatLevel == CompatFull {
					prediction.BreakingChanges = append(prediction.BreakingChanges, BreakingChange{
						Field:       name,
						ChangeType:  "required_added",
						Severity:    "error",
						Description: fmt.Sprintf("New required field %q without default", name),
						Mitigation:  "Add a default value for the new field",
					})
					prediction.IsCompatible = false
				}
			} else {
				prediction.Suggestions = append(prediction.Suggestions, fmt.Sprintf("New optional field %q added — safe change", name))
			}
		}
	}

	// Compute risk level
	criticalCount := 0
	for _, bc := range prediction.BreakingChanges {
		if bc.Severity == "critical" {
			criticalCount++
		}
	}
	switch {
	case criticalCount > 0:
		prediction.RiskLevel = "critical"
	case len(prediction.BreakingChanges) > 2:
		prediction.RiskLevel = "high"
	case len(prediction.BreakingChanges) > 0:
		prediction.RiskLevel = "medium"
	default:
		prediction.RiskLevel = "low"
	}

	return prediction
}

// GenerateMigrationPlan creates a step-by-step migration plan.
func (a *Advisor) GenerateMigrationPlan(subject string, fromVersion, toVersion int) (*MigrationPlan, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	history, ok := a.history[subject]
	if !ok || len(history) == 0 {
		return nil, fmt.Errorf("no schema history for subject %q", subject)
	}

	if fromVersion >= toVersion {
		return nil, fmt.Errorf("from_version must be less than to_version")
	}

	plan := &MigrationPlan{
		Subject:     subject,
		FromVersion: fromVersion,
		ToVersion:   toVersion,
		Rollback:    "Revert consumer config to previous schema version; old producers can continue with BACKWARD compatibility",
	}

	order := 1
	plan.Steps = append(plan.Steps, MigrationStep{
		Order:       order,
		Action:      "validate",
		Description: "Run compatibility check against schema registry",
		Reversible:  true,
	})
	order++

	plan.Steps = append(plan.Steps, MigrationStep{
		Order:       order,
		Action:      "backup",
		Description: fmt.Sprintf("Backup current schema version %d for rollback", fromVersion),
		Reversible:  true,
	})
	order++

	plan.Steps = append(plan.Steps, MigrationStep{
		Order:       order,
		Action:      "register",
		Description: fmt.Sprintf("Register new schema version %d in schema registry", toVersion),
		Reversible:  true,
	})
	order++

	plan.Steps = append(plan.Steps, MigrationStep{
		Order:       order,
		Action:      "update_producers",
		Description: "Update producer applications to use the new schema version",
		Reversible:  true,
	})
	order++

	plan.Steps = append(plan.Steps, MigrationStep{
		Order:       order,
		Action:      "canary_test",
		Description: "Deploy to canary consumers and validate message processing",
		Reversible:  true,
	})
	order++

	plan.Steps = append(plan.Steps, MigrationStep{
		Order:       order,
		Action:      "update_consumers",
		Description: "Roll out schema update to all consumer applications",
		Reversible:  true,
	})
	order++

	plan.Steps = append(plan.Steps, MigrationStep{
		Order:       order,
		Action:      "verify",
		Description: "Verify all consumers are processing messages correctly with new schema",
		Reversible:  false,
	})

	plan.EstimatedRisk = "medium"
	plan.Prerequisites = []string{
		"All consumers support schema registry client",
		"Schema registry is accessible from all environments",
		"Rollback plan reviewed and tested",
	}

	return plan, nil
}

// AnalyzeEvolution analyzes schema history and provides evolution advice.
func (a *Advisor) AnalyzeEvolution(subject string) (*EvolutionAdvice, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	history, ok := a.history[subject]
	if !ok || len(history) == 0 {
		return nil, fmt.Errorf("no schema history for subject %q", subject)
	}

	advice := &EvolutionAdvice{
		Subject:      subject,
		VersionCount: len(history),
		HealthScore:  100,
	}

	if len(history) > 0 {
		advice.SchemaType = history[0].SchemaType
	}

	// Analyze patterns
	addedFields := 0
	removedFields := 0
	typeChanges := 0

	for i := 1; i < len(history); i++ {
		prev := history[i-1]
		curr := history[i]
		prevMap := fieldMap(prev.Fields)
		currMap := fieldMap(curr.Fields)

		for name := range currMap {
			if _, ok := prevMap[name]; !ok {
				addedFields++
			}
		}
		for name, pf := range prevMap {
			if _, ok := currMap[name]; !ok {
				removedFields++
			} else if currMap[name].Type != pf.Type {
				typeChanges++
			}
		}
	}

	if addedFields > 0 {
		advice.Patterns = append(advice.Patterns, EvolutionPattern{
			Pattern: "field_additions", Frequency: addedFields, Impact: "positive",
			Description: fmt.Sprintf("%d fields added across versions — additive evolution", addedFields),
		})
	}
	if removedFields > 0 {
		advice.Patterns = append(advice.Patterns, EvolutionPattern{
			Pattern: "field_removals", Frequency: removedFields, Impact: "negative",
			Description: fmt.Sprintf("%d fields removed — may break consumers", removedFields),
		})
		advice.HealthScore -= float64(removedFields) * 10
	}
	if typeChanges > 0 {
		advice.Patterns = append(advice.Patterns, EvolutionPattern{
			Pattern: "type_changes", Frequency: typeChanges, Impact: "negative",
			Description: fmt.Sprintf("%d field type changes — high risk of breaking consumers", typeChanges),
		})
		advice.HealthScore -= float64(typeChanges) * 15
	}

	// Best practices
	advice.BestPractices = []string{
		"Use BACKWARD compatibility for consumer-first evolution",
		"Always provide default values for new fields",
		"Deprecate fields before removing them",
		"Use union types (e.g., [\"null\", \"string\"]) for optional fields in Avro",
		"Version your schemas and maintain a changelog",
	}

	// Detect antipatterns
	if removedFields > 0 {
		advice.Antipatterns = append(advice.Antipatterns, "Fields removed without deprecation period")
	}
	if typeChanges > 0 {
		advice.Antipatterns = append(advice.Antipatterns, "Field types changed in-place instead of adding new fields")
	}
	if len(history) > 10 {
		advice.Antipatterns = append(advice.Antipatterns, "High version churn — consider batching schema changes")
	}

	if advice.HealthScore < 0 {
		advice.HealthScore = 0
	}

	return advice, nil
}

// ListSubjects returns all subjects with recorded history.
func (a *Advisor) ListSubjects() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	subjects := make([]string, 0, len(a.history))
	for s := range a.history {
		subjects = append(subjects, s)
	}
	sort.Strings(subjects)
	return subjects
}

// GetHistory returns the version history for a subject.
func (a *Advisor) GetHistory(subject string) ([]SchemaVersion, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	h, ok := a.history[subject]
	if !ok {
		return nil, fmt.Errorf("no history for subject %q", subject)
	}
	return h, nil
}

func extractFieldsFromSchema(schema, schemaType string) []FieldInfo {
	var fields []FieldInfo
	schemaType = strings.ToUpper(schemaType)

	// Simple field extraction from common schema patterns
	lines := strings.Split(schema, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, `"name"`) && strings.Contains(line, `"type"`) {
			// Avro-style field
			name := extractQuotedValue(line, "name")
			typ := extractQuotedValue(line, "type")
			if name != "" && typ != "" {
				fields = append(fields, FieldInfo{Name: name, Type: typ})
			}
		} else if strings.Contains(line, "=") && (schemaType == "PROTOBUF") {
			// Protobuf-style field: "string name = 1;"
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				fields = append(fields, FieldInfo{Name: parts[1], Type: parts[0]})
			}
		}
	}

	return fields
}

func extractQuotedValue(line, key string) string {
	search := fmt.Sprintf(`"%s"`, key)
	idx := strings.Index(line, search)
	if idx < 0 {
		return ""
	}
	rest := line[idx+len(search):]
	// Find value after colon and quotes
	colonIdx := strings.Index(rest, ":")
	if colonIdx < 0 {
		return ""
	}
	rest = strings.TrimSpace(rest[colonIdx+1:])
	if len(rest) > 0 && rest[0] == '"' {
		end := strings.Index(rest[1:], `"`)
		if end >= 0 {
			return rest[1 : end+1]
		}
	}
	return ""
}

func fieldMap(fields []FieldInfo) map[string]FieldInfo {
	m := make(map[string]FieldInfo)
	for _, f := range fields {
		m[f.Name] = f
	}
	return m
}
