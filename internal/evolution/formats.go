package evolution

import (
	"encoding/json"
	"fmt"
	"strings"
)

// SchemaFormat identifies the serialization format.
type SchemaFormat string

// FormatAvro and related values define constants for this package.
const (
	FormatAvro     SchemaFormat = "avro"
	FormatProtobuf SchemaFormat = "protobuf"
	FormatJSON     SchemaFormat = "json"
	FormatUnknown  SchemaFormat = "unknown"
)

// FormatDiffer provides format-aware schema diffing.
type FormatDiffer interface {
	DetectFormat(schema any) SchemaFormat
	ExtractFields(schema any) map[string]any
	IsBreakingChange(oldField, newField any) bool
}

// MultiFormatDiffer supports Avro, Protobuf, and JSON Schema formats.
type MultiFormatDiffer struct{}

// NewMultiFormatDiffer creates a new multi-format differ.
func NewMultiFormatDiffer() *MultiFormatDiffer {
	return &MultiFormatDiffer{}
}

// DetectFormat auto-detects the schema format from its structure.
func (d *MultiFormatDiffer) DetectFormat(schema any) SchemaFormat {
	switch s := schema.(type) {
	case map[string]any:
		if _, ok := s["fields"]; ok {
			if t, _ := s["type"].(string); t == "record" {
				return FormatAvro
			}
		}
		if _, ok := s["properties"]; ok {
			return FormatJSON
		}
		if _, ok := s["$schema"]; ok {
			return FormatJSON
		}
	case string:
		if strings.Contains(s, "syntax = \"proto") {
			return FormatProtobuf
		}
		var parsed map[string]any
		if err := json.Unmarshal([]byte(s), &parsed); err == nil {
			return d.DetectFormat(parsed)
		}
	}
	return FormatUnknown
}

// ExtractFieldsWithFormat extracts fields with format awareness.
func (d *MultiFormatDiffer) ExtractFieldsWithFormat(schema any, format SchemaFormat) map[string]FieldInfo {
	fields := make(map[string]FieldInfo)

	switch format {
	case FormatAvro:
		d.extractAvroFields(schema, fields, "")
	case FormatJSON:
		d.extractJSONSchemaFields(schema, fields, "")
	case FormatProtobuf:
		d.extractProtobufFields(schema, fields, "")
	default:
		// Generic extraction
		raw := extractFields(schema)
		for k, v := range raw {
			fields[k] = FieldInfo{Name: k, Type: fmt.Sprintf("%v", v), Format: format}
		}
	}

	return fields
}

// FieldInfo contains detailed field metadata.
type FieldInfo struct {
	Name       string       `json:"name"`
	Type       string       `json:"type"`
	Format     SchemaFormat `json:"format"`
	HasDefault bool         `json:"has_default"`
	Required   bool         `json:"required"`
	Doc        string       `json:"doc,omitempty"`
}

func (d *MultiFormatDiffer) extractAvroFields(schema any, fields map[string]FieldInfo, prefix string) {
	s, ok := schema.(map[string]any)
	if !ok {
		return
	}

	fieldsArr, ok := s["fields"].([]any)
	if !ok {
		return
	}

	for _, f := range fieldsArr {
		field, ok := f.(map[string]any)
		if !ok {
			continue
		}
		name, _ := field["name"].(string)
		if name == "" {
			continue
		}

		fullName := name
		if prefix != "" {
			fullName = prefix + "." + name
		}

		_, hasDefault := field["default"]
		doc, _ := field["doc"].(string)

		fi := FieldInfo{
			Name:       fullName,
			Type:       fmt.Sprintf("%v", field["type"]),
			Format:     FormatAvro,
			HasDefault: hasDefault,
			Doc:        doc,
		}

		// Check if type is a union containing null (optional field)
		if typeArr, ok := field["type"].([]any); ok {
			for _, t := range typeArr {
				if ts, ok := t.(string); ok && ts == "null" {
					fi.HasDefault = true
					break
				}
			}
		}

		fields[fullName] = fi

		// Recurse into nested records
		if typeMap, ok := field["type"].(map[string]any); ok {
			if t, _ := typeMap["type"].(string); t == "record" {
				d.extractAvroFields(typeMap, fields, fullName)
			}
		}
	}
}

func (d *MultiFormatDiffer) extractJSONSchemaFields(schema any, fields map[string]FieldInfo, prefix string) {
	s, ok := schema.(map[string]any)
	if !ok {
		return
	}

	props, ok := s["properties"].(map[string]any)
	if !ok {
		return
	}

	requiredFields := make(map[string]bool)
	if reqArr, ok := s["required"].([]any); ok {
		for _, r := range reqArr {
			if rs, ok := r.(string); ok {
				requiredFields[rs] = true
			}
		}
	}

	for name, prop := range props {
		fullName := name
		if prefix != "" {
			fullName = prefix + "." + name
		}

		propMap, ok := prop.(map[string]any)
		if !ok {
			continue
		}

		fi := FieldInfo{
			Name:     fullName,
			Type:     fmt.Sprintf("%v", propMap["type"]),
			Format:   FormatJSON,
			Required: requiredFields[name],
		}

		if _, ok := propMap["default"]; ok {
			fi.HasDefault = true
		}
		if desc, ok := propMap["description"].(string); ok {
			fi.Doc = desc
		}

		fields[fullName] = fi

		// Recurse into nested objects
		if t, _ := propMap["type"].(string); t == "object" {
			d.extractJSONSchemaFields(propMap, fields, fullName)
		}
	}
}

func (d *MultiFormatDiffer) extractProtobufFields(schema any, fields map[string]FieldInfo, prefix string) {
	// Protobuf is typically a string schema definition
	s, ok := schema.(string)
	if !ok {
		return
	}

	lines := strings.Split(s, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Simple protobuf field extraction: "type name = number;"
		if strings.Contains(line, "=") && strings.HasSuffix(line, ";") && !strings.HasPrefix(line, "//") {
			parts := strings.Fields(strings.TrimSuffix(line, ";"))
			if len(parts) >= 3 {
				fieldType := parts[0]
				fieldName := parts[1]
				optional := false
				if fieldType == "optional" && len(parts) >= 4 {
					optional = true
					fieldType = parts[1]
					fieldName = parts[2]
				}
				fullName := fieldName
				if prefix != "" {
					fullName = prefix + "." + fieldName
				}
				fields[fullName] = FieldInfo{
					Name:       fullName,
					Type:       fieldType,
					Format:     FormatProtobuf,
					HasDefault: optional,
					Required:   !optional,
				}
			}
		}
	}
}

// ComputeFormatAwareDiff performs a diff with format-specific understanding.
func ComputeFormatAwareDiff(subject string, versionFrom, versionTo int, schemaFrom, schemaTo any) *SchemaDiff {
	differ := NewMultiFormatDiffer()
	format := differ.DetectFormat(schemaFrom)

	// Use existing ComputeDiff for the base diff
	diff := ComputeDiff(subject, versionFrom, versionTo, schemaFrom, schemaTo)

	// Enrich with format-specific metadata
	if format != FormatUnknown {
		fromFields := differ.ExtractFieldsWithFormat(schemaFrom, format)
		toFields := differ.ExtractFieldsWithFormat(schemaTo, format)

		for i, f := range diff.Fields {
			if fi, ok := fromFields[f.Field]; ok {
				if fi.HasDefault && f.Action == "removed" {
					diff.Fields[i].Breaking = false
					diff.Fields[i].Details += " (has default value — backward compatible)"
				}
			}
			if fi, ok := toFields[f.Field]; ok {
				if fi.HasDefault && f.Action == "added" {
					diff.Fields[i].Breaking = false
					diff.Fields[i].Details += " (has default value — backward compatible)"
				}
			}
		}

		// Recalculate compatibility
		diff.Compatible = true
		breakingCount := 0
		for _, f := range diff.Fields {
			if f.Breaking {
				diff.Compatible = false
				breakingCount++
			}
		}
		diff.Summary.Breaking = breakingCount
	}

	return diff
}

// CanaryValidation describes a canary validation step.
type CanaryValidation struct {
	CanaryTopic      string   `json:"canary_topic"`
	TrafficPercent   float64  `json:"traffic_percent"`
	DurationMins     int      `json:"duration_mins"`
	SuccessCriteria  []string `json:"success_criteria"`
	RollbackTriggers []string `json:"rollback_triggers"`
}

// CreateCanaryValidation generates a canary validation configuration.
func CreateCanaryValidation(subject string, diff *SchemaDiff) *CanaryValidation {
	topic := extractTopicFromSubject(subject)
	canaryTopic := topic + "-canary"

	cv := &CanaryValidation{
		CanaryTopic:    canaryTopic,
		TrafficPercent: 5.0,
		DurationMins:   30,
		SuccessCriteria: []string{
			"Zero serialization errors on canary consumers",
			"Consumer lag on canary topic < 100 messages",
			"Error rate < 0.1%",
			"P99 latency within 2x of baseline",
		},
		RollbackTriggers: []string{
			"Any serialization error detected",
			"Consumer lag exceeds 1000 messages",
			"Error rate exceeds 1%",
			"Producer unable to serialize messages",
		},
	}

	if diff.Summary.Breaking > 0 {
		cv.TrafficPercent = 1.0
		cv.DurationMins = 60
	}

	return cv
}

// DiffVisualization provides metadata for visual schema diff rendering.
type DiffVisualization struct {
	Subject       string            `json:"subject"`
	Format        SchemaFormat      `json:"format"`
	FieldDiffs    []VisualFieldDiff `json:"field_diffs"`
	BreakingCount int               `json:"breaking_count"`
	SafeCount     int               `json:"safe_count"`
	CanAutoRoll   bool              `json:"can_auto_roll"`
}

// VisualFieldDiff enriches field diffs with visualization metadata.
type VisualFieldDiff struct {
	FieldDiff
	Color    string `json:"color"`    // "red", "green", "yellow", "gray"
	Icon     string `json:"icon"`     // "➕", "➖", "✏️", "⚠️"
	Severity string `json:"severity"` // "critical", "warning", "info", "safe"
}

// CreateDiffVisualization generates visualization metadata for a schema diff.
func CreateDiffVisualization(diff *SchemaDiff, format SchemaFormat) *DiffVisualization {
	viz := &DiffVisualization{
		Subject: diff.Subject,
		Format:  format,
	}

	for _, f := range diff.Fields {
		vfd := VisualFieldDiff{FieldDiff: f}

		switch f.Action {
		case "added":
			vfd.Color = "green"
			vfd.Icon = "➕"
			if f.Breaking {
				vfd.Severity = "warning"
				vfd.Color = "yellow"
			} else {
				vfd.Severity = "safe"
			}
		case "removed":
			vfd.Color = "red"
			vfd.Icon = "➖"
			if f.Breaking {
				vfd.Severity = "critical"
			} else {
				vfd.Severity = "warning"
			}
		case "modified":
			vfd.Icon = "✏️"
			if f.Breaking {
				vfd.Color = "red"
				vfd.Severity = "critical"
			} else {
				vfd.Color = "yellow"
				vfd.Severity = "info"
			}
		default:
			vfd.Color = "gray"
			vfd.Icon = "•"
			vfd.Severity = "safe"
		}

		viz.FieldDiffs = append(viz.FieldDiffs, vfd)
	}

	viz.BreakingCount = diff.Summary.Breaking
	viz.SafeCount = diff.Summary.Added + diff.Summary.Modified + diff.Summary.Removed - diff.Summary.Breaking
	viz.CanAutoRoll = diff.Compatible

	return viz
}
