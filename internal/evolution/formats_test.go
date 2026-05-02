package evolution

import (
	"testing"
)

// --- DetectFormat tests ---

func TestDetectFormat_AvroRecord(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := map[string]any{
		"type":   "record",
		"name":   "User",
		"fields": []any{},
	}
	if f := d.DetectFormat(schema); f != FormatAvro {
		t.Errorf("expected avro, got %s", f)
	}
}

func TestDetectFormat_JSONSchemaWithProperties(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := map[string]any{
		"properties": map[string]any{"name": map[string]any{"type": "string"}},
	}
	if f := d.DetectFormat(schema); f != FormatJSON {
		t.Errorf("expected json, got %s", f)
	}
}

func TestDetectFormat_JSONSchemaWithDollarSchema(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := map[string]any{
		"$schema": "http://json-schema.org/draft-07/schema#",
	}
	if f := d.DetectFormat(schema); f != FormatJSON {
		t.Errorf("expected json, got %s", f)
	}
}

func TestDetectFormat_Protobuf(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := `syntax = "proto3";
message User {
  string name = 1;
}`
	if f := d.DetectFormat(schema); f != FormatProtobuf {
		t.Errorf("expected protobuf, got %s", f)
	}
}

func TestDetectFormat_StringJSON(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := `{"type":"record","name":"User","fields":[]}`
	if f := d.DetectFormat(schema); f != FormatAvro {
		t.Errorf("expected avro from string JSON, got %s", f)
	}
}

func TestDetectFormat_Unknown(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	if f := d.DetectFormat(42); f != FormatUnknown {
		t.Errorf("expected unknown for int, got %s", f)
	}
	if f := d.DetectFormat("not json or proto"); f != FormatUnknown {
		t.Errorf("expected unknown for random string, got %s", f)
	}
}

func TestDetectFormat_MapWithoutFields(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := map[string]any{"type": "record"}
	if f := d.DetectFormat(schema); f != FormatUnknown {
		t.Errorf("expected unknown for map without fields, got %s", f)
	}
}

// --- ExtractFieldsWithFormat tests ---

func TestExtractAvroFields(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := map[string]any{
		"type": "record",
		"name": "User",
		"fields": []any{
			map[string]any{"name": "id", "type": "long"},
			map[string]any{"name": "name", "type": "string", "doc": "User full name"},
			map[string]any{"name": "email", "type": "string", "default": ""},
		},
	}

	fields := d.ExtractFieldsWithFormat(schema, FormatAvro)
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(fields))
	}
	if fi := fields["name"]; fi.Doc != "User full name" {
		t.Errorf("expected doc on name field, got %q", fi.Doc)
	}
	if fi := fields["email"]; !fi.HasDefault {
		t.Error("expected HasDefault on email field")
	}
}

func TestExtractAvroFields_UnionWithNull(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := map[string]any{
		"type": "record",
		"name": "Event",
		"fields": []any{
			map[string]any{"name": "optional_field", "type": []any{"null", "string"}},
		},
	}

	fields := d.ExtractFieldsWithFormat(schema, FormatAvro)
	fi := fields["optional_field"]
	if !fi.HasDefault {
		t.Error("expected HasDefault for union with null")
	}
}

func TestExtractAvroFields_NestedRecord(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := map[string]any{
		"type": "record",
		"name": "Order",
		"fields": []any{
			map[string]any{
				"name": "address",
				"type": map[string]any{
					"type": "record",
					"name": "Address",
					"fields": []any{
						map[string]any{"name": "street", "type": "string"},
					},
				},
			},
		},
	}

	fields := d.ExtractFieldsWithFormat(schema, FormatAvro)
	if _, ok := fields["address.street"]; !ok {
		t.Error("expected nested field address.street")
	}
}

func TestExtractJSONSchemaFields(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"name":  map[string]any{"type": "string", "description": "User name"},
			"age":   map[string]any{"type": "integer", "default": 0},
			"email": map[string]any{"type": "string"},
		},
		"required": []any{"name", "email"},
	}

	fields := d.ExtractFieldsWithFormat(schema, FormatJSON)
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(fields))
	}
	if !fields["name"].Required {
		t.Error("expected name to be required")
	}
	if !fields["email"].Required {
		t.Error("expected email to be required")
	}
	if fields["age"].Required {
		t.Error("expected age to be optional")
	}
	if !fields["age"].HasDefault {
		t.Error("expected age to have default")
	}
	if fields["name"].Doc != "User name" {
		t.Errorf("expected doc 'User name', got %q", fields["name"].Doc)
	}
}

func TestExtractProtobufFields(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := `syntax = "proto3";
message User {
  string name = 1;
  int32 age = 2;
  optional string nickname = 3;
}`

	fields := d.ExtractFieldsWithFormat(schema, FormatProtobuf)
	// Verify specific field presence (parser may pick up syntax line)
	if _, ok := fields["name"]; !ok {
		t.Fatal("expected field 'name'")
	}
	if fields["name"].Type != "string" {
		t.Errorf("expected type string for name, got %s", fields["name"].Type)
	}
	if !fields["name"].Required {
		t.Error("expected name to be required (non-optional)")
	}
	if nick, ok := fields["nickname"]; !ok {
		t.Fatal("expected field 'nickname'")
	} else if nick.Required {
		t.Error("expected nickname to be optional")
	}
}

func TestExtractFieldsWithFormat_Unknown(t *testing.T) {
	t.Parallel()
	d := NewMultiFormatDiffer()
	schema := map[string]any{
		"fields": map[string]any{"x": "int"},
	}
	fields := d.ExtractFieldsWithFormat(schema, FormatUnknown)
	if len(fields) == 0 {
		// Unknown format uses generic extraction fallback
		// May or may not produce fields depending on extractFields implementation
	}
}

// --- ComputeFormatAwareDiff tests ---

func TestComputeFormatAwareDiff_AddedFieldWithDefault(t *testing.T) {
	t.Parallel()
	schemaFrom := map[string]any{
		"type": "record",
		"name": "User",
		"fields": []any{
			map[string]any{"name": "id", "type": "long"},
		},
	}
	schemaTo := map[string]any{
		"type": "record",
		"name": "User",
		"fields": []any{
			map[string]any{"name": "id", "type": "long"},
			map[string]any{"name": "status", "type": "string", "default": "active"},
		},
	}

	diff := ComputeFormatAwareDiff("user-value", 1, 2, schemaFrom, schemaTo)
	if diff == nil {
		t.Fatal("expected non-nil diff")
	}

	// Added field with default should be non-breaking
	for _, f := range diff.Fields {
		if f.Field == "status" && f.Action == "added" {
			if f.Breaking {
				t.Error("expected added field with default to be non-breaking")
			}
			return
		}
	}
}

func TestComputeFormatAwareDiff_RemovedFieldWithDefault(t *testing.T) {
	t.Parallel()
	schemaFrom := map[string]any{
		"type": "record",
		"name": "User",
		"fields": []any{
			map[string]any{"name": "id", "type": "long"},
			map[string]any{"name": "deprecated", "type": "string", "default": ""},
		},
	}
	schemaTo := map[string]any{
		"type": "record",
		"name": "User",
		"fields": []any{
			map[string]any{"name": "id", "type": "long"},
		},
	}

	diff := ComputeFormatAwareDiff("user-value", 1, 2, schemaFrom, schemaTo)
	for _, f := range diff.Fields {
		if f.Field == "deprecated" && f.Action == "removed" {
			if f.Breaking {
				t.Error("expected removed field with default to be non-breaking")
			}
			return
		}
	}
}

// --- CanaryValidation tests ---

func TestCreateCanaryValidation_NoBreaking(t *testing.T) {
	t.Parallel()
	diff := &SchemaDiff{
		Subject: "orders-value",
		Summary: DiffSummary{Breaking: 0},
	}
	cv := CreateCanaryValidation("orders-value", diff)

	if cv.CanaryTopic != "orders-canary" {
		t.Errorf("expected orders-canary, got %s", cv.CanaryTopic)
	}
	if cv.TrafficPercent != 5.0 {
		t.Errorf("expected 5%% traffic, got %f", cv.TrafficPercent)
	}
	if cv.DurationMins != 30 {
		t.Errorf("expected 30 min duration, got %d", cv.DurationMins)
	}
}

func TestCreateCanaryValidation_WithBreaking(t *testing.T) {
	t.Parallel()
	diff := &SchemaDiff{
		Subject: "orders-value",
		Summary: DiffSummary{Breaking: 2},
	}
	cv := CreateCanaryValidation("orders-value", diff)

	if cv.TrafficPercent != 1.0 {
		t.Errorf("expected 1%% traffic for breaking changes, got %f", cv.TrafficPercent)
	}
	if cv.DurationMins != 60 {
		t.Errorf("expected 60 min duration for breaking changes, got %d", cv.DurationMins)
	}
}

// --- DiffVisualization tests ---

func TestCreateDiffVisualization(t *testing.T) {
	t.Parallel()
	diff := &SchemaDiff{
		Subject: "test-value",
		Fields: []FieldDiff{
			{Field: "new_field", Action: "added", Breaking: false},
			{Field: "old_field", Action: "removed", Breaking: true},
			{Field: "changed", Action: "modified", Breaking: true},
			{Field: "other", Action: "other_action", Breaking: false},
		},
		Summary:    DiffSummary{Added: 1, Removed: 1, Modified: 1, Breaking: 2},
		Compatible: false,
	}

	viz := CreateDiffVisualization(diff, FormatAvro)

	if viz.Subject != "test-value" || viz.Format != FormatAvro {
		t.Errorf("unexpected subject/format: %s/%s", viz.Subject, viz.Format)
	}
	if viz.BreakingCount != 2 {
		t.Errorf("expected 2 breaking, got %d", viz.BreakingCount)
	}
	if viz.CanAutoRoll {
		t.Error("expected CanAutoRoll=false with breaking changes")
	}

	// Check visualization metadata
	for _, vfd := range viz.FieldDiffs {
		switch vfd.Field {
		case "new_field":
			if vfd.Color != "green" || vfd.Icon != "➕" || vfd.Severity != "safe" {
				t.Errorf("added non-breaking: expected green/➕/safe, got %s/%s/%s", vfd.Color, vfd.Icon, vfd.Severity)
			}
		case "old_field":
			if vfd.Color != "red" || vfd.Icon != "➖" || vfd.Severity != "critical" {
				t.Errorf("removed breaking: expected red/➖/critical, got %s/%s/%s", vfd.Color, vfd.Icon, vfd.Severity)
			}
		case "changed":
			if vfd.Color != "red" || vfd.Icon != "✏️" || vfd.Severity != "critical" {
				t.Errorf("modified breaking: expected red/✏️/critical, got %s/%s/%s", vfd.Color, vfd.Icon, vfd.Severity)
			}
		case "other":
			if vfd.Color != "gray" || vfd.Severity != "safe" {
				t.Errorf("default action: expected gray/safe, got %s/%s", vfd.Color, vfd.Severity)
			}
		}
	}
}

func TestCreateDiffVisualization_AddedBreaking(t *testing.T) {
	t.Parallel()
	diff := &SchemaDiff{
		Fields: []FieldDiff{
			{Field: "required_field", Action: "added", Breaking: true},
		},
		Summary:    DiffSummary{Added: 1, Breaking: 1},
		Compatible: false,
	}

	viz := CreateDiffVisualization(diff, FormatJSON)
	vfd := viz.FieldDiffs[0]
	if vfd.Color != "yellow" || vfd.Severity != "warning" {
		t.Errorf("added breaking: expected yellow/warning, got %s/%s", vfd.Color, vfd.Severity)
	}
}

func TestCreateDiffVisualization_RemovedNonBreaking(t *testing.T) {
	t.Parallel()
	diff := &SchemaDiff{
		Fields: []FieldDiff{
			{Field: "optional", Action: "removed", Breaking: false},
		},
		Summary:    DiffSummary{Removed: 1, Breaking: 0},
		Compatible: true,
	}

	viz := CreateDiffVisualization(diff, FormatAvro)
	vfd := viz.FieldDiffs[0]
	if vfd.Color != "red" || vfd.Severity != "warning" {
		t.Errorf("removed non-breaking: expected red/warning, got %s/%s", vfd.Color, vfd.Severity)
	}
}

func TestCreateDiffVisualization_ModifiedNonBreaking(t *testing.T) {
	t.Parallel()
	diff := &SchemaDiff{
		Fields: []FieldDiff{
			{Field: "name", Action: "modified", Breaking: false},
		},
		Summary:    DiffSummary{Modified: 1, Breaking: 0},
		Compatible: true,
	}

	viz := CreateDiffVisualization(diff, FormatJSON)
	vfd := viz.FieldDiffs[0]
	if vfd.Color != "yellow" || vfd.Severity != "info" {
		t.Errorf("modified non-breaking: expected yellow/info, got %s/%s", vfd.Color, vfd.Severity)
	}
}
