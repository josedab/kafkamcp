package evolution

import (
	"testing"
)

func TestExtractFieldsFromAvro(t *testing.T) {
	schema := map[string]any{
		"type": "record",
		"name": "Order",
		"fields": []any{
			map[string]any{"name": "order_id", "type": "string"},
			map[string]any{"name": "amount", "type": "double"},
		},
	}
	fields := extractFields(schema)
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}
	if _, ok := fields["order_id"]; !ok {
		t.Error("expected order_id field")
	}
}

func TestExtractFieldsFromJSONSchema(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"order_id": map[string]any{"type": "string"},
			"amount":   map[string]any{"type": "number"},
		},
	}
	fields := extractFields(schema)
	if len(fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(fields))
	}
}

func TestExtractFieldsFromString(t *testing.T) {
	schema := `{"fields": [{"name": "id", "type": "string"}]}`
	fields := extractFields(schema)
	if len(fields) != 1 {
		t.Errorf("expected 1 field, got %d", len(fields))
	}
}

func TestExtractFieldsInvalidString(t *testing.T) {
	fields := extractFields("not valid json")
	if len(fields) != 0 {
		t.Errorf("expected 0 fields for invalid JSON, got %d", len(fields))
	}
}

func TestExtractFieldsEmpty(t *testing.T) {
	fields := extractFields(map[string]any{})
	if len(fields) != 0 {
		t.Errorf("expected 0 fields, got %d", len(fields))
	}
}

func TestHasDefault(t *testing.T) {
	schema := map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "type": "string"},
			map[string]any{"name": "status", "type": "string", "default": "active"},
		},
	}
	if hasDefault(schema, "id") {
		t.Error("id should not have default")
	}
	if !hasDefault(schema, "status") {
		t.Error("status should have default")
	}
	if hasDefault(schema, "nonexistent") {
		t.Error("nonexistent field should not have default")
	}
}

func TestHasDefaultNotMap(t *testing.T) {
	if hasDefault("not a map", "field") {
		t.Error("expected false for non-map schema")
	}
}

func TestHasDefaultNoFields(t *testing.T) {
	if hasDefault(map[string]any{"type": "record"}, "field") {
		t.Error("expected false when no fields key")
	}
}

func TestCountBreaking(t *testing.T) {
	fields := []FieldDiff{
		{Field: "new_field", Action: "added", Breaking: false},
		{Field: "old_field", Action: "removed", Breaking: true},
		{Field: "mod_field", Action: "modified", Breaking: true},
	}
	breaking := countBreaking(fields)
	if breaking != 2 {
		t.Errorf("expected 2 breaking changes, got %d", breaking)
	}
}

func TestCountBreakingEmpty(t *testing.T) {
	breaking := countBreaking(nil)
	if breaking != 0 {
		t.Errorf("expected 0 breaking changes, got %d", breaking)
	}
}
