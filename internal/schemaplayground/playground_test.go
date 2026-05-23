package schemaplayground

import (
	"log/slog"
	"testing"
)

const testAvroSchema = `{
	"type": "record",
	"name": "Order",
	"fields": [
		{"name": "order_id", "type": "string"},
		{"name": "amount", "type": "double"},
		{"name": "status", "type": "string"},
		{"name": "notes", "type": ["null", "string"], "default": null}
	]
}`

const testAvroSchemaV2 = `{
	"type": "record",
	"name": "Order",
	"fields": [
		{"name": "order_id", "type": "string"},
		{"name": "amount", "type": "double"},
		{"name": "currency", "type": "string"},
		{"name": "notes", "type": ["null", "string"], "default": null}
	]
}`

func TestDraftLifecycle(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	draft, err := e.CreateDraft("orders-value", SchemaAvro, testAvroSchema, "agent-1")
	if err != nil {
		t.Fatalf("CreateDraft failed: %v", err)
	}
	if draft.Status != StatusDraft {
		t.Errorf("expected draft status, got %s", draft.Status)
	}

	// Test a valid message
	result, err := e.TestMessage(draft.ID, `{"order_id": "123", "amount": 99.99, "status": "pending"}`)
	if err != nil {
		t.Fatalf("TestMessage failed: %v", err)
	}
	if !result.Valid {
		t.Errorf("expected valid message, got errors: %v", result.Errors)
	}

	// Test an invalid message (missing required field)
	result, err = e.TestMessage(draft.ID, `{"order_id": "123"}`)
	if err != nil {
		t.Fatalf("TestMessage failed: %v", err)
	}
	if result.Valid {
		t.Error("expected invalid message")
	}
}

func TestCompatibilityValidation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	draft, _ := e.CreateDraft("orders-value", SchemaAvro, testAvroSchemaV2, "agent-1")

	validation, err := e.ValidateCompatibility(draft.ID, testAvroSchema, "BACKWARD")
	if err != nil {
		t.Fatalf("ValidateCompatibility failed: %v", err)
	}
	// currency is new required field without default = not backward compatible
	if validation.Compatible {
		t.Error("expected incompatible (new required field without default)")
	}
}

func TestEvolutionSimulation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	draft, _ := e.CreateDraft("orders-value", SchemaAvro, testAvroSchemaV2, "agent-1")

	impact, err := e.SimulateEvolution(draft.ID, testAvroSchema)
	if err != nil {
		t.Fatalf("SimulateEvolution failed: %v", err)
	}

	if len(impact.AddedFields) == 0 {
		t.Error("expected added fields")
	}
	if len(impact.RemovedFields) == 0 {
		t.Error("expected removed fields (status was removed)")
	}
	if len(impact.Recommendations) == 0 {
		t.Error("expected recommendations")
	}
}

func TestPromoteDraft(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	draft, _ := e.CreateDraft("test", SchemaAvro, testAvroSchema, "agent-1")

	// Can't promote a fresh draft
	_, err := e.PromoteDraft(draft.ID)
	if err == nil {
		t.Error("expected error promoting untested draft")
	}

	// Test it first
	_, _ = e.TestMessage(draft.ID, `{"order_id": "1", "amount": 10, "status": "ok"}`)

	// Now promote
	promoted, err := e.PromoteDraft(draft.ID)
	if err != nil {
		t.Fatalf("PromoteDraft failed: %v", err)
	}
	if promoted.Status != StatusPromoted {
		t.Errorf("expected promoted, got %s", promoted.Status)
	}
}

func TestExtraFieldsDetected(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	draft, _ := e.CreateDraft("test", SchemaAvro, testAvroSchema, "agent-1")

	result, _ := e.TestMessage(draft.ID, `{"order_id": "1", "amount": 10, "status": "ok", "extra_field": "value"}`)

	hasDataLoss := false
	for _, fm := range result.FieldMap {
		if fm.Field == "extra_field" && fm.DataLoss {
			hasDataLoss = true
		}
	}
	if !hasDataLoss {
		t.Error("expected data loss warning for extra field")
	}
}

func TestUpdateDraft_ModifySchema(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	draft, _ := e.CreateDraft("test", SchemaAvro, testAvroSchema, "agent-1")
	origVersion := draft.Version

	updated, err := e.UpdateDraft(draft.ID, testAvroSchemaV2)
	if err != nil {
		t.Fatalf("UpdateDraft: %v", err)
	}
	if updated.Schema != testAvroSchemaV2 {
		t.Error("expected schema to be updated")
	}
	if updated.Version != origVersion+1 {
		t.Errorf("expected version %d, got %d", origVersion+1, updated.Version)
	}
}

func TestUpdateDraft_IncrementsVersion(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	draft, _ := e.CreateDraft("test", SchemaAvro, testAvroSchema, "agent-1")

	_, _ = e.UpdateDraft(draft.ID, `{"type":"record","name":"V2","fields":[]}`)
	updated, _ := e.UpdateDraft(draft.ID, `{"type":"record","name":"V3","fields":[]}`)
	if updated.Version != 3 {
		t.Errorf("expected version 3, got %d", updated.Version)
	}
}

func TestUpdateDraft_BlocksPromotedDraft(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	draft, _ := e.CreateDraft("test", SchemaAvro, testAvroSchema, "agent-1")

	// Test and promote
	_, _ = e.TestMessage(draft.ID, `{"order_id": "1", "amount": 10, "status": "ok"}`)
	_, _ = e.PromoteDraft(draft.ID)

	_, err := e.UpdateDraft(draft.ID, testAvroSchemaV2)
	if err == nil {
		t.Error("expected error updating promoted draft")
	}
}

func TestUpdateDraft_ResetsStatus(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	draft, _ := e.CreateDraft("test", SchemaAvro, testAvroSchema, "agent-1")

	// Test it to change status to "testing"
	_, _ = e.TestMessage(draft.ID, `{"order_id": "1", "amount": 10, "status": "ok"}`)

	updated, _ := e.UpdateDraft(draft.ID, testAvroSchemaV2)
	if updated.Status != StatusDraft {
		t.Errorf("expected status reset to draft, got %s", updated.Status)
	}
}

func TestUpdateDraft_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.UpdateDraft("nonexistent", "{}")
	if err == nil {
		t.Error("expected error for non-existent draft")
	}
}

func TestGetDraft_Exists(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	draft, _ := e.CreateDraft("test", SchemaAvro, testAvroSchema, "agent-1")

	got, err := e.GetDraft(draft.ID)
	if err != nil {
		t.Fatalf("GetDraft: %v", err)
	}
	if got.ID != draft.ID {
		t.Errorf("expected ID %s, got %s", draft.ID, got.ID)
	}
}

func TestGetDraft_NotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.GetDraft("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent draft")
	}
}

func TestListDrafts_FilterByStatus(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, _ = e.CreateDraft("s1", SchemaAvro, testAvroSchema, "agent-1")
	d2, _ := e.CreateDraft("s2", SchemaAvro, testAvroSchema, "agent-1")

	// Test d2 to change its status
	_, _ = e.TestMessage(d2.ID, `{"order_id": "1", "amount": 10, "status": "ok"}`)

	drafts := e.ListDrafts("draft")
	for _, d := range drafts {
		if d.Status != StatusDraft {
			t.Errorf("expected only draft status, got %s", d.Status)
		}
	}

	testing_ := e.ListDrafts("testing")
	for _, d := range testing_ {
		if d.Status != StatusTesting {
			t.Errorf("expected only testing status, got %s", d.Status)
		}
	}
}

func TestListDrafts_AllWhenEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, _ = e.CreateDraft("s1", SchemaAvro, testAvroSchema, "agent-1")
	_, _ = e.CreateDraft("s2", SchemaAvro, testAvroSchema, "agent-1")

	all := e.ListDrafts("")
	if len(all) != 2 {
		t.Errorf("expected 2 drafts, got %d", len(all))
	}
}

func TestListDrafts_EmptyEngine(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	drafts := e.ListDrafts("")
	if len(drafts) != 0 {
		t.Errorf("expected 0 drafts, got %d", len(drafts))
	}
}

const testJSONSchema = `{
	"type": "object",
	"properties": {
		"name": {"type": "string"},
		"age": {"type": "integer"},
		"email": {"type": "string"}
	},
	"required": ["name", "age"]
}`

func TestExtractJSONSchemaFields_ValidSchema(t *testing.T) {
	t.Parallel()
	fields := extractJSONSchemaFields(testJSONSchema)
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(fields))
	}

	fieldMap := make(map[string]schemaField)
	for _, f := range fields {
		fieldMap[f.Name] = f
	}

	if f, ok := fieldMap["name"]; !ok || !f.Required {
		t.Error("expected name to be required")
	}
	if f, ok := fieldMap["age"]; !ok || !f.Required {
		t.Error("expected age to be required")
	}
	if f, ok := fieldMap["email"]; !ok || f.Required {
		t.Error("expected email to be optional")
	}
}

func TestExtractJSONSchemaFields_MissingProperties(t *testing.T) {
	t.Parallel()
	fields := extractJSONSchemaFields(`{"type": "object"}`)
	if fields != nil {
		t.Error("expected nil for schema without properties")
	}
}

func TestExtractJSONSchemaFields_InvalidJSON(t *testing.T) {
	t.Parallel()
	fields := extractJSONSchemaFields(`not json`)
	if fields != nil {
		t.Error("expected nil for invalid JSON")
	}
}

func TestExtractJSONSchemaFields_EmptySchema(t *testing.T) {
	t.Parallel()
	fields := extractJSONSchemaFields(`{}`)
	if fields != nil {
		t.Error("expected nil for empty schema")
	}
}

func TestExtractJSONSchemaFields_NoRequiredArray(t *testing.T) {
	t.Parallel()
	fields := extractJSONSchemaFields(`{
		"type": "object",
		"properties": {
			"name": {"type": "string"}
		}
	}`)
	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}
	if fields[0].Required {
		t.Error("expected field to be optional when no required array")
	}
}

func TestExtractJSONSchemaFields_NestedObject(t *testing.T) {
	t.Parallel()
	fields := extractJSONSchemaFields(`{
		"type": "object",
		"properties": {
			"address": {"type": "object"},
			"name": {"type": "string"}
		}
	}`)
	if len(fields) != 2 {
		t.Fatalf("expected 2 fields, got %d", len(fields))
	}
}

func TestExtractJSONSchemaFields_Unicode(t *testing.T) {
	t.Parallel()
	fields := extractJSONSchemaFields(`{
		"type": "object",
		"properties": {
			"名前": {"type": "string"}
		},
		"required": ["名前"]
	}`)
	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}
	if !fields[0].Required {
		t.Error("expected unicode field to be required")
	}
}
