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
	e := NewEngine(slog.Default())

	draft, _ := e.CreateDraft("test", SchemaAvro, testAvroSchema, "agent-1")

	// Can't promote a fresh draft
	_, err := e.PromoteDraft(draft.ID)
	if err == nil {
		t.Error("expected error promoting untested draft")
	}

	// Test it first
	e.TestMessage(draft.ID, `{"order_id": "1", "amount": 10, "status": "ok"}`)

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
