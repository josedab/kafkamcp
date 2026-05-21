package codec

import (
	"errors"
	"fmt"
	"testing"
)

// stubProvider implements SchemaProvider for engine tests.
type stubProvider struct {
	schemas map[int]string
}

func (s *stubProvider) GetSchemaByID(id int) (string, Format, error) {
	sch, ok := s.schemas[id]
	if !ok {
		return "", "", fmt.Errorf("schema %d not found", id)
	}
	return sch, FormatAvro, nil
}

func (s *stubProvider) GetLatestSchema(subject string) (int, string, Format, error) {
	for id, sch := range s.schemas {
		return id, sch, FormatAvro, nil
	}
	_ = subject
	return 0, "", "", fmt.Errorf("no schemas")
}

const orderSchema = `{
	"type": "record",
	"name": "Order",
	"fields": [
		{"name": "id",     "type": "long"},
		{"name": "amount", "type": "double"},
		{"name": "items",  "type": {"type": "array", "items": "string"}}
	]
}`

func TestEngineAvroWireFormatRoundTrip(t *testing.T) {
	t.Parallel()

	provider := &stubProvider{schemas: map[int]string{101: orderSchema}}
	engine := NewEngineWithAvroCodec(provider)

	order := map[string]any{
		"id":     int64(2025),
		"amount": 19.95,
		"items":  []any{"sku-a", "sku-b"},
	}

	encoded, err := engine.Encode(order, 101, FormatAvro)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if encoded.Data[0] != confluentMagicByte {
		t.Fatalf("missing magic byte: 0x%02x", encoded.Data[0])
	}
	if encoded.SchemaID != 101 {
		t.Errorf("schema ID = %d, want 101", encoded.SchemaID)
	}

	decoded, err := engine.Decode(encoded.Data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Format != FormatAvro {
		t.Errorf("format = %s, want avro", decoded.Format)
	}
	if decoded.SchemaID != 101 {
		t.Errorf("schema ID = %d, want 101", decoded.SchemaID)
	}
	rec, ok := decoded.Value.(map[string]any)
	if !ok {
		t.Fatalf("decoded value type = %T, want map[string]any", decoded.Value)
	}
	if rec["id"].(int64) != 2025 || rec["amount"].(float64) != 19.95 {
		t.Errorf("round-trip mismatch: %+v", rec)
	}
}

func TestEngineAvroNoCodecStrictError(t *testing.T) {
	t.Parallel()

	provider := &stubProvider{schemas: map[int]string{101: orderSchema}}
	engine := NewEngine(provider)

	_, err := engine.Encode(map[string]any{"id": int64(1)}, 101, FormatAvro)
	if !errors.Is(err, ErrNoAvroCodec) {
		t.Errorf("expected ErrNoAvroCodec, got %v", err)
	}
}

// NewEngineWithAvroCodec is a thin test helper that wires the package-level
// avro codec into a fresh engine.
func NewEngineWithAvroCodec(provider SchemaProvider) *Engine {
	// Use the public constructor to keep this test self-contained.
	return NewEngineWithAvro(provider, newTestAvroCodec())
}
