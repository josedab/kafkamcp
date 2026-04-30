package codec

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
)

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		name   string
		data   []byte
		expect Format
	}{
		{"confluent wire format", []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03}, FormatAvro},
		{"json object", []byte(`{"key":"value"}`), FormatJSON},
		{"json array", []byte(`[1,2,3]`), FormatJSON},
		{"raw bytes", []byte("hello"), FormatRaw},
		{"empty", []byte{}, FormatRaw},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectFormat(tt.data)
			if got != tt.expect {
				t.Errorf("DetectFormat() = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestEngineDecodeJSON(t *testing.T) {
	engine := NewEngine(nil)

	data := []byte(`{"name":"test","value":42}`)
	result, err := engine.Decode(data)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if result.Format != FormatJSON {
		t.Errorf("Format = %v, want %v", result.Format, FormatJSON)
	}
	m, ok := result.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", result.Value)
	}
	if m["name"] != "test" {
		t.Errorf("name = %v, want test", m["name"])
	}
}

func TestEngineDecodeRaw(t *testing.T) {
	engine := NewEngine(nil)
	data := []byte("plain text message")
	result, err := engine.Decode(data)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if result.Format != FormatRaw {
		t.Errorf("Format = %v, want %v", result.Format, FormatRaw)
	}
}

func TestEngineDecodeEmpty(t *testing.T) {
	engine := NewEngine(nil)
	_, err := engine.Decode([]byte{})
	if err == nil {
		t.Error("expected error for empty data")
	}
}

func TestEngineEncodeJSON(t *testing.T) {
	engine := NewEngine(nil)
	msg, err := engine.Encode(map[string]string{"key": "val"}, 1, FormatJSON)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if msg.Format != FormatJSON {
		t.Errorf("Format = %v, want %v", msg.Format, FormatJSON)
	}
	var v map[string]string
	if err := json.Unmarshal(msg.Data, &v); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if v["key"] != "val" {
		t.Errorf("key = %v, want val", v["key"])
	}
}

func TestEngineEncodeNilValue(t *testing.T) {
	engine := NewEngine(nil)
	_, err := engine.Encode(nil, 1, FormatJSON)
	if err == nil {
		t.Error("expected error for nil value")
	}
}

func TestEngineEncodeAvro(t *testing.T) {
	engine := NewEngine(nil)
	msg, err := engine.Encode(map[string]string{"name": "test"}, 1, FormatAvro)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if msg.Format != FormatAvro {
		t.Errorf("Format = %v, want %v", msg.Format, FormatAvro)
	}
	// Verify Confluent wire format header
	if msg.Data[0] != confluentMagicByte {
		t.Error("missing magic byte")
	}
	if msg.SchemaID != 1 {
		t.Errorf("SchemaID = %d, want 1", msg.SchemaID)
	}
}

func TestEngineDecodeConfluentWireFormat(t *testing.T) {
	engine := NewEngine(nil)

	// Encode first
	encoded, err := engine.Encode(map[string]string{"name": "test"}, 42, FormatAvro)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	// Decode
	decoded, err := engine.Decode(encoded.Data)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded.SchemaID != 42 {
		t.Errorf("SchemaID = %d, want 42", decoded.SchemaID)
	}
	if decoded.Format != FormatAvro {
		t.Errorf("Format = %v, want %v", decoded.Format, FormatAvro)
	}
}

func TestEngineEncodeUnsupportedFormat(t *testing.T) {
	engine := NewEngine(nil)
	_, err := engine.Encode("test", 1, "xml")
	if err == nil {
		t.Error("expected error for unsupported format")
	}
}

// --- mockProvider for testing ---

type mockProvider struct {
	schemas map[int]struct {
		schema string
		format Format
	}
}

func (m *mockProvider) GetSchemaByID(id int) (string, Format, error) {
	entry, ok := m.schemas[id]
	if !ok {
		return "", "", fmt.Errorf("schema %d not found", id)
	}
	return entry.schema, entry.format, nil
}

func (m *mockProvider) GetLatestSchema(subject string) (int, string, Format, error) {
	return 0, "", "", fmt.Errorf("not implemented")
}

func newMockProvider(entries map[int]Format) *mockProvider {
	mp := &mockProvider{schemas: make(map[int]struct {
		schema string
		format Format
	})}
	for id, f := range entries {
		mp.schemas[id] = struct {
			schema string
			format Format
		}{schema: `{}`, format: f}
	}
	return mp
}

// --- encodeProtobuf tests ---

func TestEncodeProtobuf_VarintPrefix(t *testing.T) {
	engine := NewEngine(nil) // nil provider returns default "{}" schema
	msg, err := engine.Encode(map[string]string{"key": "val"}, 1, FormatProtobuf)
	if err != nil {
		t.Fatalf("encodeProtobuf: %v", err)
	}
	// Confluent wire format: magic byte + 4-byte schema ID + varint prefix + JSON
	if msg.Data[0] != confluentMagicByte {
		t.Error("missing magic byte")
	}
	payload := msg.Data[5:]
	if len(payload) == 0 {
		t.Fatal("empty payload")
	}
	if payload[0] != 0x00 {
		t.Errorf("expected varint prefix 0x00, got 0x%02x", payload[0])
	}
}

func TestEncodeProtobuf_NilValue(t *testing.T) {
	engine := NewEngine(nil)
	_, err := engine.Encode(nil, 1, FormatProtobuf)
	if err == nil {
		t.Error("expected error for nil value")
	}
}

func TestEncodeProtobuf_NoSchemaClient(t *testing.T) {
	// nil provider returns default "{}" schema, so encoding should succeed
	engine := NewEngine(nil)
	_, err := engine.Encode(map[string]string{"k": "v"}, 1, FormatProtobuf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- decodeProtobuf tests ---

func TestDecodeProtobuf_VarintStripping(t *testing.T) {
	provider := newMockProvider(map[int]Format{1: FormatProtobuf})
	engine := NewEngine(provider)

	// Build a Confluent wire format protobuf message: magic + 4-byte ID + varint prefix + JSON
	jsonData := []byte(`{"key":"value"}`)
	wire := make([]byte, 5+1+len(jsonData))
	wire[0] = confluentMagicByte
	binary.BigEndian.PutUint32(wire[1:5], 1)
	wire[5] = 0x00 // varint prefix
	copy(wire[6:], jsonData)

	decoded, err := engine.Decode(wire)
	if err != nil {
		t.Fatalf("Decode protobuf: %v", err)
	}
	if decoded.Format != FormatProtobuf {
		t.Errorf("expected format protobuf, got %s", decoded.Format)
	}
	m, ok := decoded.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", decoded.Value)
	}
	if m["key"] != "value" {
		t.Errorf("expected key=value, got %v", m["key"])
	}
}

func TestDecodeProtobuf_TruncatedData(t *testing.T) {
	provider := newMockProvider(map[int]Format{1: FormatProtobuf})
	engine := NewEngine(provider)

	// Wire format with only the varint prefix, no JSON
	wire := make([]byte, 6)
	wire[0] = confluentMagicByte
	binary.BigEndian.PutUint32(wire[1:5], 1)
	wire[5] = 0x00 // varint prefix, then empty

	_, err := engine.Decode(wire)
	if err == nil {
		t.Error("expected error for truncated protobuf data")
	}
}

func TestDecodeProtobuf_InvalidJSONAfterPrefix(t *testing.T) {
	provider := newMockProvider(map[int]Format{1: FormatProtobuf})
	engine := NewEngine(provider)

	wire := make([]byte, 5+1+3)
	wire[0] = confluentMagicByte
	binary.BigEndian.PutUint32(wire[1:5], 1)
	wire[5] = 0x00
	copy(wire[6:], []byte("xyz"))

	_, err := engine.Decode(wire)
	if err == nil {
		t.Error("expected error for invalid JSON after varint prefix")
	}
}

// --- resolveFormat tests ---

func TestResolveFormat_WithProvider(t *testing.T) {
	provider := newMockProvider(map[int]Format{42: FormatProtobuf})
	engine := NewEngine(provider)

	format, err := engine.resolveFormat(42)
	if err != nil {
		t.Fatalf("resolveFormat: %v", err)
	}
	if format != FormatProtobuf {
		t.Errorf("expected protobuf, got %s", format)
	}
}

func TestResolveFormat_DefaultsToAvroWithoutProvider(t *testing.T) {
	engine := NewEngine(nil)
	format, err := engine.resolveFormat(1)
	if err != nil {
		t.Fatalf("resolveFormat: %v", err)
	}
	if format != FormatAvro {
		t.Errorf("expected avro default, got %s", format)
	}
}

func TestResolveFormat_CachesResult(t *testing.T) {
	callCount := 0
	provider := &countingProvider{
		calls:  &callCount,
		format: FormatAvro,
	}
	engine := NewEngine(provider)

	engine.resolveFormat(1)
	engine.resolveFormat(1) // should hit cache

	if callCount != 1 {
		t.Errorf("expected 1 provider call (cached), got %d", callCount)
	}
}

type countingProvider struct {
	calls  *int
	format Format
}

func (p *countingProvider) GetSchemaByID(id int) (string, Format, error) {
	*p.calls++
	return `{}`, p.format, nil
}

func (p *countingProvider) GetLatestSchema(subject string) (int, string, Format, error) {
	return 0, "", "", fmt.Errorf("not implemented")
}

func TestResolveFormat_ProviderError(t *testing.T) {
	provider := &errorProvider{}
	engine := NewEngine(provider)

	_, err := engine.resolveFormat(999)
	if err == nil {
		t.Error("expected error from provider")
	}
}

type errorProvider struct{}

func (p *errorProvider) GetSchemaByID(id int) (string, Format, error) {
	return "", "", fmt.Errorf("connection refused")
}

func (p *errorProvider) GetLatestSchema(subject string) (int, string, Format, error) {
	return 0, "", "", fmt.Errorf("not implemented")
}

// --- Round-trip test ---

func TestRoundTrip_EncodeDecodeProtobuf(t *testing.T) {
	provider := newMockProvider(map[int]Format{10: FormatProtobuf})
	engine := NewEngine(provider)

	original := map[string]any{"name": "test", "count": float64(42)}
	encoded, err := engine.Encode(original, 10, FormatProtobuf)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := engine.Decode(encoded.Data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.Format != FormatProtobuf {
		t.Errorf("expected format protobuf, got %s", decoded.Format)
	}
	m, ok := decoded.Value.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", decoded.Value)
	}
	if m["name"] != "test" {
		t.Errorf("expected name=test, got %v", m["name"])
	}
}

func TestRoundTrip_EncodeDecodeAvro(t *testing.T) {
	engine := NewEngine(nil)

	original := map[string]string{"key": "value"}
	encoded, err := engine.Encode(original, 5, FormatAvro)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := engine.Decode(encoded.Data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.SchemaID != 5 {
		t.Errorf("expected schema ID 5, got %d", decoded.SchemaID)
	}
}
