package codec

import (
	"encoding/json"
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
