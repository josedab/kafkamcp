package codec

import (
	"fmt"
	"testing"

	codecprotobuf "github.com/kafkamcp/kafkamcp/internal/codec/protobuf"
)

// stubProtobufProvider returns a single Protobuf schema for engine
// integration tests.
type stubProtobufProvider struct {
	schemas map[int]string
}

func (s *stubProtobufProvider) GetSchemaByID(id int) (string, Format, error) {
	sch, ok := s.schemas[id]
	if !ok {
		return "", "", fmt.Errorf("schema %d not found", id)
	}
	return sch, FormatProtobuf, nil
}

func (s *stubProtobufProvider) GetLatestSchema(subject string) (int, string, Format, error) {
	for id, sch := range s.schemas {
		_ = subject
		return id, sch, FormatProtobuf, nil
	}
	return 0, "", "", fmt.Errorf("no schemas")
}

const orderProtoSchema = `
syntax = "proto3";
package kafkamcp.test;

message Order {
  string id = 1;
  int64 qty = 2;
  string customer = 3;
}
`

// TestEngineProtobufWireFormatRoundTrip exercises the engine's
// Encode/Decode path against a real protobuf.Codec, including the
// Confluent magic-byte + schema-id prefix added by the engine and the
// inner message-index varint added by the codec.
func TestEngineProtobufWireFormatRoundTrip(t *testing.T) {
	t.Parallel()

	provider := &stubProtobufProvider{schemas: map[int]string{42: orderProtoSchema}}
	engine := NewEngineWithProtobuf(provider, codecprotobuf.New())

	order := map[string]any{
		"id":       "order-42",
		"qty":      int64(7),
		"customer": "alice",
	}
	encoded, err := engine.Encode(order, 42, FormatProtobuf)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if encoded.Data[0] != confluentMagicByte {
		t.Fatalf("missing magic byte: 0x%02x", encoded.Data[0])
	}
	if encoded.SchemaID != 42 {
		t.Errorf("schema id = %d", encoded.SchemaID)
	}
	if encoded.Format != FormatProtobuf {
		t.Errorf("format = %s", encoded.Format)
	}

	decoded, err := engine.Decode(encoded.Data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.Format != FormatProtobuf {
		t.Errorf("decoded format = %s", decoded.Format)
	}
	m, ok := decoded.Value.(map[string]any)
	if !ok {
		t.Fatalf("decoded value type %T", decoded.Value)
	}
	if m["id"] != "order-42" {
		t.Errorf("id = %v", m["id"])
	}
	if m["qty"] != int64(7) {
		t.Errorf("qty = %v (%T)", m["qty"], m["qty"])
	}
	if m["customer"] != "alice" {
		t.Errorf("customer = %v", m["customer"])
	}
}

// TestEngineProtobufStrict_NoCodec confirms that the strict path
// (no codec, no JSON fallback) refuses to silently produce JSON.
func TestEngineProtobufStrict_NoCodec(t *testing.T) {
	provider := &stubProtobufProvider{schemas: map[int]string{42: orderProtoSchema}}
	engine := NewEngine(provider)
	_, err := engine.Encode(map[string]any{"id": "x"}, 42, FormatProtobuf)
	if err == nil {
		t.Fatal("expected ErrNoProtobufCodec without registered codec")
	}
}
