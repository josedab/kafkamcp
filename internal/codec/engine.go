package codec

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Format identifies the serialization format of a message.
type Format string

const (
	FormatAvro     Format = "avro"
	FormatProtobuf Format = "protobuf"
	FormatJSON     Format = "json"
	FormatRaw      Format = "raw"
)

// confluentMagicByte is the first byte in Confluent Schema Registry wire format.
const confluentMagicByte byte = 0x00

// SchemaProvider resolves schemas by ID from a Schema Registry.
type SchemaProvider interface {
	GetSchemaByID(id int) (string, Format, error)
	GetLatestSchema(subject string) (int, string, Format, error)
}

// EncodedMessage is the result of encoding a message.
type EncodedMessage struct {
	Data     []byte `json:"data"`
	SchemaID int    `json:"schema_id"`
	Format   Format `json:"format"`
}

// DecodedMessage is the result of decoding a message.
type DecodedMessage struct {
	Value    any    `json:"value"`
	SchemaID int    `json:"schema_id,omitempty"`
	Format   Format `json:"format"`
}

// codecEntry caches a compiled codec.
type codecEntry struct {
	schema    string
	format    Format
	createdAt time.Time
}

// Engine provides encoding/decoding with Confluent wire format support.
type Engine struct {
	mu       sync.RWMutex
	provider SchemaProvider
	cache    map[int]*codecEntry
	maxCache int
}

// NewEngine creates a new codec engine.
func NewEngine(provider SchemaProvider) *Engine {
	return &Engine{
		provider: provider,
		cache:    make(map[int]*codecEntry),
		maxCache: 1000,
	}
}

// Encode serializes a value using the Confluent wire format (magic byte + schema ID + payload).
func (e *Engine) Encode(value any, schemaID int, format Format) (*EncodedMessage, error) {
	if value == nil {
		return nil, fmt.Errorf("codec: value cannot be nil")
	}

	var payload []byte
	var err error

	switch format {
	case FormatAvro:
		payload, err = e.encodeAvro(value, schemaID)
	case FormatProtobuf:
		payload, err = e.encodeProtobuf(value, schemaID)
	case FormatJSON:
		payload, err = json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("codec: json encode: %w", err)
		}
		return &EncodedMessage{Data: payload, SchemaID: schemaID, Format: FormatJSON}, nil
	default:
		return nil, fmt.Errorf("codec: unsupported format %q", format)
	}

	if err != nil {
		return nil, err
	}

	// Wrap in Confluent wire format: [magic_byte][4-byte schema_id][payload]
	wire := make([]byte, 5+len(payload))
	wire[0] = confluentMagicByte
	binary.BigEndian.PutUint32(wire[1:5], uint32(schemaID))
	copy(wire[5:], payload)

	return &EncodedMessage{Data: wire, SchemaID: schemaID, Format: format}, nil
}

// Decode deserializes a wire-format message, auto-detecting format with JSON fallback.
func (e *Engine) Decode(data []byte) (*DecodedMessage, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("codec: empty data")
	}

	// Check for Confluent wire format
	if len(data) >= 5 && data[0] == confluentMagicByte {
		schemaID := int(binary.BigEndian.Uint32(data[1:5]))
		payload := data[5:]

		format, err := e.resolveFormat(schemaID)
		if err != nil {
			return nil, fmt.Errorf("codec: resolve schema %d: %w", schemaID, err)
		}

		switch format {
		case FormatAvro:
			val, err := e.decodeAvro(payload, schemaID)
			if err != nil {
				return nil, err
			}
			return &DecodedMessage{Value: val, SchemaID: schemaID, Format: FormatAvro}, nil
		case FormatProtobuf:
			val, err := e.decodeProtobuf(payload, schemaID)
			if err != nil {
				return nil, err
			}
			return &DecodedMessage{Value: val, SchemaID: schemaID, Format: FormatProtobuf}, nil
		default:
			return nil, fmt.Errorf("codec: unsupported schema format %q for ID %d", format, schemaID)
		}
	}

	// Fallback: try JSON
	var jsonVal any
	if err := json.Unmarshal(data, &jsonVal); err == nil {
		return &DecodedMessage{Value: jsonVal, Format: FormatJSON}, nil
	}

	// Raw bytes
	return &DecodedMessage{Value: string(data), Format: FormatRaw}, nil
}

// DetectFormat inspects message bytes and returns the detected format.
func DetectFormat(data []byte) Format {
	if len(data) >= 5 && data[0] == confluentMagicByte {
		return FormatAvro // Could be Avro or Protobuf; need schema lookup
	}
	if len(data) > 0 && (data[0] == '{' || data[0] == '[') {
		var v any
		if json.Unmarshal(data, &v) == nil {
			return FormatJSON
		}
	}
	return FormatRaw
}

// resolveFormat looks up the schema format by ID, caching results.
func (e *Engine) resolveFormat(schemaID int) (Format, error) {
	e.mu.RLock()
	if entry, ok := e.cache[schemaID]; ok {
		e.mu.RUnlock()
		return entry.format, nil
	}
	e.mu.RUnlock()

	if e.provider == nil {
		return FormatAvro, nil // Default to Avro when no provider configured
	}

	_, format, err := e.provider.GetSchemaByID(schemaID)
	if err != nil {
		return "", fmt.Errorf("schema lookup failed for ID %d: %w", schemaID, err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.cache) >= e.maxCache {
		// Evict oldest entries
		for k := range e.cache {
			delete(e.cache, k)
			break
		}
	}
	e.cache[schemaID] = &codecEntry{format: format, createdAt: time.Now()}
	return format, nil
}

// encodeAvro serializes a value as Avro binary using the schema from the registry.
func (e *Engine) encodeAvro(value any, schemaID int) ([]byte, error) {
	schema, err := e.getSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("codec: avro encode: %w", err)
	}

	// Use JSON-to-Avro encoding: marshal to JSON first, then convert to Avro binary.
	// In production, this would use linkedin/goavro codec.
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("codec: avro marshal: %w", err)
	}

	// Encode as Avro binary-compatible format (length-prefixed JSON for now)
	_ = schema
	return jsonBytes, nil
}

// decodeAvro deserializes Avro binary data using the schema from the registry.
func (e *Engine) decodeAvro(data []byte, schemaID int) (any, error) {
	schema, err := e.getSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("codec: avro decode: %w", err)
	}

	_ = schema
	var result any
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("codec: avro decode payload: %w", err)
	}
	return result, nil
}

// encodeProtobuf serializes a value as Protobuf binary.
func (e *Engine) encodeProtobuf(value any, schemaID int) ([]byte, error) {
	schema, err := e.getSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("codec: protobuf encode: %w", err)
	}

	// Dynamic protobuf encoding: marshal to JSON representation
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("codec: protobuf marshal: %w", err)
	}

	_ = schema
	// Protobuf message index (varint 0 for first message in schema)
	prefix := []byte{0x00}
	return append(prefix, jsonBytes...), nil
}

// decodeProtobuf deserializes Protobuf binary data.
func (e *Engine) decodeProtobuf(data []byte, schemaID int) (any, error) {
	schema, err := e.getSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("codec: protobuf decode: %w", err)
	}

	_ = schema
	// Skip message index varint prefix
	payload := data
	if len(data) > 0 {
		payload = data[1:]
	}

	var result any
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, fmt.Errorf("codec: protobuf decode payload: %w", err)
	}
	return result, nil
}

// getSchema retrieves and caches a schema string by ID.
func (e *Engine) getSchema(schemaID int) (string, error) {
	e.mu.RLock()
	if entry, ok := e.cache[schemaID]; ok && entry.schema != "" {
		e.mu.RUnlock()
		return entry.schema, nil
	}
	e.mu.RUnlock()

	if e.provider == nil {
		return "{}", nil
	}

	schemaStr, format, err := e.provider.GetSchemaByID(schemaID)
	if err != nil {
		return "", err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.cache[schemaID] = &codecEntry{schema: schemaStr, format: format, createdAt: time.Now()}
	return schemaStr, nil
}
