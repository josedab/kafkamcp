package codec

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/codec/avro"
	"github.com/kafkamcp/kafkamcp/internal/codec/protobuf"
	"github.com/kafkamcp/kafkamcp/internal/codec/wire"
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
//
// Deprecated: prefer [wire.Magic] from `internal/codec/wire`. Kept as a
// package-level alias so any out-of-package callers that referenced the old
// name still compile.
const confluentMagicByte byte = wire.Magic

// Sentinel errors returned by the engine when it cannot fulfil a request
// because the supporting infrastructure (Schema Registry, real Avro/Protobuf
// codec) is not wired in.
var (
	// ErrNoSchemaProvider is returned by Decode when a wire-format message
	// references a schema ID but the engine has no Schema Registry provider
	// to resolve the schema.
	ErrNoSchemaProvider = errors.New("codec: no schema registry provider configured")

	// ErrNoAvroCodec is returned by Encode/Decode for FormatAvro when no real
	// Avro codec is registered. The current engine only supports JSON; Avro
	// encoding requires a real codec (e.g. linkedin/goavro). Set
	// AllowJSONFallback for development-only JSON-in-Avro-wire-format behaviour.
	ErrNoAvroCodec = errors.New("codec: avro encoding not available — no avro codec registered")

	// ErrNoProtobufCodec is the protobuf counterpart of ErrNoAvroCodec.
	ErrNoProtobufCodec = errors.New("codec: protobuf encoding not available — no protobuf codec registered")
)

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
	schema     string
	format     Format
	createdAt  time.Time
	lastAccess time.Time
}

// Engine provides encoding/decoding with Confluent wire format support.
//
// By default the engine refuses to encode Avro/Protobuf because no real codec
// is registered. Setting AllowJSONFallback (via NewEngineWithFallback) wraps
// JSON payloads in the Confluent wire-format header — useful for development
// and contract-tests, but the resulting bytes are NOT valid Avro/Protobuf and
// will NOT be readable by standards-compliant consumers.
//
// Pass a non-nil [avro.Codec] (via [NewEngineWithAvro] or [WithAvroCodec]) to
// enable real Avro encoding/decoding via github.com/hamba/avro/v2.
// Pass a non-nil [protobuf.Codec] (via [NewEngineWithProtobuf] or
// [SetProtobufCodec]) to enable real Protobuf encoding/decoding via
// github.com/bufbuild/protocompile + google.golang.org/protobuf.
type Engine struct {
	mu                sync.RWMutex
	provider          SchemaProvider
	cache             map[int]*codecEntry
	maxCache          int
	allowJSONFallback bool
	avroCodec         *avro.Codec
	protobufCodec     *protobuf.Codec
}

// NewEngine creates a new codec engine with strict semantics: Avro/Protobuf
// operations fail with ErrNoAvroCodec / ErrNoProtobufCodec when no real codec
// is registered.
func NewEngine(provider SchemaProvider) *Engine {
	return &Engine{
		provider: provider,
		cache:    make(map[int]*codecEntry),
		maxCache: 1000,
	}
}

// NewEngineWithFallback creates a codec engine that wraps JSON payloads in
// the Confluent wire-format header when asked for Avro/Protobuf. Use only in
// development. The produced bytes are NOT valid Avro/Protobuf.
func NewEngineWithFallback(provider SchemaProvider) *Engine {
	e := NewEngine(provider)
	e.allowJSONFallback = true
	return e
}

// NewEngineWithAvro creates a codec engine that performs real Avro
// encoding/decoding via the supplied [avro.Codec]. JSON fallback is
// disabled by default; combine with [Engine.SetAllowJSONFallback] if
// you need both. Pass a nil avroCodec to fall back to [NewEngine]
// semantics (Avro operations return [ErrNoAvroCodec]).
func NewEngineWithAvro(provider SchemaProvider, avroCodec *avro.Codec) *Engine {
	e := NewEngine(provider)
	e.avroCodec = avroCodec
	return e
}

// NewEngineWithProtobuf creates a codec engine that performs real
// Protobuf encoding/decoding via the supplied [protobuf.Codec]. JSON
// fallback is disabled by default. Pass a nil protobufCodec to fall
// back to [NewEngine] semantics (Protobuf operations return
// [ErrNoProtobufCodec]).
func NewEngineWithProtobuf(provider SchemaProvider, pb *protobuf.Codec) *Engine {
	e := NewEngine(provider)
	e.protobufCodec = pb
	return e
}

// SetProtobufCodec swaps the engine's Protobuf codec at runtime.
// Primarily for tests and for the platform initializer that needs to
// register the codec after engine construction.
func (e *Engine) SetProtobufCodec(pb *protobuf.Codec) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.protobufCodec = pb
}

// ProtobufCodec returns the engine's registered Protobuf codec (may
// be nil).
func (e *Engine) ProtobufCodec() *protobuf.Codec {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.protobufCodec
}

// SetAllowJSONFallback toggles the JSON-in-wire-format fallback at runtime.
// Use sparingly; primarily for tests that want to swap a strict engine into
// permissive mode.
func (e *Engine) SetAllowJSONFallback(allow bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.allowJSONFallback = allow
}

// AvroCodec returns the engine's registered Avro codec (may be nil).
func (e *Engine) AvroCodec() *avro.Codec {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.avroCodec
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
	if schemaID < 0 {
		return nil, fmt.Errorf("codec: schema ID must be non-negative, got %d", schemaID)
	}
	wireBytes := wire.Encode(uint32(schemaID), payload)
	return &EncodedMessage{Data: wireBytes, SchemaID: schemaID, Format: format}, nil
}

// Decode deserializes a wire-format message, auto-detecting format with JSON fallback.
func (e *Engine) Decode(data []byte) (*DecodedMessage, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("codec: empty data")
	}

	// Check for Confluent wire format
	if wire.IsWireFormat(data) {
		schemaIDU32, payload, err := wire.Decode(data)
		if err != nil {
			return nil, fmt.Errorf("codec: %w", err)
		}
		schemaID := int(schemaIDU32)

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
	if wire.IsWireFormat(data) {
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
// Returns ErrNoSchemaProvider when no Schema Registry provider is configured
// (unless AllowJSONFallback is enabled, in which case it returns FormatAvro
// — the historical default — so round-trips through the dev shim succeed).
func (e *Engine) resolveFormat(schemaID int) (Format, error) {
	e.mu.RLock()
	if entry, ok := e.cache[schemaID]; ok {
		e.touchLocked(schemaID, entry)
		e.mu.RUnlock()
		return entry.format, nil
	}
	e.mu.RUnlock()

	if e.provider == nil {
		if e.allowJSONFallback {
			return FormatAvro, nil
		}
		return "", ErrNoSchemaProvider
	}

	_, format, err := e.provider.GetSchemaByID(schemaID)
	if err != nil {
		return "", fmt.Errorf("schema lookup failed for ID %d: %w", schemaID, err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.evictIfFullLocked()
	e.cache[schemaID] = &codecEntry{format: format, createdAt: time.Now(), lastAccess: time.Now()}
	return format, nil
}

// encodeAvro serializes a value as Avro binary using the schema from the registry.
//
// Prefers the registered [avro.Codec] when one is set (real Avro binary
// output that's interoperable with kafka-avro-serializer consumers).
// Falls back to JSON-in-wire-format when AllowJSONFallback is set, or
// returns ErrNoAvroCodec when neither is available.
func (e *Engine) encodeAvro(value any, schemaID int) ([]byte, error) {
	schemaStr, err := e.getSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("codec: avro encode: %w", err)
	}

	codec := e.AvroCodec()
	if codec != nil && schemaStr != "" && schemaStr != "{}" {
		out, err := codec.Encode(schemaStr, value)
		if err != nil {
			return nil, fmt.Errorf("codec: avro encode: %w", err)
		}
		return out, nil
	}

	if !e.allowJSONFallback {
		return nil, ErrNoAvroCodec
	}
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("codec: avro marshal: %w", err)
	}
	return jsonBytes, nil
}

// decodeAvro deserializes Avro binary data using the schema from the registry.
//
// Prefers the registered [avro.Codec] when one is set. Falls back to
// JSON-in-wire-format when AllowJSONFallback is set, or returns
// ErrNoAvroCodec when neither is available.
func (e *Engine) decodeAvro(data []byte, schemaID int) (any, error) {
	schemaStr, err := e.getSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("codec: avro decode: %w", err)
	}

	codec := e.AvroCodec()
	if codec != nil && schemaStr != "" && schemaStr != "{}" {
		val, err := codec.Decode(schemaStr, data)
		if err != nil {
			return nil, fmt.Errorf("codec: avro decode: %w", err)
		}
		return val, nil
	}

	if !e.allowJSONFallback {
		return nil, ErrNoAvroCodec
	}
	var result any
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("codec: avro decode payload: %w", err)
	}
	return result, nil
}

// encodeProtobuf serializes a value as Confluent-wire Protobuf binary.
//
// Prefers the registered [protobuf.Codec] when one is set (real
// Protobuf binary output that's interoperable with
// kafka-protobuf-serializer consumers). Falls back to
// JSON-in-wire-format when AllowJSONFallback is set, or returns
// ErrNoProtobufCodec when neither is available.
func (e *Engine) encodeProtobuf(value any, schemaID int) ([]byte, error) {
	schemaStr, err := e.getSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("codec: protobuf encode: %w", err)
	}

	codec := e.ProtobufCodec()
	if codec != nil && schemaStr != "" && schemaStr != "{}" {
		// Default to the top-level message (index 0); deployments that
		// need multi-message schemas can call protobuf.Codec.Encode
		// directly with an explicit index.
		out, err := codec.Encode(schemaStr, nil, value)
		if err != nil {
			return nil, fmt.Errorf("codec: protobuf encode: %w", err)
		}
		return out, nil
	}

	if !e.allowJSONFallback {
		return nil, ErrNoProtobufCodec
	}
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("codec: protobuf marshal: %w", err)
	}
	prefix := []byte{0x00} // Protobuf message index varint (first message)
	return append(prefix, jsonBytes...), nil
}

// decodeProtobuf deserializes Confluent-wire Protobuf data.
//
// Prefers the registered [protobuf.Codec] when one is set. Falls back
// to JSON-in-wire-format when AllowJSONFallback is set, or returns
// ErrNoProtobufCodec when neither is available.
func (e *Engine) decodeProtobuf(data []byte, schemaID int) (any, error) {
	schemaStr, err := e.getSchema(schemaID)
	if err != nil {
		return nil, fmt.Errorf("codec: protobuf decode: %w", err)
	}

	codec := e.ProtobufCodec()
	if codec != nil && schemaStr != "" && schemaStr != "{}" {
		_, val, err := codec.Decode(schemaStr, data)
		if err != nil {
			return nil, fmt.Errorf("codec: protobuf decode: %w", err)
		}
		return val, nil
	}

	if !e.allowJSONFallback {
		return nil, ErrNoProtobufCodec
	}

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
		e.touchLocked(schemaID, entry)
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
	e.evictIfFullLocked()
	e.cache[schemaID] = &codecEntry{
		schema:     schemaStr,
		format:     format,
		createdAt:  time.Now(),
		lastAccess: time.Now(),
	}
	return schemaStr, nil
}

// touchLocked records an access for LRU bookkeeping. The caller may hold
// either RLock or Lock; we only update lastAccess (a write to a non-shared
// field on the entry pointer). Concurrent racy updates to lastAccess are
// acceptable here — the field is only consulted at eviction time, and any
// observed access from "around now" is fine for LRU purposes.
func (e *Engine) touchLocked(_ int, entry *codecEntry) {
	entry.lastAccess = time.Now()
}

// evictIfFullLocked removes the least-recently-used entry when the cache is
// at capacity. Caller must hold the write lock.
func (e *Engine) evictIfFullLocked() {
	if len(e.cache) < e.maxCache {
		return
	}
	var oldestID int
	var oldestTime time.Time
	first := true
	for id, entry := range e.cache {
		if first || entry.lastAccess.Before(oldestTime) {
			oldestID = id
			oldestTime = entry.lastAccess
			first = false
		}
	}
	delete(e.cache, oldestID)
}
