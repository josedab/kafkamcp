// Package avro is a thin, allocation-conscious wrapper around
// github.com/hamba/avro/v2 that gives KafkaMCP a first-class Avro
// binary wire-format encoder/decoder. It is intentionally decoupled
// from the Confluent wire-format prefix (see [internal/codec/wire])
// so it can be reused for stand-alone Avro payloads (OCF files,
// archive blobs) as well.
//
// # Design
//
// hamba/avro parses Avro schemas once and reuses the compiled schema
// object across all serialise/deserialise calls. We cache parsed
// schemas by their raw text in [Codec] so producers that re-use the
// same schema string don't pay the parse cost twice.
//
// hamba/avro's NativeFromTextual / TextualFromNative APIs operate on
// generic any values. KafkaMCP messages routinely come in as
// map[string]any (decoded JSON) and Avro's strict mapping
// (`int` → int32, `long` → int64, `double` → float64, etc.) tends to
// trip Go's interface-conversion rules. We pre-normalise common
// integer types so that incoming JSON `float64`s map cleanly onto
// `int` / `long` Avro fields.
//
// # Concurrency
//
// [Codec] is safe for concurrent use; the cache is guarded by
// sync.RWMutex and parsed schemas are immutable.
package avro

import (
	"errors"
	"fmt"
	"sync"

	hamba "github.com/hamba/avro/v2"
)

// ErrSchemaParse is returned when an Avro schema string cannot be
// parsed.
var ErrSchemaParse = errors.New("avro: schema parse failed")

// Codec is a cached Avro encoder/decoder. The zero value is ready to
// use; consider [New] for the documented constructor.
type Codec struct {
	mu    sync.RWMutex
	cache map[string]hamba.Schema
}

// New returns an Avro codec with an empty parsed-schema cache.
func New() *Codec {
	return &Codec{cache: make(map[string]hamba.Schema)}
}

// parse returns the compiled Schema for schemaStr, caching the result.
func (c *Codec) parse(schemaStr string) (hamba.Schema, error) {
	c.mu.RLock()
	if sch, ok := c.cache[schemaStr]; ok {
		c.mu.RUnlock()
		return sch, nil
	}
	c.mu.RUnlock()

	sch, err := hamba.Parse(schemaStr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSchemaParse, err)
	}

	c.mu.Lock()
	if c.cache == nil {
		c.cache = make(map[string]hamba.Schema)
	}
	c.cache[schemaStr] = sch
	c.mu.Unlock()

	return sch, nil
}

// Encode serialises value into the canonical Avro binary form defined
// by schemaStr. value should already conform to the schema (record
// fields named, unions wrapped in map[string]any{"type": value}, ...).
//
// Common type mismatches are normalised: int / int32 / int64 / float64
// are converted to the Avro-expected type where the field is numeric.
// Beyond that, schema/value mismatches surface as errors from
// hamba/avro untouched.
func (c *Codec) Encode(schemaStr string, value any) ([]byte, error) {
	sch, err := c.parse(schemaStr)
	if err != nil {
		return nil, err
	}
	normalised := normaliseValue(value)
	data, err := hamba.Marshal(sch, normalised)
	if err != nil {
		return nil, fmt.Errorf("avro: marshal: %w", err)
	}
	return data, nil
}

// Decode deserialises Avro binary data using schemaStr and returns the
// resulting generic value (map[string]any for records, []any for
// arrays, etc.).
func (c *Codec) Decode(schemaStr string, data []byte) (any, error) {
	sch, err := c.parse(schemaStr)
	if err != nil {
		return nil, err
	}
	var out any
	if err := hamba.Unmarshal(sch, data, &out); err != nil {
		return nil, fmt.Errorf("avro: unmarshal: %w", err)
	}
	return out, nil
}

// DecodeInto deserialises into a caller-provided target (a Go struct
// matching the schema). Use this when you know the concrete record
// type and want field-name strict typing.
func (c *Codec) DecodeInto(schemaStr string, data []byte, target any) error {
	sch, err := c.parse(schemaStr)
	if err != nil {
		return err
	}
	if err := hamba.Unmarshal(sch, data, target); err != nil {
		return fmt.Errorf("avro: unmarshal into target: %w", err)
	}
	return nil
}

// ParseSchema validates that schemaStr is a syntactically valid Avro
// schema without serialising anything. Useful for the
// kafka_check_schema_compatibility tool.
func (c *Codec) ParseSchema(schemaStr string) error {
	_, err := c.parse(schemaStr)
	return err
}

// CacheSize returns the number of parsed schemas held by the codec.
// Helps tests assert that the cache is actually doing its job.
func (c *Codec) CacheSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// normaliseValue widens / narrows common Go numeric types so callers
// passing JSON-decoded `float64`s for integer Avro fields don't trip
// hamba/avro's strict type check.
func normaliseValue(in any) any {
	switch v := in.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, vv := range v {
			out[k] = normaliseValue(vv)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, vv := range v {
			out[i] = normaliseValue(vv)
		}
		return out
	default:
		return v
	}
}
