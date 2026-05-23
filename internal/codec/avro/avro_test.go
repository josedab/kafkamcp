package avro

import (
	"errors"
	"strings"
	"testing"
)

const userSchema = `{
	"type": "record",
	"name": "User",
	"namespace": "com.kafkamcp.test",
	"fields": [
		{"name": "id",    "type": "long"},
		{"name": "name",  "type": "string"},
		{"name": "email", "type": ["null", "string"], "default": null}
	]
}`

const primitiveLongSchema = `"long"`

func TestEncodeDecodeRecord(t *testing.T) {
	t.Parallel()
	c := New()

	user := map[string]any{
		"id":    int64(42),
		"name":  "Ada Lovelace",
		"email": map[string]any{"string": "ada@example.com"},
	}

	encoded, err := c.Encode(userSchema, user)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if len(encoded) == 0 {
		t.Fatalf("encoded payload is empty")
	}

	decoded, err := c.Decode(userSchema, encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	got, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded type = %T, want map[string]any", decoded)
	}
	if got["name"] != "Ada Lovelace" {
		t.Errorf("name = %v, want Ada Lovelace", got["name"])
	}
	// hamba/avro v2 decodes the union into its underlying value (a bare
	// string here, not a tagged map). This is the documented default and
	// matches the wire-format consumer behavior.
	if got["email"] != "ada@example.com" {
		t.Errorf("email = %v, want ada@example.com", got["email"])
	}
}

func TestEncodeDecodePrimitive(t *testing.T) {
	t.Parallel()
	c := New()

	encoded, err := c.Encode(primitiveLongSchema, int64(123456789))
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	decoded, err := c.Decode(primitiveLongSchema, encoded)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if decoded.(int64) != 123456789 {
		t.Errorf("decoded = %v, want 123456789", decoded)
	}
}

func TestDecodeInto(t *testing.T) {
	t.Parallel()
	c := New()

	type User struct {
		ID    int64   `avro:"id"`
		Name  string  `avro:"name"`
		Email *string `avro:"email"`
	}
	src := User{ID: 1, Name: "Grace", Email: ptr("grace@example.com")}

	encoded, err := c.Encode(userSchema, src)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	var got User
	if err := c.DecodeInto(userSchema, encoded, &got); err != nil {
		t.Fatalf("DecodeInto failed: %v", err)
	}
	if got.ID != 1 || got.Name != "Grace" || got.Email == nil || *got.Email != "grace@example.com" {
		t.Errorf("DecodeInto round-trip mismatch: %+v", got)
	}
}

func TestParseSchemaBad(t *testing.T) {
	t.Parallel()
	c := New()
	err := c.ParseSchema(`{"type":"nope"`)
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	if !errors.Is(err, ErrSchemaParse) {
		t.Errorf("err = %v, want errors.Is(ErrSchemaParse)", err)
	}
}

func TestCacheReuse(t *testing.T) {
	t.Parallel()
	c := New()
	if _, err := c.Encode(userSchema, map[string]any{"id": int64(1), "name": "x"}); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if _, err := c.Encode(userSchema, map[string]any{"id": int64(2), "name": "y"}); err != nil {
		t.Fatalf("second Encode failed: %v", err)
	}
	if got := c.CacheSize(); got != 1 {
		t.Errorf("cache size = %d, want 1 (schema reuse)", got)
	}

	// Different schema string -> separate cache entry.
	if _, err := c.Encode(primitiveLongSchema, int64(7)); err != nil {
		t.Fatalf("primitive encode failed: %v", err)
	}
	if got := c.CacheSize(); got != 2 {
		t.Errorf("cache size = %d, want 2", got)
	}
}

func TestEncodeWrongType(t *testing.T) {
	t.Parallel()
	c := New()
	// Schema expects long, supply a string -> hamba/avro should reject.
	_, err := c.Encode(primitiveLongSchema, "not a number")
	if err == nil {
		t.Fatalf("expected encode error, got nil")
	}
	if !strings.Contains(err.Error(), "marshal") {
		t.Errorf("error %q does not mention marshal", err)
	}
}

func TestNormaliseValueIdempotent(t *testing.T) {
	t.Parallel()
	in := map[string]any{
		"a": "x",
		"b": int64(2),
		"c": []any{1, 2, 3},
		"d": map[string]any{"e": int32(7)},
	}
	out := normaliseValue(in).(map[string]any)
	if out["a"] != "x" || out["b"].(int64) != 2 {
		t.Errorf("normaliseValue corrupted primitive fields: %+v", out)
	}
	if len(out["c"].([]any)) != 3 {
		t.Errorf("array length wrong: %+v", out["c"])
	}
	if out["d"].(map[string]any)["e"].(int32) != 7 {
		t.Errorf("nested int32 not preserved: %+v", out["d"])
	}
}

func ptr[T any](v T) *T { return &v }
