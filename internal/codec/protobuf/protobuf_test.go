package protobuf

import (
	"bytes"
	"errors"
	"testing"
)

const simpleSchema = `
syntax = "proto3";
package kafkamcp.test;

message Order {
  string id = 1;
  int64 qty = 2;
  string customer = 3;
  repeated string tags = 4;
}
`

const multiMessageSchema = `
syntax = "proto3";
package kafkamcp.test;

message Order {
  string id = 1;
  int64 qty = 2;
}

message Invoice {
  string id = 1;
  double total = 2;
}
`

const nestedSchema = `
syntax = "proto3";
package kafkamcp.test;

message Outer {
  string id = 1;
  message Inner {
    string nested_id = 1;
    int32 count = 2;
  }
  Inner detail = 2;
}
`

func TestRoundTrip_Simple(t *testing.T) {
	c := New()
	val := map[string]any{
		"id":       "order-1",
		"qty":      int64(42),
		"customer": "acme",
		"tags":     []any{"priority", "wholesale"},
	}
	data, err := c.Encode(simpleSchema, nil, val)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Single-message schema should use the optimised 0x00 prefix.
	if data[0] != 0x00 {
		t.Errorf("expected 0x00 prefix, got 0x%02x", data[0])
	}
	idx, decoded, err := c.Decode(simpleSchema, data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(idx) != 1 || idx[0] != 0 {
		t.Errorf("idx = %v, want [0]", idx)
	}
	m, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("decoded = %T", decoded)
	}
	if m["id"] != "order-1" {
		t.Errorf("id = %v", m["id"])
	}
	if m["qty"] != int64(42) {
		t.Errorf("qty = %v (%T)", m["qty"], m["qty"])
	}
	if m["customer"] != "acme" {
		t.Errorf("customer = %v", m["customer"])
	}
	tags, ok := m["tags"].([]any)
	if !ok || len(tags) != 2 || tags[0] != "priority" || tags[1] != "wholesale" {
		t.Errorf("tags = %v", m["tags"])
	}
}

func TestRoundTrip_MultiMessage(t *testing.T) {
	c := New()
	// Encode an Invoice (index 1) — exercises the multi-byte index path.
	val := map[string]any{
		"id":    "INV-99",
		"total": 199.95,
	}
	data, err := c.Encode(multiMessageSchema, []int{1}, val)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// {1} → varint(1) varint(1) = two bytes
	if len(data) < 2 || data[0] == 0x00 {
		t.Errorf("expected 2-byte index prefix, got %x", data[:2])
	}
	idx, decoded, err := c.Decode(multiMessageSchema, data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(idx) != 1 || idx[0] != 1 {
		t.Errorf("idx = %v, want [1]", idx)
	}
	m := decoded.(map[string]any)
	if m["id"] != "INV-99" {
		t.Errorf("id = %v", m["id"])
	}
	if m["total"] != 199.95 {
		t.Errorf("total = %v", m["total"])
	}
}

func TestRoundTrip_Nested(t *testing.T) {
	c := New()
	val := map[string]any{
		"id": "outer-1",
		"detail": map[string]any{
			"nestedId": "inner-1",
			"count":    int32(7),
		},
	}
	data, err := c.Encode(nestedSchema, []int{0}, val)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	idx, decoded, err := c.Decode(nestedSchema, data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(idx) != 1 || idx[0] != 0 {
		t.Errorf("idx = %v", idx)
	}
	m := decoded.(map[string]any)
	if m["id"] != "outer-1" {
		t.Errorf("id = %v", m["id"])
	}
	inner, ok := m["detail"].(map[string]any)
	if !ok {
		t.Fatalf("detail = %T", m["detail"])
	}
	if inner["nestedId"] != "inner-1" {
		t.Errorf("nested id = %v", inner["nestedId"])
	}
	if inner["count"] != int32(7) {
		t.Errorf("count = %v (%T)", inner["count"], inner["count"])
	}
}

func TestMessageIndexEncoding(t *testing.T) {
	cases := []struct {
		name string
		idx  []int
		want []byte
	}{
		{"nil-default", nil, []byte{0x00}},
		{"zero-singleton", []int{0}, []byte{0x00}},
		{"single-one", []int{1}, []byte{0x02, 0x02}},        // count=1 varint, idx=1 zigzag→2
		{"two-elems", []int{1, 0}, []byte{0x04, 0x02, 0x00}}, // count=2, idx=1, idx=0
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := encodeMessageIndex(tc.idx)
			if !bytes.Equal(got, tc.want) {
				t.Errorf("got %x, want %x", got, tc.want)
			}
		})
	}
}

func TestDecodeMessageIndex_RoundTrip(t *testing.T) {
	for _, idx := range [][]int{nil, {0}, {1}, {3}, {1, 0}, {2, 1, 4}} {
		enc := encodeMessageIndex(idx)
		got, rest, err := decodeMessageIndex(append(enc, 0xDE, 0xAD))
		if err != nil {
			t.Fatalf("idx=%v decode: %v", idx, err)
		}
		expect := idx
		if len(expect) == 0 {
			expect = []int{0}
		}
		if len(got) != len(expect) {
			t.Fatalf("idx=%v got %v want %v", idx, got, expect)
		}
		for i := range expect {
			if got[i] != expect[i] {
				t.Fatalf("idx=%v position %d: got %d want %d", idx, i, got[i], expect[i])
			}
		}
		if !bytes.Equal(rest, []byte{0xDE, 0xAD}) {
			t.Errorf("rest = %x", rest)
		}
	}
}

func TestEncode_BadSchema(t *testing.T) {
	c := New()
	_, err := c.Encode("not a real proto schema", nil, map[string]any{})
	if !errors.Is(err, ErrSchemaParse) {
		t.Errorf("err = %v, want ErrSchemaParse", err)
	}
}

func TestEncode_BadIndex(t *testing.T) {
	c := New()
	_, err := c.Encode(simpleSchema, []int{5}, map[string]any{"id": "x"})
	if !errors.Is(err, ErrMessageIndex) {
		t.Errorf("err = %v, want ErrMessageIndex", err)
	}
}

func TestEncode_RejectsCrossFileImport(t *testing.T) {
	c := New()
	schema := `
syntax = "proto3";
import "some/other/file.proto";
message X { string id = 1; }
`
	_, err := c.Encode(schema, nil, map[string]any{"id": "x"})
	if err == nil {
		t.Error("expected error for cross-file import")
	}
	if !errors.Is(err, ErrSchemaParse) {
		t.Errorf("err = %v, want ErrSchemaParse", err)
	}
}

func TestCacheReuse(t *testing.T) {
	c := New()
	if _, err := c.Encode(simpleSchema, nil, map[string]any{"id": "x"}); err != nil {
		t.Fatal(err)
	}
	if c.CacheSize() != 1 {
		t.Errorf("cache size = %d", c.CacheSize())
	}
	if _, err := c.Encode(simpleSchema, nil, map[string]any{"id": "y"}); err != nil {
		t.Fatal(err)
	}
	if c.CacheSize() != 1 {
		t.Errorf("cache size after re-use = %d", c.CacheSize())
	}
}

func TestParseSchema(t *testing.T) {
	c := New()
	if err := c.ParseSchema(simpleSchema); err != nil {
		t.Errorf("valid schema rejected: %v", err)
	}
	if err := c.ParseSchema("bogus"); err == nil {
		t.Error("invalid schema accepted")
	}
}
