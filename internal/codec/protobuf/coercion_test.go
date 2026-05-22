package protobuf

import (
	"testing"
)

func TestToInt64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      any
		want    int64
		wantErr bool
	}{
		{"int", int(42), 42, false},
		{"int32", int32(99), 99, false},
		{"int64", int64(123), 123, false},
		{"uint", uint(7), 7, false},
		{"uint32", uint32(8), 8, false},
		{"uint64", uint64(9), 9, false},
		{"float32", float32(1.6), 1, false},
		{"float64", float64(2.9), 2, false},
		{"string fails", "abc", 0, true},
		{"nil fails", nil, 0, true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := toInt64(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("got %d want %d", got, tc.want)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      any
		want    float64
		wantErr bool
	}{
		{"float32", float32(1.5), 1.5, false},
		{"float64", float64(2.5), 2.5, false},
		{"int", int(10), 10, false},
		{"int32", int32(20), 20, false},
		{"int64", int64(30), 30, false},
		{"uint64 not supported", uint64(5), 0, true},
		{"string fails", "abc", 0, true},
		{"nil fails", nil, 0, true},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := toFloat64(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v got=%v", err, tc.wantErr, got)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("got %v want %v", got, tc.want)
			}
		})
	}
}

const proto3SchemaWithScalars = `
syntax = "proto3";
package test;
message ScalarMessage {
	string id = 1;
	int32 n32 = 2;
	int64 n64 = 3;
	double d = 4;
	float f = 5;
	bool b = 6;
	bytes blob = 7;
	repeated string tags = 8;
}
`

func TestEncodeDecode_AllScalars(t *testing.T) {
	t.Parallel()
	c := New()
	value := map[string]any{
		"id":   "abc",
		"n32":  int32(7),
		"n64":  int64(99),
		"d":    float64(1.5),
		"f":    float32(2.5),
		"b":    true,
		"blob": []byte{1, 2, 3},
		"tags": []any{"x", "y"},
	}
	encoded, err := c.Encode(proto3SchemaWithScalars, []int{0}, value)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	_, decoded, err := c.Decode(proto3SchemaWithScalars, encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	out, ok := decoded.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", decoded)
	}
	if out["id"] != "abc" {
		t.Errorf("id=%v", out["id"])
	}
	if out["b"] != true {
		t.Errorf("b=%v", out["b"])
	}
}

func TestEncode_TypeCoercion(t *testing.T) {
	t.Parallel()
	c := New()
	// Send float64 for int fields (matches JSON decoding).
	value := map[string]any{
		"id":  "x",
		"n32": float64(5),
		"n64": float64(10),
		"d":   int(1),
		"f":   int(2),
	}
	if _, err := c.Encode(proto3SchemaWithScalars, []int{0}, value); err != nil {
		t.Fatalf("Encode with coercion: %v", err)
	}
}

func TestEncode_UnknownMessage(t *testing.T) {
	t.Parallel()
	c := New()
	if _, err := c.Encode(proto3SchemaWithScalars, []int{999}, map[string]any{}); err == nil {
		t.Fatal("expected error for unknown message index")
	}
}

func TestEncode_UnknownField(t *testing.T) {
	t.Parallel()
	c := New()
	// Field that doesn't exist in the message should be silently ignored
	// (matches protoreflect behavior).
	value := map[string]any{"nonexistent_field": "val"}
	if _, err := c.Encode(proto3SchemaWithScalars, []int{0}, value); err != nil {
		t.Fatalf("Encode should ignore unknown fields, got: %v", err)
	}
}
