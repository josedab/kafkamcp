package kafka

import (
	"encoding/base64"
	"testing"
	"time"
)

func TestDecodeValueAuto(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []byte
		want any
	}{
		{"nil", nil, nil},
		{"empty", []byte{}, nil},
		{"string", []byte("hello"), "hello"},
		{"json object", []byte(`{"key":"value"}`), map[string]any{"key": "value"}},
		{"json array", []byte(`[1,2,3]`), []any{float64(1), float64(2), float64(3)}},
		{"json number", []byte(`42`), float64(42)},
		{"invalid json", []byte(`not json {`), "not json {"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decodeValue(tt.data, "auto")
			if got == nil && tt.want == nil {
				return
			}
			gotStr, wantStr := serializeValue(got), serializeValue(tt.want)
			if gotStr != wantStr {
				t.Errorf("decodeValue(%q, auto) = %v, want %v", tt.data, got, tt.want)
			}
		})
	}
}

func TestDecodeValueBytes(t *testing.T) {
	t.Parallel()
	data := []byte{0x00, 0x01, 0xFF}
	got := decodeValue(data, "bytes")
	want := base64.StdEncoding.EncodeToString(data)
	if got != want {
		t.Errorf("decodeValue(bytes) = %v, want %v", got, want)
	}
}

func TestDecodeValueString(t *testing.T) {
	t.Parallel()
	data := []byte(`{"key":"value"}`)
	got := decodeValue(data, "string")
	if got != `{"key":"value"}` {
		t.Errorf("decodeValue(string) = %v, want raw string", got)
	}
}

func TestDecodeValueJSON(t *testing.T) {
	t.Parallel()
	data := []byte(`{"key":"value"}`)
	got := decodeValue(data, "json")
	m, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map, got %T", got)
	}
	if m["key"] != "value" {
		t.Errorf("expected key=value, got %v", m)
	}
}

func TestSerializeValue(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		val  any
		want string
	}{
		{"nil", nil, ""},
		{"string", "hello", "hello"},
		{"number", 42, "42"},
		{"map", map[string]string{"a": "b"}, `{"a":"b"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := serializeValue(tt.val)
			if got != tt.want {
				t.Errorf("serializeValue(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}

func TestJavaTimeout(t *testing.T) {
	t.Parallel()
	if parseDurationMs(0) != 5*time.Second {
		t.Error("expected default 5s for 0")
	}
	if parseDurationMs(-1) != 5*time.Second {
		t.Error("expected default 5s for -1")
	}
	if parseDurationMs(1000) != time.Second {
		t.Errorf("expected 1s, got %v", parseDurationMs(1000))
	}
}

func TestParseTimestamp(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input   string
		wantErr bool
	}{
		{"2025-07-15T14:32:01Z", false},
		{"2025-07-15T14:32:01.234Z", false},
		{"2025-07-15", false},
		{"not a timestamp", true},
		{"", true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			_, err := parseTimestamp(tt.input)
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
