package schema

import (
	"testing"
	"time"
)

func TestCacheSetAndGet(t *testing.T) {
	c := NewCache(100, 5*time.Minute)
	info := &SchemaInfo{Subject: "test", Version: 1, SchemaID: 42, SchemaType: "AVRO"}

	c.Set("test:latest", info)
	got, ok := c.Get("test:latest")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got.SchemaID != 42 {
		t.Errorf("expected SchemaID 42, got %d", got.SchemaID)
	}
}

func TestCacheMiss(t *testing.T) {
	c := NewCache(100, 5*time.Minute)
	_, ok := c.Get("nonexistent")
	if ok {
		t.Fatal("expected cache miss")
	}
}

func TestCacheTTLExpiry(t *testing.T) {
	c := NewCache(100, 1*time.Millisecond)
	info := &SchemaInfo{Subject: "test", Version: 1}
	c.Set("test:1", info)

	time.Sleep(5 * time.Millisecond)
	_, ok := c.Get("test:1")
	if ok {
		t.Fatal("expected cache miss after TTL expiry")
	}
}

func TestCacheEviction(t *testing.T) {
	c := NewCache(2, 5*time.Minute)
	c.Set("a", &SchemaInfo{Subject: "a"})
	c.Set("b", &SchemaInfo{Subject: "b"})
	c.Set("c", &SchemaInfo{Subject: "c"}) // should evict "a"

	stats := c.Stats()
	if stats.Size != 2 {
		t.Errorf("expected size 2 after eviction, got %d", stats.Size)
	}
}

func TestGenerateFieldSummary(t *testing.T) {
	schema := map[string]any{
		"type": "record",
		"name": "Order",
		"fields": []any{
			map[string]any{"name": "id", "type": "string"},
			map[string]any{"name": "amount", "type": "double"},
			map[string]any{"name": "currency", "type": "string", "default": "USD"},
		},
	}

	summary := generateFieldSummary(schema, "AVRO")
	if summary == "" {
		t.Fatal("expected non-empty summary")
	}
	if !containsAll(summary, "Order", "id", "amount", "currency", "USD") {
		t.Errorf("summary missing expected content: %s", summary)
	}
}

func TestGenerateFieldSummaryNonAvro(t *testing.T) {
	summary := generateFieldSummary(map[string]any{}, "PROTOBUF")
	if summary != "(PROTOBUF schema)" {
		t.Errorf("unexpected summary for protobuf: %s", summary)
	}
}

func TestFormatAvroType(t *testing.T) {
	tests := []struct {
		name string
		typ  any
		want string
	}{
		{"string", "string", "string"},
		{"int", "int", "int"},
		{"nullable", []any{"null", "string"}, "string?"},
		{"array", map[string]any{"type": "array", "items": "string"}, "array<string>"},
		{"logical", map[string]any{"type": "long", "logicalType": "timestamp-millis"}, "timestamp-millis"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatAvroType(tt.typ, 0)
			if got != tt.want {
				t.Errorf("formatAvroType(%v) = %q, want %q", tt.typ, got, tt.want)
			}
		})
	}
}

func containsAll(s string, substrs ...string) bool {
	for _, sub := range substrs {
		found := false
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
