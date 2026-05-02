package schema

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ---------- NewClient ----------

func TestNewClient_HTTPSWarning(t *testing.T) {
	t.Parallel()
	// Non-HTTPS with credentials should still create a client (just logs warning)
	c, _ := NewClient("http://localhost:8081", "", "", 100, 60)
	if c == nil {
		t.Fatal("expected non-nil client")
	}
	if c.baseURL != "http://localhost:8081" {
		t.Errorf("baseURL = %q, want %q", c.baseURL, "http://localhost:8081")
	}
}

func TestNewClient_TrailingSlash(t *testing.T) {
	t.Parallel()
	c, _ := NewClient("http://localhost:8081/", "", "", 0, 0)
	if c.baseURL != "http://localhost:8081" {
		t.Errorf("baseURL = %q, want trailing slash trimmed", c.baseURL)
	}
}

func TestNewClient_DefaultCacheParams(t *testing.T) {
	t.Parallel()
	c, _ := NewClient("http://localhost:8081", "", "", 0, 0)
	stats := c.cache.Stats()
	if stats.MaxEntries != 10000 {
		t.Errorf("cache max = %d, want 10000", stats.MaxEntries)
	}
}

// ---------- GetSubjectVersions ----------

func TestGetSubjectVersions_Success(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/subjects/orders-value/versions" {
			json.NewEncoder(w).Encode([]int{1, 2, 3})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 60)
	versions, err := c.GetSubjectVersions(context.Background(), "orders-value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(versions) != 3 {
		t.Fatalf("got %d versions, want 3", len(versions))
	}
	if versions[2] != 3 {
		t.Errorf("versions[2] = %d, want 3", versions[2])
	}
}

func TestGetSubjectVersions_NotFound(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 60)
	_, err := c.GetSubjectVersions(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for 404")
	}
}

// ---------- GetLatestSchema ----------

func TestGetLatestSchema_Success(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subjects/orders-value/versions/latest":
			json.NewEncoder(w).Encode(map[string]any{
				"subject":    "orders-value",
				"version":    2,
				"id":         42,
				"schemaType": "AVRO",
				"schema":     `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`,
			})
		case "/config/orders-value":
			json.NewEncoder(w).Encode(map[string]string{"compatibilityLevel": "BACKWARD"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 60)
	schema, err := c.GetLatestSchema(context.Background(), "orders-value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schema.Subject != "orders-value" {
		t.Errorf("subject = %q, want %q", schema.Subject, "orders-value")
	}
	if schema.Version != 2 {
		t.Errorf("version = %d, want 2", schema.Version)
	}
	if schema.SchemaType != "AVRO" {
		t.Errorf("schemaType = %q, want AVRO", schema.SchemaType)
	}
	if schema.CompatibilityLevel != "BACKWARD" {
		t.Errorf("compat = %q, want BACKWARD", schema.CompatibilityLevel)
	}
}

// ---------- IsAvailable ----------

func TestIsAvailable_True(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]string{"subject-a"})
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 60)
	if !c.IsAvailable(context.Background()) {
		t.Error("expected IsAvailable to return true")
	}
}

func TestIsAvailable_False(t *testing.T) {
	t.Parallel()
	c, _ := NewClient("http://127.0.0.1:1", "", "", 100, 60)
	c.httpClient.Timeout = 100 * time.Millisecond
	if c.IsAvailable(context.Background()) {
		t.Error("expected IsAvailable to return false for unreachable server")
	}
}

func TestIsAvailable_EmptyURL(t *testing.T) {
	t.Parallel()
	c := &Client{baseURL: ""}
	if c.IsAvailable(context.Background()) {
		t.Error("expected IsAvailable to return false for empty URL")
	}
}

// ---------- generateFieldSummary ----------

func TestGenerateFieldSummary_Avro(t *testing.T) {
	t.Parallel()
	schema := map[string]any{
		"type": "record",
		"name": "Order",
		"fields": []any{
			map[string]any{"name": "id", "type": "string"},
			map[string]any{"name": "amount", "type": "double", "default": 0.0},
		},
	}
	result := generateFieldSummary(schema, "AVRO")
	if result == "" {
		t.Error("expected non-empty field summary")
	}
	if !containsStr(result, "id: string") {
		t.Errorf("summary %q missing 'id: string'", result)
	}
	if !containsStr(result, "amount: double") {
		t.Errorf("summary %q missing 'amount: double'", result)
	}
	if !containsStr(result, "default: 0") {
		t.Errorf("summary %q missing default annotation", result)
	}
}

func TestGenerateFieldSummary_NonAvro(t *testing.T) {
	t.Parallel()
	result := generateFieldSummary("some schema", "PROTOBUF")
	if result != "(PROTOBUF schema)" {
		t.Errorf("result = %q, want %q", result, "(PROTOBUF schema)")
	}
}

func TestGenerateFieldSummary_NoFields(t *testing.T) {
	t.Parallel()
	schema := map[string]any{"name": "Empty"}
	result := generateFieldSummary(schema, "AVRO")
	if result != "Empty" {
		t.Errorf("result = %q, want %q", result, "Empty")
	}
}

func TestGenerateFieldSummary_NotMap(t *testing.T) {
	t.Parallel()
	result := generateFieldSummary("not a map", "AVRO")
	if result != "" {
		t.Errorf("result = %q, want empty", result)
	}
}

// ---------- formatAvroType ----------

func TestFormatAvroType_String(t *testing.T) {
	t.Parallel()
	result := formatAvroType("string", 0)
	if result != "string" {
		t.Errorf("result = %q, want %q", result, "string")
	}
}

func TestFormatAvroType_Array(t *testing.T) {
	t.Parallel()
	result := formatAvroType(map[string]any{
		"type":  "array",
		"items": "string",
	}, 0)
	if result != "array<string>" {
		t.Errorf("result = %q, want %q", result, "array<string>")
	}
}

func TestFormatAvroType_Map(t *testing.T) {
	t.Parallel()
	result := formatAvroType(map[string]any{
		"type":   "map",
		"values": "int",
	}, 0)
	if result != "map<string, int>" {
		t.Errorf("result = %q, want %q", result, "map<string, int>")
	}
}

func TestFormatAvroType_LogicalType(t *testing.T) {
	t.Parallel()
	result := formatAvroType(map[string]any{
		"type":        "long",
		"logicalType": "timestamp-millis",
	}, 0)
	if result != "timestamp-millis" {
		t.Errorf("result = %q, want %q", result, "timestamp-millis")
	}
}

func TestFormatAvroType_NullableUnion(t *testing.T) {
	t.Parallel()
	result := formatAvroType([]any{"null", "string"}, 0)
	if result != "string?" {
		t.Errorf("result = %q, want %q", result, "string?")
	}
}

func TestFormatAvroType_MultiUnion(t *testing.T) {
	t.Parallel()
	result := formatAvroType([]any{"null", "string", "int"}, 0)
	if result != "union<null, string, int>" {
		t.Errorf("result = %q, want %q", result, "union<null, string, int>")
	}
}

func TestFormatAvroType_MaxDepth(t *testing.T) {
	t.Parallel()
	result := formatAvroType("string", maxAvroTypeDepth)
	if result != "..." {
		t.Errorf("result = %q, want %q", result, "...")
	}
}

func TestFormatAvroType_ComplexMap(t *testing.T) {
	t.Parallel()
	result := formatAvroType(map[string]any{"no_type_key": "value"}, 0)
	if result != "complex" {
		t.Errorf("result = %q, want %q", result, "complex")
	}
}

// ---------- jsonEscapeString ----------

func TestJsonEscapeString_Variants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  string
	}{
		{`hello`, `"hello"`},
		{`"quoted"`, `"\"quoted\""`},
		{"line\nbreak", `"line\nbreak"`},
	}
	for _, tt := range tests {
		got := jsonEscapeString(tt.input)
		if got != tt.want {
			t.Errorf("jsonEscapeString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// ---------- Cache integration ----------

func TestCacheGetSet(t *testing.T) {
	t.Parallel()
	c := NewCache(10, 5*time.Minute)
	info := &SchemaInfo{Subject: "test", Version: 1, SchemaID: 1}

	// Miss
	if _, ok := c.Get("key1"); ok {
		t.Error("expected cache miss")
	}

	// Set and hit
	c.Set("key1", info)
	got, ok := c.Get("key1")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got.Subject != "test" {
		t.Errorf("subject = %q, want %q", got.Subject, "test")
	}
}

func TestCacheEviction_LRU(t *testing.T) {
	t.Parallel()
	c := NewCache(2, 5*time.Minute)
	c.Set("a", &SchemaInfo{Subject: "a"})
	c.Set("b", &SchemaInfo{Subject: "b"})
	c.Set("c", &SchemaInfo{Subject: "c"}) // should evict "a"

	if _, ok := c.Get("a"); ok {
		t.Error("expected 'a' to be evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("expected 'b' to be present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("expected 'c' to be present")
	}
}

func TestCacheStats(t *testing.T) {
	t.Parallel()
	c := NewCache(100, 5*time.Minute)
	c.Set("a", &SchemaInfo{Subject: "a"})
	c.Set("b", &SchemaInfo{Subject: "b"})

	stats := c.Stats()
	if stats.Size != 2 {
		t.Errorf("size = %d, want 2", stats.Size)
	}
	if stats.MaxEntries != 100 {
		t.Errorf("maxEntries = %d, want 100", stats.MaxEntries)
	}
}

func TestCacheUpdate(t *testing.T) {
	t.Parallel()
	c := NewCache(10, 5*time.Minute)
	c.Set("key", &SchemaInfo{Subject: "v1"})
	c.Set("key", &SchemaInfo{Subject: "v2"})

	got, ok := c.Get("key")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if got.Subject != "v2" {
		t.Errorf("subject = %q, want %q", got.Subject, "v2")
	}
	if c.Stats().Size != 1 {
		t.Errorf("size = %d, want 1", c.Stats().Size)
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && findSubstr(s, substr)
}

func findSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
