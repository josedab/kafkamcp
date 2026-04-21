package schema

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	client, _ := NewClient("http://localhost:8081", "", "", 100, 300)
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.baseURL != "http://localhost:8081" {
		t.Errorf("unexpected baseURL: %s", client.baseURL)
	}
}

func TestNewClientTrimsSlash(t *testing.T) {
	client, _ := NewClient("http://localhost:8081/", "", "", 100, 300)
	if client.baseURL != "http://localhost:8081" {
		t.Errorf("expected trailing slash trimmed, got %s", client.baseURL)
	}
}

func TestIsAvailableNoURL(t *testing.T) {
	client, _ := NewClient("", "", "", 100, 300)
	if client.IsAvailable() {
		t.Error("expected IsAvailable=false for empty URL")
	}
}

func TestIsAvailableUnreachable(t *testing.T) {
	client, _ := NewClient("http://localhost:1", "", "", 100, 300)
	if client.IsAvailable() {
		t.Error("expected IsAvailable=false for unreachable server")
	}
}

func TestIsAvailableReachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]string{})
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	if !client.IsAvailable() {
		t.Error("expected IsAvailable=true for reachable server")
	}
}

func TestListSubjects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode([]string{"orders-value", "payments-value"})
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	subjects, err := client.ListSubjects(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(subjects) != 2 {
		t.Errorf("expected 2 subjects, got %d", len(subjects))
	}
}

func TestListSubjectsWithAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode([]string{})
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	client.username = "user"
	client.password = "pass"
	_, err := client.ListSubjects(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotAuth == "" {
		t.Error("expected Authorization header to be set")
	}
}

func TestListSubjectsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	_, err := client.ListSubjects(context.Background())
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestGetSubjectVersions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]int{1, 2, 3})
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	versions, err := client.GetSubjectVersions(context.Background(), "orders-value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(versions) != 3 {
		t.Errorf("expected 3 versions, got %d", len(versions))
	}
}

func TestGetSchemaVersionValidation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{})
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)

	// Invalid version
	_, err := client.GetSchema(context.Background(), "test", "abc")
	if err == nil {
		t.Error("expected error for non-numeric version")
	}
}

func TestGetSchemaLatest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/subjects/test-value/versions/latest":
			json.NewEncoder(w).Encode(map[string]any{
				"subject":    "test-value",
				"version":    1,
				"id":         42,
				"schemaType": "AVRO",
				"schema":     `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`,
			})
		case r.URL.Path == "/config/test-value":
			json.NewEncoder(w).Encode(map[string]any{"compatibilityLevel": "BACKWARD"})
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	schema, err := client.GetLatestSchema(context.Background(), "test-value")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schema.Subject != "test-value" {
		t.Errorf("expected subject 'test-value', got %q", schema.Subject)
	}
	if schema.SchemaType != "AVRO" {
		t.Errorf("expected schema type 'AVRO', got %q", schema.SchemaType)
	}
	if schema.CompatibilityLevel != "BACKWARD" {
		t.Errorf("expected compatibility 'BACKWARD', got %q", schema.CompatibilityLevel)
	}
	if schema.FieldSummary == "" {
		t.Error("expected non-empty field summary")
	}
}

func TestGetSchemaDefaultType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/subjects/test/versions/1":
			json.NewEncoder(w).Encode(map[string]any{
				"subject": "test",
				"version": 1,
				"id":      1,
				"schema":  `{"type":"string"}`,
			})
		case r.URL.Path == "/config/test":
			json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	schema, err := client.GetSchema(context.Background(), "test", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if schema.SchemaType != "AVRO" {
		t.Errorf("expected default schema type 'AVRO', got %q", schema.SchemaType)
	}
}

func TestGetSchemaCaching(t *testing.T) {
	callCount := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/subjects/cached/versions/1":
			callCount++
			json.NewEncoder(w).Encode(map[string]any{
				"subject": "cached", "version": 1, "id": 1, "schema": `"string"`,
			})
		case r.URL.Path == "/config/cached":
			json.NewEncoder(w).Encode(map[string]any{})
		}
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	client.GetSchema(context.Background(), "cached", "1")
	client.GetSchema(context.Background(), "cached", "1")

	if callCount != 1 {
		t.Errorf("expected 1 HTTP call (cached), got %d", callCount)
	}
}

func TestCacheStatsMethod(t *testing.T) {
	cache := NewCache(10, time.Minute)
	cache.Set("key1", &SchemaInfo{Subject: "test"})
	cache.Get("key1")
	cache.Get("miss")

	stats := cache.Stats()
	if stats.Size != 1 {
		t.Errorf("expected size 1, got %d", stats.Size)
	}
	if stats.MaxEntries != 10 {
		t.Errorf("expected max entries 10, got %d", stats.MaxEntries)
	}
}

func TestCacheEvictionBySize(t *testing.T) {
	cache := NewCache(2, time.Minute)
	cache.Set("a", &SchemaInfo{Subject: "a"})
	cache.Set("b", &SchemaInfo{Subject: "b"})
	cache.Set("c", &SchemaInfo{Subject: "c"})

	// "a" should be evicted (oldest)
	if _, ok := cache.Get("a"); ok {
		t.Error("expected 'a' to be evicted")
	}
	if _, ok := cache.Get("b"); !ok {
		t.Error("expected 'b' to still be in cache")
	}
	if _, ok := cache.Get("c"); !ok {
		t.Error("expected 'c' to still be in cache")
	}
}

func TestCacheTTL(t *testing.T) {
	cache := NewCache(10, 1*time.Millisecond)
	cache.Set("key", &SchemaInfo{Subject: "test"})
	time.Sleep(5 * time.Millisecond)

	if _, ok := cache.Get("key"); ok {
		t.Error("expected cache entry to expire")
	}
}

func TestFormatAvroTypeString(t *testing.T) {
	result := formatAvroType("string", 0)
	if result != "string" {
		t.Errorf("expected 'string', got %q", result)
	}
}

func TestFormatAvroTypeNullable(t *testing.T) {
	result := formatAvroType([]any{"null", "string"}, 0)
	if result != "string?" {
		t.Errorf("expected 'string?', got %q", result)
	}
}

func TestFormatAvroTypeArray(t *testing.T) {
	result := formatAvroType(map[string]any{"type": "array", "items": "string"}, 0)
	if result != "array<string>" {
		t.Errorf("expected 'array<string>', got %q", result)
	}
}

func TestFormatAvroTypeMap(t *testing.T) {
	result := formatAvroType(map[string]any{"type": "map", "values": "int"}, 0)
	if result != "map<string, int>" {
		t.Errorf("expected 'map<string, int>', got %q", result)
	}
}

func TestFormatAvroTypeLogical(t *testing.T) {
	result := formatAvroType(map[string]any{"type": "long", "logicalType": "timestamp-millis"}, 0)
	if result != "timestamp-millis" {
		t.Errorf("expected 'timestamp-millis', got %q", result)
	}
}

func TestFormatAvroTypeUnion(t *testing.T) {
	result := formatAvroType([]any{"string", "int", "long"}, 0)
	if result != "union<string, int, long>" {
		t.Errorf("expected 'union<string, int, long>', got %q", result)
	}
}

func TestFormatAvroTypeMaxDepth(t *testing.T) {
	result := formatAvroType("string", maxAvroTypeDepth)
	if result != "..." {
		t.Errorf("expected '...' at max depth, got %q", result)
	}
}

func TestFormatAvroTypeComplex(t *testing.T) {
	result := formatAvroType(map[string]any{"notType": "value"}, 0)
	if result != "complex" {
		t.Errorf("expected 'complex', got %q", result)
	}
}

func TestFormatAvroTypeOther(t *testing.T) {
	result := formatAvroType(42, 0)
	if result != "42" {
		t.Errorf("expected '42', got %q", result)
	}
}

func TestGenerateFieldSummaryAvro(t *testing.T) {
	schema := map[string]any{
		"type": "record",
		"name": "User",
		"fields": []any{
			map[string]any{"name": "id", "type": "int"},
			map[string]any{"name": "name", "type": "string"},
			map[string]any{"name": "email", "type": []any{"null", "string"}, "default": nil},
		},
	}
	result := generateFieldSummary(schema, "AVRO")
	if result == "" {
		t.Error("expected non-empty field summary")
	}
	if !contains(result, "User") {
		t.Errorf("expected 'User' in summary, got %q", result)
	}
	if !contains(result, "id: int") {
		t.Errorf("expected 'id: int' in summary, got %q", result)
	}
}

func TestGenerateFieldSummaryNonAvroExtended(t *testing.T) {
	result := generateFieldSummary(nil, "PROTOBUF")
	if result != "(PROTOBUF schema)" {
		t.Errorf("expected '(PROTOBUF schema)', got %q", result)
	}
	result2 := generateFieldSummary(nil, "JSON")
	if result2 != "(JSON schema)" {
		t.Errorf("expected '(JSON schema)', got %q", result2)
	}
}

func TestGenerateFieldSummaryNotMap(t *testing.T) {
	result := generateFieldSummary("string-schema", "AVRO")
	if result != "" {
		t.Errorf("expected empty string for non-map schema, got %q", result)
	}
}

func TestGenerateFieldSummaryNoFields(t *testing.T) {
	schema := map[string]any{"type": "record", "name": "Simple"}
	result := generateFieldSummary(schema, "AVRO")
	if result != "Simple" {
		t.Errorf("expected 'Simple', got %q", result)
	}
}

func TestJsonEscapeString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{`hello`, `"hello"`},
		{`"quoted"`, `"\"quoted\""`},
		{"line\nbreak", `"line\nbreak"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := jsonEscapeString(tt.input)
			if got != tt.want {
				t.Errorf("jsonEscapeString(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestCheckCompatibilityInvalidVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	_, err := client.CheckCompatibility(context.Background(), "test", "abc", `"string"`, "")
	if err == nil {
		t.Error("expected error for non-numeric version")
	}
}

func TestCheckCompatibilityDefaultVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"is_compatible": true})
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	result, err := client.CheckCompatibility(context.Background(), "test", "", `"string"`, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsCompatible {
		t.Error("expected compatible result")
	}
}

func TestCheckCompatibilityNotCompatible(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"is_compatible": false,
			"messages":      []string{"Breaking change detected"},
		})
	}))
	defer srv.Close()

	client, _ := NewClient(srv.URL, "", "", 100, 300)
	result, err := client.CheckCompatibility(context.Background(), "test", "latest", `"int"`, "AVRO")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsCompatible {
		t.Error("expected incompatible result")
	}
	if len(result.Messages) == 0 {
		t.Error("expected error messages")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstring(s, substr))
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
