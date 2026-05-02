package schema

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestListSubjects_Success(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/subjects" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{"topic-value", "topic-key", "orders-value"})
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	subjects, err := c.ListSubjects(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(subjects) != 3 {
		t.Fatalf("expected 3 subjects, got %d", len(subjects))
	}
	if subjects[0] != "topic-value" {
		t.Errorf("expected first subject 'topic-value', got %q", subjects[0])
	}
}

func TestListSubjects_EmptyRegistry(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{})
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	subjects, err := c.ListSubjects(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(subjects) != 0 {
		t.Fatalf("expected 0 subjects, got %d", len(subjects))
	}
}

func TestListSubjects_NetworkError(t *testing.T) {
	t.Parallel()
	c, _ := NewClient("http://127.0.0.1:1", "", "", 100, 300)
	_, err := c.ListSubjects(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid URL, got nil")
	}
}

func TestListSubjects_Unauthorized(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error_code":401,"message":"Unauthorized"}`))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	c.username = "user"
	c.password = "wrong-pass"
	_, err := c.ListSubjects(context.Background())
	if err == nil {
		t.Fatal("expected error for 401, got nil")
	}
}

func TestGetSchema_CacheMiss(t *testing.T) {
	t.Parallel()
	avroSchema := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`
	var reqCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subjects/user-value/versions/1":
			reqCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"subject":    "user-value",
				"version":    1,
				"id":         10,
				"schemaType": "AVRO",
				"schema":     avroSchema,
			})
		case "/config/user-value":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"compatibilityLevel": "BACKWARD"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	info, err := c.GetSchema(context.Background(), "user-value", "1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Subject != "user-value" {
		t.Errorf("expected subject 'user-value', got %q", info.Subject)
	}
	if info.Version != 1 {
		t.Errorf("expected version 1, got %d", info.Version)
	}
	if info.SchemaType != "AVRO" {
		t.Errorf("expected schema type AVRO, got %q", info.SchemaType)
	}
	if info.CompatibilityLevel != "BACKWARD" {
		t.Errorf("expected compatibility BACKWARD, got %q", info.CompatibilityLevel)
	}
}

func TestGetSchema_NotFound(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_code":40401,"message":"Subject not found"}`))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	_, err := c.GetSchema(context.Background(), "nonexistent", "1")
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
}

func TestGetSchema_CacheHit(t *testing.T) {
	t.Parallel()
	avroSchema := `{"type":"record","name":"Order","fields":[{"name":"id","type":"int"}]}`
	var schemaReqCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subjects/order-value/versions/latest":
			schemaReqCount.Add(1)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"subject":    "order-value",
				"version":    3,
				"id":         42,
				"schemaType": "AVRO",
				"schema":     avroSchema,
			})
		case "/config/order-value":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"compatibilityLevel": "FULL"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	ctx := context.Background()

	// First call — cache miss
	info1, err := c.GetSchema(ctx, "order-value", "latest")
	if err != nil {
		t.Fatalf("first call error: %v", err)
	}

	// Second call — should hit cache
	info2, err := c.GetSchema(ctx, "order-value", "latest")
	if err != nil {
		t.Fatalf("second call error: %v", err)
	}

	if info1.Subject != info2.Subject || info1.Version != info2.Version {
		t.Error("cached result differs from original")
	}

	if count := schemaReqCount.Load(); count != 1 {
		t.Errorf("expected 1 HTTP request for schema, got %d (cache not working)", count)
	}
}

func TestCheckCompatibility_Compatible(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"is_compatible": true})
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	result, err := c.CheckCompatibility(context.Background(), "user-value", "latest",
		`{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`, "AVRO")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsCompatible {
		t.Error("expected compatible, got incompatible")
	}
}

func TestCheckCompatibility_Incompatible(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"is_compatible": false,
			"messages":      []string{"field removed without default"},
		})
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	result, err := c.CheckCompatibility(context.Background(), "user-value", "latest",
		`{"type":"record","name":"User","fields":[]}`, "AVRO")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IsCompatible {
		t.Error("expected incompatible, got compatible")
	}
	if len(result.Messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(result.Messages))
	}
}

func TestCheckCompatibility_MalformedJSON(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{not valid json`))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	_, err := c.CheckCompatibility(context.Background(), "user-value", "latest",
		`{"type":"record","name":"User","fields":[]}`, "AVRO")
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestHTTPErrors_400(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error_code":400,"message":"Bad Request"}`))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	_, err := c.ListSubjects(context.Background())
	if err == nil {
		t.Fatal("expected error for HTTP 400")
	}
}

func TestHTTPErrors_404(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_code":404,"message":"Not Found"}`))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	_, err := c.ListSubjects(context.Background())
	if err == nil {
		t.Fatal("expected error for HTTP 404")
	}
}

func TestHTTPErrors_500(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error_code":500,"message":"Internal Server Error"}`))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	_, err := c.ListSubjects(context.Background())
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}

func TestDoRequest_BasicAuth(t *testing.T) {
	t.Parallel()
	var gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{})
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	c.username = "myuser"
	c.password = "mypass"
	_, _ = c.ListSubjects(context.Background())

	if gotUser != "myuser" || gotPass != "mypass" {
		t.Errorf("expected basic auth myuser/mypass, got %s/%s", gotUser, gotPass)
	}
}

func TestDoRequest_AcceptHeader(t *testing.T) {
	t.Parallel()
	var gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAccept = r.Header.Get("Accept")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{})
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	_, _ = c.ListSubjects(context.Background())

	expected := "application/vnd.schemaregistry.v1+json"
	if gotAccept != expected {
		t.Errorf("expected Accept header %q, got %q", expected, gotAccept)
	}
}

func TestGetSchema_InvalidVersion(t *testing.T) {
	t.Parallel()
	c, _ := NewClient("http://localhost:1", "", "", 100, 300)
	_, err := c.GetSchema(context.Background(), "test", "abc")
	if err == nil {
		t.Fatal("expected error for invalid version")
	}
}

func TestCheckCompatibility_InvalidVersion(t *testing.T) {
	t.Parallel()
	c, _ := NewClient("http://localhost:1", "", "", 100, 300)
	_, err := c.CheckCompatibility(context.Background(), "test", "abc", `{}`, "AVRO")
	if err == nil {
		t.Fatal("expected error for invalid version")
	}
}

func TestCheckCompatibility_DefaultVersionAndType(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the path uses "latest" when version is empty
		expectedPath := "/compatibility/subjects/user-value/versions/latest"
		if r.URL.Path != expectedPath {
			t.Errorf("expected path %q, got %q", expectedPath, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"is_compatible": true})
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	result, err := c.CheckCompatibility(context.Background(), "user-value", "", `{"type":"record"}`, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.IsCompatible {
		t.Error("expected compatible result")
	}
}

func TestCheckCompatibility_HTTPError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error_code":500}`))
	}))
	defer srv.Close()

	c, _ := NewClient(srv.URL, "", "", 100, 300)
	_, err := c.CheckCompatibility(context.Background(), "test", "latest", `{}`, "AVRO")
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
}
