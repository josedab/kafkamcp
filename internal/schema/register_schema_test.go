package schema

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRegisterSchema_Success(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/vnd.schemaregistry.v1+json" {
			t.Errorf("unexpected Content-Type: %s", r.Header.Get("Content-Type"))
		}

		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["schemaType"] != "AVRO" {
			t.Errorf("expected schemaType AVRO, got %v", body["schemaType"])
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"id": 42})
	}))
	defer ts.Close()

	client, err := NewClient(ts.URL, "", "", 10, 60)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	id, err := client.RegisterSchema(context.Background(), "test-subject", `{"type":"record","name":"Test","fields":[]}`, "AVRO")
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if id != 42 {
		t.Errorf("expected schema ID 42, got %d", id)
	}
}

func TestRegisterSchema_EmptyTypeDefaultsAVRO(t *testing.T) {
	t.Parallel()
	var receivedType string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		receivedType = body["schemaType"].(string)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"id": 1})
	}))
	defer ts.Close()

	client, _ := NewClient(ts.URL, "", "", 10, 60)
	_, _ = client.RegisterSchema(context.Background(), "test", `{}`, "")

	if receivedType != "AVRO" {
		t.Errorf("expected AVRO default, got %q", receivedType)
	}
}

func TestRegisterSchema_ProtobufType(t *testing.T) {
	t.Parallel()
	var receivedType string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		receivedType = body["schemaType"].(string)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"id": 2})
	}))
	defer ts.Close()

	client, _ := NewClient(ts.URL, "", "", 10, 60)
	_, _ = client.RegisterSchema(context.Background(), "test", `syntax = "proto3";`, "PROTOBUF")

	if receivedType != "PROTOBUF" {
		t.Errorf("expected PROTOBUF, got %q", receivedType)
	}
}

func TestRegisterSchema_JSONType(t *testing.T) {
	t.Parallel()
	var receivedType string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		receivedType = body["schemaType"].(string)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"id": 3})
	}))
	defer ts.Close()

	client, _ := NewClient(ts.URL, "", "", 10, 60)
	_, _ = client.RegisterSchema(context.Background(), "test", `{"type":"object"}`, "JSON")

	if receivedType != "JSON" {
		t.Errorf("expected JSON, got %q", receivedType)
	}
}

func TestRegisterSchema_BasicAuth(t *testing.T) {
	t.Parallel()
	var gotUser, gotPass string
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"id": 5})
	}))
	defer ts.Close()

	client := &Client{
		baseURL:    ts.URL,
		username:   "myuser",
		password:   "mypass",
		httpClient: ts.Client(),
		cache:      NewCache(10, 60),
	}

	_, _ = client.RegisterSchema(context.Background(), "test", `{}`, "AVRO")

	if gotUser != "myuser" || gotPass != "mypass" {
		t.Errorf("expected myuser:mypass, got %s:%s", gotUser, gotPass)
	}
}

func TestRegisterSchema_HTTP409Conflict(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error_code":409,"message":"schema already exists"}`))
	}))
	defer ts.Close()

	client, _ := NewClient(ts.URL, "", "", 10, 60)
	_, err := client.RegisterSchema(context.Background(), "test", `{}`, "AVRO")
	if err == nil {
		t.Fatal("expected error for 409")
	}
}

func TestRegisterSchema_HTTP422InvalidSchema(t *testing.T) {
	t.Parallel()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(422)
		_, _ = w.Write([]byte(`{"error_code":42201,"message":"invalid schema"}`))
	}))
	defer ts.Close()

	client, _ := NewClient(ts.URL, "", "", 10, 60)
	_, err := client.RegisterSchema(context.Background(), "test", `invalid`, "AVRO")
	if err == nil {
		t.Fatal("expected error for 422")
	}
}

func TestRegisterSchema_JSONEscapedBody(t *testing.T) {
	t.Parallel()
	var receivedSchema string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		receivedSchema = body["schema"].(string)
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"id": 10})
	}))
	defer ts.Close()

	client, _ := NewClient(ts.URL, "", "", 10, 60)
	schemaStr := `{"type":"record","name":"Test","fields":[]}`
	_, _ = client.RegisterSchema(context.Background(), "test", schemaStr, "AVRO")

	if receivedSchema != schemaStr {
		t.Errorf("expected schema to be properly escaped, got %q", receivedSchema)
	}
}

func TestRegisterSchema_ContentTypeHeader(t *testing.T) {
	t.Parallel()
	var gotContentType string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]int{"id": 1})
	}))
	defer ts.Close()

	client, _ := NewClient(ts.URL, "", "", 10, 60)
	_, _ = client.RegisterSchema(context.Background(), "test", `{}`, "AVRO")

	if gotContentType != "application/vnd.schemaregistry.v1+json" {
		t.Errorf("expected schema registry content type, got %q", gotContentType)
	}
}
