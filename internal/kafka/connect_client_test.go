package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// ---------- NewConnectClient ----------

func TestNewConnectClient_EmptyURL(t *testing.T) {
	client, err := NewConnectClient("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != nil {
		t.Error("expected nil client for empty URL")
	}
}

func TestNewConnectClient_ValidURL(t *testing.T) {
	client, err := NewConnectClient("http://localhost:8083")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.baseURL != "http://localhost:8083" {
		t.Errorf("baseURL = %q, want %q", client.baseURL, "http://localhost:8083")
	}
}

func TestNewConnectClient_TrailingSlash(t *testing.T) {
	client, err := NewConnectClient("http://localhost:8083/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client.baseURL != "http://localhost:8083" {
		t.Errorf("baseURL = %q, want trailing slash trimmed", client.baseURL)
	}
}

func TestNewConnectClient_WithTLSOptions(t *testing.T) {
	// WithConnectCA with empty file should be a no-op
	client, err := NewConnectClient("https://localhost:8083", WithConnectCA(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestNewConnectClient_WithBadCA(t *testing.T) {
	_, err := NewConnectClient("https://localhost:8083", WithConnectCA("/nonexistent/ca.pem"))
	if err == nil {
		t.Error("expected error for nonexistent CA file")
	}
}

func TestNewConnectClient_InsecureSkipVerifyFalse(t *testing.T) {
	client, err := NewConnectClient("https://localhost:8083", WithConnectInsecureSkipVerify(false))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

// ---------- ListConnectors ----------

func TestListConnectors_NilClient(t *testing.T) {
	var c *ConnectClient
	_, err := c.ListConnectors(context.Background())
	if err == nil {
		t.Error("expected error for nil client")
	}
}

func TestListConnectors_ExpandedFormat(t *testing.T) {
	expanded := map[string]any{
		"my-source": map[string]any{
			"status": map[string]any{
				"name": "my-source",
				"type": "source",
				"connector": map[string]any{
					"state":     "RUNNING",
					"worker_id": "worker1:8083",
				},
			},
		},
		"my-sink": map[string]any{
			"status": map[string]any{
				"name": "my-sink",
				"type": "sink",
				"connector": map[string]any{
					"state":     "PAUSED",
					"worker_id": "worker2:8083",
				},
			},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(expanded)
	}))
	defer srv.Close()

	client := &ConnectClient{
		baseURL:    srv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	connectors, err := client.ListConnectors(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(connectors) != 2 {
		t.Fatalf("got %d connectors, want 2", len(connectors))
	}
	// Results sorted by name
	if connectors[0].Name != "my-sink" {
		t.Errorf("first connector = %q, want %q", connectors[0].Name, "my-sink")
	}
	if connectors[1].State != "RUNNING" {
		t.Errorf("second connector state = %q, want RUNNING", connectors[1].State)
	}
}

func TestListConnectors_SimpleFormat(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{"connector-a", "connector-b"})
	}))
	defer srv.Close()

	client := &ConnectClient{
		baseURL:    srv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	connectors, err := client.ListConnectors(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(connectors) != 2 {
		t.Fatalf("got %d connectors, want 2", len(connectors))
	}
	if connectors[0].Name != "connector-a" {
		t.Errorf("first connector = %q, want %q", connectors[0].Name, "connector-a")
	}
}

func TestListConnectors_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := &ConnectClient{
		baseURL:    srv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	_, err := client.ListConnectors(context.Background())
	if err == nil {
		t.Error("expected error for HTTP 500")
	}
}

func TestListConnectors_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer srv.Close()

	client := &ConnectClient{
		baseURL:    srv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	_, err := client.ListConnectors(context.Background())
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// ---------- DescribeConnector ----------

func TestDescribeConnector_NilClient(t *testing.T) {
	var c *ConnectClient
	_, err := c.DescribeConnector(context.Background(), "test")
	if err == nil {
		t.Error("expected error for nil client")
	}
}

func TestDescribeConnector_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/connectors/my-connector/config":
			json.NewEncoder(w).Encode(map[string]string{
				"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
				"database.password": "secret",
				"database.hostname": "db.example.com",
			})
		case r.URL.Path == "/connectors/my-connector/status":
			json.NewEncoder(w).Encode(map[string]any{
				"name": "my-connector",
				"type": "source",
				"connector": map[string]any{
					"state":     "RUNNING",
					"worker_id": "worker1:8083",
				},
				"tasks": []map[string]any{
					{"id": 0, "state": "RUNNING", "worker_id": "worker1:8083"},
					{"id": 1, "state": "FAILED", "worker_id": "worker2:8083", "trace": "java.lang.Exception: connection refused"},
				},
			})
		}
	}))
	defer srv.Close()

	client := &ConnectClient{
		baseURL:    srv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	detail, err := client.DescribeConnector(context.Background(), "my-connector")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if detail.Name != "my-connector" {
		t.Errorf("name = %q, want %q", detail.Name, "my-connector")
	}
	if detail.State != "RUNNING" {
		t.Errorf("state = %q, want RUNNING", detail.State)
	}
	if detail.Type != "source" {
		t.Errorf("type = %q, want source", detail.Type)
	}
	if len(detail.Tasks) != 2 {
		t.Fatalf("got %d tasks, want 2", len(detail.Tasks))
	}
	if detail.Tasks[1].State != "FAILED" {
		t.Errorf("task 1 state = %q, want FAILED", detail.Tasks[1].State)
	}
	// Verify sensitive config is redacted
	if detail.Config["database.password"] != "**REDACTED**" {
		t.Errorf("password should be redacted, got %q", detail.Config["database.password"])
	}
	if detail.Config["database.hostname"] != "db.example.com" {
		t.Errorf("hostname should not be redacted, got %q", detail.Config["database.hostname"])
	}
}

func TestDescribeConnector_ConfigError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	client := &ConnectClient{
		baseURL:    srv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	_, err := client.DescribeConnector(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for 404")
	}
}

// ---------- httpGet ----------

func TestHttpGet_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
	}))
	defer srv.Close()

	client := &ConnectClient{
		baseURL:    srv.URL,
		httpClient: &http.Client{Timeout: 5 * time.Second},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := client.httpGet(ctx, srv.URL+"/test")
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}
