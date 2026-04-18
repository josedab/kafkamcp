package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ===================== RestartConnector: specific task =====================

func TestRestartConnector_SpecificTask(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if !strings.Contains(r.URL.Path, "/tasks/2/restart") {
			t.Errorf("expected task 2 restart path, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	result, err := client.RestartConnector(context.Background(), "my-conn", 2, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "task_restarted" {
		t.Errorf("expected 'task_restarted', got %q", result.Status)
	}
	if result.TasksRestarted != 1 {
		t.Errorf("expected 1, got %d", result.TasksRestarted)
	}
}

// ===================== RestartConnector: failed-only =====================

func TestRestartConnector_FailedOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/connectors/my-conn/config":
			json.NewEncoder(w).Encode(map[string]string{"connector.class": "Test"})
		case r.URL.Path == "/connectors/my-conn/status":
			json.NewEncoder(w).Encode(map[string]any{
				"name": "my-conn",
				"type": "source",
				"connector": map[string]any{
					"state":     "RUNNING",
					"worker_id": "w1:8083",
				},
				"tasks": []map[string]any{
					{"id": 0, "state": "RUNNING", "worker_id": "w1:8083"},
					{"id": 1, "state": "FAILED", "worker_id": "w1:8083", "trace": "NPE"},
					{"id": 2, "state": "FAILED", "worker_id": "w1:8083", "trace": "OOM"},
				},
			})
		case strings.Contains(r.URL.Path, "/tasks/") && strings.Contains(r.URL.Path, "/restart"):
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	result, err := client.RestartConnector(context.Background(), "my-conn", -1, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "failed_tasks_restarted" {
		t.Errorf("expected 'failed_tasks_restarted', got %q", result.Status)
	}
	if result.TasksRestarted != 2 {
		t.Errorf("expected 2 tasks restarted, got %d", result.TasksRestarted)
	}
}

// ===================== RestartConnector: no failed tasks =====================

func TestRestartConnector_NoFailedTasks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/connectors/my-conn/config":
			json.NewEncoder(w).Encode(map[string]string{"connector.class": "Test"})
		case r.URL.Path == "/connectors/my-conn/status":
			json.NewEncoder(w).Encode(map[string]any{
				"name": "my-conn",
				"type": "source",
				"connector": map[string]any{
					"state":     "RUNNING",
					"worker_id": "w1:8083",
				},
				"tasks": []map[string]any{
					{"id": 0, "state": "RUNNING", "worker_id": "w1:8083"},
				},
			})
		}
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	result, err := client.RestartConnector(context.Background(), "my-conn", -1, true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TasksRestarted != 0 {
		t.Errorf("expected 0 tasks restarted, got %d", result.TasksRestarted)
	}
}

// ===================== RestartConnector: full restart =====================

func TestRestartConnector_FullRestart(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/restart") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	result, err := client.RestartConnector(context.Background(), "my-conn", -1, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "connector_restarted" {
		t.Errorf("expected 'connector_restarted', got %q", result.Status)
	}
	if result.TasksRestarted != -1 {
		t.Errorf("expected -1, got %d", result.TasksRestarted)
	}
}

// ===================== httpPostRaw: HTTP 400+ errors =====================

func TestHTTPPostRaw_HTTP400Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid request"}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	_, err := client.httpPostRaw(context.Background(), server.URL+"/test", []byte(`{}`))
	if err == nil {
		t.Fatal("expected error for HTTP 400")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("expected HTTP 400 in error, got: %v", err)
	}
}

func TestHTTPPostRaw_HTTP500Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	_, err := client.httpPostRaw(context.Background(), server.URL+"/test", nil)
	if err == nil {
		t.Fatal("expected error for HTTP 500")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected HTTP 500 in error, got: %v", err)
	}
}

// ===================== httpPostRaw: context deadline =====================

func TestHTTPPostRaw_ContextDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.httpPostRaw(ctx, server.URL+"/test", nil)
	if err == nil {
		t.Fatal("expected error for context deadline")
	}
}

// ===================== httpPostRaw: success =====================

func TestHTTPPostRaw_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("expected Content-Type application/json")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	body, err := client.httpPostRaw(context.Background(), server.URL+"/test", []byte(`{"data":"test"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(body) != `{"ok":true}` {
		t.Errorf("unexpected body: %s", string(body))
	}
}

// ===================== httpPostRaw: nil body =====================

func TestHTTPPostRaw_NilBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	_, err := client.httpPostRaw(context.Background(), server.URL+"/test", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ===================== httpPut =====================

func TestHTTPPut_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"updated":true}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	body, err := client.httpPut(context.Background(), server.URL+"/test", map[string]string{"key": "val"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(string(body), "updated") {
		t.Errorf("unexpected body: %s", string(body))
	}
}

func TestHTTPPut_Non2xx(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte("conflict"))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	_, err := client.httpPut(context.Background(), server.URL+"/test", map[string]string{"k": "v"})
	if err == nil {
		t.Fatal("expected error for HTTP 409")
	}
	if !strings.Contains(err.Error(), "409") {
		t.Errorf("expected HTTP 409 in error, got: %v", err)
	}
}

// ===================== httpPost =====================

func TestHTTPPost_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"created":true}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	body, err := client.httpPost(context.Background(), server.URL+"/test", map[string]string{"k": "v"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(string(body), "created") {
		t.Errorf("unexpected body: %s", string(body))
	}
}

func TestHTTPPost_MarshalError(t *testing.T) {
	client, _ := NewConnectClient("http://localhost:8083")
	_, err := client.httpPost(context.Background(), "http://localhost:8083/test", make(chan int))
	if err == nil {
		t.Fatal("expected marshal error")
	}
}
