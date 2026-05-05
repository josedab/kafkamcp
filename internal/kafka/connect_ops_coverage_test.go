package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --- PauseConnector ---

func TestPauseConnector_Success(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/pause") {
			t.Errorf("expected /pause path, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("null"))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	err := client.PauseConnector(context.Background(), "my-conn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPauseConnector_NotFound(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_code":404,"message":"Connector not found"}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	err := client.PauseConnector(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected 404 in error, got: %v", err)
	}
}

func TestPauseConnector_ClusterUnreachable(t *testing.T) {
	t.Parallel()
	client, _ := NewConnectClient("http://localhost:1") // unreachable
	err := client.PauseConnector(context.Background(), "my-conn")
	if err == nil {
		t.Fatal("expected error for unreachable cluster")
	}
}

func TestPauseConnector_NilClient(t *testing.T) {
	t.Parallel()
	var client *ConnectClient
	err := client.PauseConnector(context.Background(), "my-conn")
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

// --- ResumeConnector ---

func TestResumeConnector_Success(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/resume") {
			t.Errorf("expected /resume path, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("null"))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	err := client.ResumeConnector(context.Background(), "my-conn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResumeConnector_NotFound(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_code":404,"message":"Connector not found"}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	err := client.ResumeConnector(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for 404")
	}
}

func TestResumeConnector_NilClient(t *testing.T) {
	t.Parallel()
	var client *ConnectClient
	err := client.ResumeConnector(context.Background(), "my-conn")
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

// --- GetConnectorStatus (via DescribeConnector) ---

func TestDescribeConnector_MixedTaskStates(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/config"):
			json.NewEncoder(w).Encode(map[string]string{"connector.class": "TestConn"})
		case strings.HasSuffix(r.URL.Path, "/status"):
			json.NewEncoder(w).Encode(map[string]any{
				"name": "mixed-conn",
				"type": "source",
				"connector": map[string]any{
					"state":     "RUNNING",
					"worker_id": "w1:8083",
				},
				"tasks": []map[string]any{
					{"id": 0, "state": "RUNNING", "worker_id": "w1:8083"},
					{"id": 1, "state": "FAILED", "worker_id": "w1:8083", "trace": "NPE"},
					{"id": 2, "state": "PAUSED", "worker_id": "w1:8083"},
				},
			})
		}
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	detail, err := client.DescribeConnector(context.Background(), "mixed-conn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(detail.Tasks) != 3 {
		t.Errorf("expected 3 tasks, got %d", len(detail.Tasks))
	}

	states := map[string]int{}
	for _, task := range detail.Tasks {
		states[task.State]++
	}
	if states["RUNNING"] != 1 || states["FAILED"] != 1 || states["PAUSED"] != 1 {
		t.Errorf("unexpected task states: %v", states)
	}
}

func TestDescribeConnector_RebalancingState(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/config"):
			json.NewEncoder(w).Encode(map[string]string{"connector.class": "TestConn"})
		case strings.HasSuffix(r.URL.Path, "/status"):
			json.NewEncoder(w).Encode(map[string]any{
				"name": "rebal-conn",
				"type": "sink",
				"connector": map[string]any{
					"state":     "REBALANCING",
					"worker_id": "w1:8083",
				},
				"tasks": []map[string]any{},
			})
		}
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	detail, err := client.DescribeConnector(context.Background(), "rebal-conn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if detail.State != "REBALANCING" {
		t.Errorf("expected REBALANCING, got %s", detail.State)
	}
}

// --- RestartConnectorTask improvements ---

func TestRestartConnectorTask_SpecificTaskIndex(t *testing.T) {
	t.Parallel()
	var requestedPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	result, err := client.RestartConnector(context.Background(), "my-conn", 5, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TasksRestarted != 1 {
		t.Errorf("expected 1, got %d", result.TasksRestarted)
	}
	if !strings.Contains(requestedPath, "/tasks/5/restart") {
		t.Errorf("expected task 5 in path, got %s", requestedPath)
	}
}

func TestRestartConnectorTask_InvalidTaskID_ServerError(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_code":404,"message":"Task not found"}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	_, err := client.RestartConnector(context.Background(), "my-conn", 999, false)
	if err == nil {
		t.Fatal("expected error for invalid task ID")
	}
}

func TestRestartConnector_409Conflict(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"error_code":409,"message":"Rebalance in progress"}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	_, err := client.RestartConnector(context.Background(), "my-conn", -1, false)
	if err == nil {
		t.Fatal("expected error for 409 conflict")
	}
	if !strings.Contains(err.Error(), "409") {
		t.Errorf("expected 409 in error, got: %v", err)
	}
}

// --- ConnectorHealth ---

func TestConnectorHealth_AllHealthy(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/config"):
			json.NewEncoder(w).Encode(map[string]string{"connector.class": "TestConn"})
		case strings.HasSuffix(r.URL.Path, "/status"):
			json.NewEncoder(w).Encode(map[string]any{
				"name": "healthy-conn",
				"type": "source",
				"connector": map[string]any{"state": "RUNNING", "worker_id": "w1:8083"},
				"tasks": []map[string]any{
					{"id": 0, "state": "RUNNING", "worker_id": "w1:8083"},
					{"id": 1, "state": "RUNNING", "worker_id": "w1:8083"},
				},
			})
		}
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	health, err := client.ConnectorHealth(context.Background(), "healthy-conn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health.HealthScore < 0.9 {
		t.Errorf("expected high health score, got %f", health.HealthScore)
	}
	if health.FailedTasks != 0 {
		t.Errorf("expected 0 failed tasks, got %d", health.FailedTasks)
	}
}
