package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRedactSensitiveConfigConnect(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		config map[string]string
		key    string
		want   string
	}{
		{"password key", map[string]string{"db.password": "secret123"}, "db.password", "**REDACTED**"},
		{"token key", map[string]string{"auth.token": "tok_abc"}, "auth.token", "**REDACTED**"},
		{"api.key", map[string]string{"api.key": "key123"}, "api.key", "**REDACTED**"},
		{"ssl.key", map[string]string{"ssl.key.location": "/path"}, "ssl.key.location", "**REDACTED**"},
		{"keystore", map[string]string{"ssl.keystore.password": "pass"}, "ssl.keystore.password", "**REDACTED**"},
		{"truststore", map[string]string{"ssl.truststore.location": "/path"}, "ssl.truststore.location", "**REDACTED**"},
		{"safe key", map[string]string{"retention.ms": "86400"}, "retention.ms", "86400"},
		{"empty secret", map[string]string{"db.password": ""}, "db.password", ""},
		{"case insensitive", map[string]string{"SSL.KeyStore.Password": "pass"}, "SSL.KeyStore.Password", "**REDACTED**"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redactSensitiveConfig(tt.config)
			if result[tt.key] != tt.want {
				t.Errorf("redactSensitiveConfig[%q] = %q, want %q", tt.key, result[tt.key], tt.want)
			}
		})
	}
}

func TestRedactSensitiveConfigPreservesAll(t *testing.T) {
	t.Parallel()
	cfg := map[string]string{
		"connector.class":       "FileStreamSource",
		"topic":                 "test-topic",
		"database.password":     "secret",
		"aws.secret.access.key": "AKIAIOSFODNN7EXAMPLE",
	}
	result := redactSensitiveConfig(cfg)
	if len(result) != len(cfg) {
		t.Errorf("expected %d keys, got %d", len(cfg), len(result))
	}
	if result["connector.class"] != "FileStreamSource" {
		t.Errorf("non-sensitive key was modified")
	}
	if result["topic"] != "test-topic" {
		t.Errorf("non-sensitive key was modified")
	}
}

func TestNewConnectClientEmptyURL(t *testing.T) {
	t.Parallel()
	client, err := NewConnectClient("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != nil {
		t.Error("expected nil client for empty URL")
	}
}

func TestNewConnectClientValidURL(t *testing.T) {
	t.Parallel()
	client, err := NewConnectClient("http://localhost:8083")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	if client.baseURL != "http://localhost:8083" {
		t.Errorf("unexpected baseURL: %s", client.baseURL)
	}
}

func TestNewConnectClientTrimsSlash(t *testing.T) {
	t.Parallel()
	client, err := NewConnectClient("http://localhost:8083/")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client.baseURL != "http://localhost:8083" {
		t.Errorf("expected trailing slash trimmed, got %s", client.baseURL)
	}
}

func TestConnectClientNilMethods(t *testing.T) {
	t.Parallel()
	var c *ConnectClient

	_, err := c.ListConnectors(context.Background())
	if err == nil {
		t.Error("expected error from nil client ListConnectors")
	}

	_, err = c.DescribeConnector(context.Background(), "test")
	if err == nil {
		t.Error("expected error from nil client DescribeConnector")
	}

	_, err = c.CreateConnector(context.Background(), "test", map[string]string{})
	if err == nil {
		t.Error("expected error from nil client CreateConnector")
	}

	_, err = c.UpdateConnector(context.Background(), "test", map[string]string{})
	if err == nil {
		t.Error("expected error from nil client UpdateConnector")
	}

	err = c.DeleteConnector(context.Background(), "test")
	if err == nil {
		t.Error("expected error from nil client DeleteConnector")
	}

	_, err = c.RestartConnector(context.Background(), "test", -1, false)
	if err == nil {
		t.Error("expected error from nil client RestartConnector")
	}

	_, err = c.ConnectorHealth(context.Background(), "test")
	if err == nil {
		t.Error("expected error from nil client ConnectorHealth")
	}
}

func TestConnectClientListConnectorsExpanded(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/connectors" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"my-connector": map[string]any{
					"status": map[string]any{
						"name": "my-connector",
						"type": "source",
						"connector": map[string]any{
							"state":     "RUNNING",
							"worker_id": "worker-1:8083",
						},
					},
				},
			})
		}
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	connectors, err := client.ListConnectors(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(connectors) != 1 {
		t.Fatalf("expected 1 connector, got %d", len(connectors))
	}
	if connectors[0].Name != "my-connector" {
		t.Errorf("expected name 'my-connector', got %q", connectors[0].Name)
	}
	if connectors[0].State != "RUNNING" {
		t.Errorf("expected state 'RUNNING', got %q", connectors[0].State)
	}
}

func TestConnectClientListConnectorsSimple(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]string{"connector-a", "connector-b"})
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	connectors, err := client.ListConnectors(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(connectors) != 2 {
		t.Fatalf("expected 2 connectors, got %d", len(connectors))
	}
	if connectors[0].Name != "connector-a" {
		t.Errorf("expected sorted first connector 'connector-a', got %q", connectors[0].Name)
	}
}

func TestConnectClientListConnectorsHTTPError(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	_, err := client.ListConnectors(context.Background())
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestConnectClientDescribeConnector(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/connectors/test-conn/config":
			json.NewEncoder(w).Encode(map[string]string{
				"connector.class": "FileStreamSource",
				"topic":           "test",
				"db.password":     "secret",
			})
		case r.URL.Path == "/connectors/test-conn/status":
			json.NewEncoder(w).Encode(map[string]any{
				"name": "test-conn",
				"type": "source",
				"connector": map[string]any{
					"state":     "RUNNING",
					"worker_id": "worker-1:8083",
				},
				"tasks": []map[string]any{
					{"id": 0, "state": "RUNNING", "worker_id": "worker-1:8083"},
				},
			})
		}
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	detail, err := client.DescribeConnector(context.Background(), "test-conn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if detail.Name != "test-conn" {
		t.Errorf("expected name 'test-conn', got %q", detail.Name)
	}
	if detail.State != "RUNNING" {
		t.Errorf("expected state 'RUNNING', got %q", detail.State)
	}
	if detail.Config["db.password"] != "**REDACTED**" {
		t.Errorf("expected password redacted, got %q", detail.Config["db.password"])
	}
	if detail.Config["connector.class"] != "FileStreamSource" {
		t.Errorf("expected connector.class preserved, got %q", detail.Config["connector.class"])
	}
	if len(detail.Tasks) != 1 {
		t.Errorf("expected 1 task, got %d", len(detail.Tasks))
	}
}

func TestConnectClientCreateConnector(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"name":   "new-conn",
			"config": map[string]string{"connector.class": "FileSink"},
		})
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	result, err := client.CreateConnector(context.Background(), "new-conn", map[string]string{
		"connector.class": "FileSink",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Name != "new-conn" {
		t.Errorf("expected name 'new-conn', got %q", result.Name)
	}
	if result.Status != "created" {
		t.Errorf("expected status 'created', got %q", result.Status)
	}
}

func TestConnectClientUpdateConnector(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"connector.class": "FileSink",
			"topic":           "updated-topic",
		})
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	result, err := client.UpdateConnector(context.Background(), "my-conn", map[string]string{
		"topic": "updated-topic",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "updated" {
		t.Errorf("expected status 'updated', got %q", result.Status)
	}
}

func TestConnectClientDeleteConnector(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	err := client.DeleteConnector(context.Background(), "dead-conn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConnectClientDeleteConnectorError(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_code":404,"message":"Connector not found"}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	err := client.DeleteConnector(context.Background(), "nonexistent")
	if err == nil {
		t.Error("expected error for 404 response")
	}
}

func TestConnectClientRestartConnectorFull(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	result, err := client.RestartConnector(context.Background(), "my-conn", -1, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "connector_restarted" {
		t.Errorf("expected status 'connector_restarted', got %q", result.Status)
	}
}

func TestConnectClientRestartSpecificTask(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	result, err := client.RestartConnector(context.Background(), "my-conn", 0, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "task_restarted" {
		t.Errorf("expected status 'task_restarted', got %q", result.Status)
	}
	if result.TasksRestarted != 1 {
		t.Errorf("expected 1 task restarted, got %d", result.TasksRestarted)
	}
}

func TestConnectorHealthAllRunning(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/connectors/test/config":
			json.NewEncoder(w).Encode(map[string]string{"connector.class": "Test"})
		case r.URL.Path == "/connectors/test/status":
			json.NewEncoder(w).Encode(map[string]any{
				"name": "test",
				"type": "source",
				"connector": map[string]any{
					"state":     "RUNNING",
					"worker_id": "w1:8083",
				},
				"tasks": []map[string]any{
					{"id": 0, "state": "RUNNING", "worker_id": "w1:8083"},
					{"id": 1, "state": "RUNNING", "worker_id": "w1:8083"},
				},
			})
		}
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	health, err := client.ConnectorHealth(context.Background(), "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health.HealthScore != 1.0 {
		t.Errorf("expected health score 1.0, got %f", health.HealthScore)
	}
	if health.RunningTasks != 2 {
		t.Errorf("expected 2 running tasks, got %d", health.RunningTasks)
	}
	if health.FailedTasks != 0 {
		t.Errorf("expected 0 failed tasks, got %d", health.FailedTasks)
	}
}

func TestConnectorHealthWithFailedTasks(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/connectors/test/config":
			json.NewEncoder(w).Encode(map[string]string{"connector.class": "Test"})
		case r.URL.Path == "/connectors/test/status":
			json.NewEncoder(w).Encode(map[string]any{
				"name": "test",
				"type": "source",
				"connector": map[string]any{
					"state":     "RUNNING",
					"worker_id": "w1:8083",
				},
				"tasks": []map[string]any{
					{"id": 0, "state": "RUNNING", "worker_id": "w1:8083"},
					{"id": 1, "state": "FAILED", "worker_id": "w1:8083", "trace": "NPE"},
				},
			})
		}
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	health, err := client.ConnectorHealth(context.Background(), "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health.HealthScore >= 1.0 {
		t.Errorf("expected health score < 1.0, got %f", health.HealthScore)
	}
	if health.FailedTasks != 1 {
		t.Errorf("expected 1 failed task, got %d", health.FailedTasks)
	}
	if len(health.Recommendations) == 0 {
		t.Error("expected recommendations for failed tasks")
	}
}

func TestConnectorHealthPaused(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/connectors/test/config":
			json.NewEncoder(w).Encode(map[string]string{"connector.class": "Test"})
		case r.URL.Path == "/connectors/test/status":
			json.NewEncoder(w).Encode(map[string]any{
				"name": "test",
				"type": "source",
				"connector": map[string]any{
					"state":     "PAUSED",
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
	health, err := client.ConnectorHealth(context.Background(), "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health.HealthScore >= 1.0 {
		t.Errorf("expected health score < 1.0 for paused connector, got %f", health.HealthScore)
	}
	found := false
	for _, rec := range health.Recommendations {
		if rec == "Connector is paused. Resume if it should be running." {
			found = true
		}
	}
	if !found {
		t.Error("expected paused recommendation")
	}
}

func TestConnectorHealthNoTasks(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/connectors/test/config":
			json.NewEncoder(w).Encode(map[string]string{"connector.class": "Test"})
		case r.URL.Path == "/connectors/test/status":
			json.NewEncoder(w).Encode(map[string]any{
				"name": "test",
				"type": "source",
				"connector": map[string]any{
					"state":     "RUNNING",
					"worker_id": "w1:8083",
				},
				"tasks": []map[string]any{},
			})
		}
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	health, err := client.ConnectorHealth(context.Background(), "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if health.TotalTasks != 0 {
		t.Errorf("expected 0 tasks, got %d", health.TotalTasks)
	}
}

func TestWithConnectInsecureSkipVerify(t *testing.T) {
	t.Parallel()
	client, err := NewConnectClient("https://localhost:8083", WithConnectInsecureSkipVerify(true))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestWithConnectInsecureSkipVerifyFalse(t *testing.T) {
	t.Parallel()
	client, err := NewConnectClient("https://localhost:8083", WithConnectInsecureSkipVerify(false))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestWithConnectCAEmpty(t *testing.T) {
	t.Parallel()
	client, err := NewConnectClient("https://localhost:8083", WithConnectCA(""))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestWithConnectCAInvalidFile(t *testing.T) {
	t.Parallel()
	_, err := NewConnectClient("https://localhost:8083", WithConnectCA("/nonexistent/ca.pem"))
	if err == nil {
		t.Error("expected error for invalid CA file")
	}
}
