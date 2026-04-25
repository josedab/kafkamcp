package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRestartConnector_NilClient(t *testing.T) {
	var client *ConnectClient
	_, err := client.RestartConnector(context.Background(), "conn", -1, false)
	if err == nil {
		t.Fatal("expected error for nil client")
	}
	if !strings.Contains(err.Error(), "not configured") {
		t.Errorf("expected 'not configured' error, got: %v", err)
	}
}

func TestRestartConnector_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_code":404,"message":"Connector not found"}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	_, err := client.RestartConnector(context.Background(), "nonexistent", -1, false)
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected 404 in error, got: %v", err)
	}
}

func TestRestartConnector_TaskHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	_, err := client.RestartConnector(context.Background(), "conn", 0, false)
	if err == nil {
		t.Fatal("expected error for 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("expected 401 in error, got: %v", err)
	}
}

func TestGetConnectorConfig_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/config") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
			"connection.url":  "jdbc:postgresql://localhost/mydb",
			"topic.prefix":    "db.",
		})
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	// Use httpPut to GET config since ConnectClient doesn't have a GetConfig method directly,
	// but we can test the UpdateConnector which calls httpPut
	result, err := client.UpdateConnector(context.Background(), "my-conn", map[string]string{
		"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
		"topic.prefix":    "db.",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Name != "my-conn" {
		t.Errorf("expected name 'my-conn', got %q", result.Name)
	}
	if result.Status != "updated" {
		t.Errorf("expected status 'updated', got %q", result.Status)
	}
}

func TestUpdateConnectorConfig_Roundtrip(t *testing.T) {
	var receivedConfig map[string]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			json.NewDecoder(r.Body).Decode(&receivedConfig)
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(receivedConfig)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	config := map[string]string{
		"connector.class": "FileStreamSource",
		"file":            "/tmp/test.txt",
		"topic":           "file-topic",
	}
	result, err := client.UpdateConnector(context.Background(), "file-conn", config)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Name != "file-conn" {
		t.Errorf("expected name 'file-conn', got %q", result.Name)
	}
	if receivedConfig["connector.class"] != "FileStreamSource" {
		t.Errorf("expected connector.class to be passed, got %v", receivedConfig)
	}
}

func TestUpdateConnector_NilClient(t *testing.T) {
	var client *ConnectClient
	_, err := client.UpdateConnector(context.Background(), "conn", map[string]string{"k": "v"})
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestCreateConnector_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"name":   "new-conn",
			"config": map[string]string{"connector.class": "FileSource", "topic": "test"},
		})
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	result, err := client.CreateConnector(context.Background(), "new-conn", map[string]string{
		"connector.class": "FileSource",
		"topic":           "test",
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

func TestCreateConnector_NilClient(t *testing.T) {
	var client *ConnectClient
	_, err := client.CreateConnector(context.Background(), "conn", map[string]string{})
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestDeleteConnector_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	err := client.DeleteConnector(context.Background(), "old-conn")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteConnector_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error_code":404,"message":"Connector not found"}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	err := client.DeleteConnector(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for 404")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Errorf("expected 404 in error, got: %v", err)
	}
}

func TestDeleteConnector_NilClient(t *testing.T) {
	var client *ConnectClient
	err := client.DeleteConnector(context.Background(), "conn")
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestConnectorHealth_NilClient(t *testing.T) {
	var client *ConnectClient
	_, err := client.ConnectorHealth(context.Background(), "conn")
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestHTTPDelete_AuthFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"unauthorized"}`))
	}))
	defer server.Close()

	client, _ := NewConnectClient(server.URL)
	err := client.httpDelete(context.Background(), server.URL+"/connectors/test")
	if err == nil {
		t.Fatal("expected error for 401")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("expected 401 in error, got: %v", err)
	}
}
