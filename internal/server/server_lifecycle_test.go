package server

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== HealthCheck =====================

func TestHealthCheck_PingSuccess(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		PingFn: func(ctx context.Context) error {
			return nil
		},
	}
	s := newTestServerWithMock(t, mock)
	err := s.HealthCheck(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	if mock.CallCount("Ping") != 1 {
		t.Error("expected Ping called once")
	}
}

func TestHealthCheck_PingFailure(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		PingFn: func(ctx context.Context) error {
			return fmt.Errorf("connection refused")
		},
	}
	s := newTestServerWithMock(t, mock)
	err := s.HealthCheck(context.Background())
	if err == nil {
		t.Fatal("expected error from HealthCheck")
	}
}

// ===================== Close =====================

func TestClose_AllManagersClosed(t *testing.T) {
	t.Parallel()
	closeCalled := false
	mock := &kafka.MockKafkaManager{
		CloseFn: func() {
			closeCalled = true
		},
	}
	s := newTestServerWithMock(t, mock)
	err := s.Close()
	if err != nil {
		t.Fatalf("unexpected error on Close: %v", err)
	}
	if !closeCalled {
		t.Error("expected kafkaManager.Close() to be called")
	}
	if mock.CallCount("Close") != 1 {
		t.Error("expected Close called once")
	}
}

func TestClose_NilWatchManager(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	s.watchManager = nil
	err := s.Close()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ===================== getConnectClient =====================

func TestGetConnectClient_EmptyURL(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	cfg := &config.ClusterConfig{Name: "test", ConnectURL: ""}

	client, err := s.getConnectClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client != nil {
		t.Error("expected nil client for empty ConnectURL")
	}
}

func TestGetConnectClient_Caching(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	cfg := &config.ClusterConfig{Name: "test-cluster", ConnectURL: "http://localhost:8083"}

	client1, err := s.getConnectClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client1 == nil {
		t.Fatal("expected non-nil client")
	}

	// Second call should return cached client
	client2, err := s.getConnectClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if client1 != client2 {
		t.Error("expected same client instance from cache")
	}
}

// ===================== checkResourceRateLimit =====================

func TestCheckResourceRateLimit_Allowed(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	err := s.checkResourceRateLimit("agent-1", "topics")
	if err != nil {
		t.Fatalf("expected nil (allowed), got: %v", err)
	}
}

// ===================== resolveClusterName =====================

func TestResolveClusterName_ValidCluster(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterConfigFn: func(name string) (*config.ClusterConfig, error) {
			if name == "staging" {
				return &config.ClusterConfig{Name: "staging"}, nil
			}
			return nil, fmt.Errorf("not found")
		},
	}
	s := newTestServerWithMock(t, mock)
	result := s.resolveClusterName("staging")
	if result != "staging" {
		t.Errorf("expected 'staging', got %q", result)
	}
}

func TestResolveClusterName_EmptyFallsToDefault(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DefaultClusterNameFn: func() string { return "prod" },
	}
	s := newTestServerWithMock(t, mock)
	result := s.resolveClusterName("")
	if result != "prod" {
		t.Errorf("expected 'prod', got %q", result)
	}
}

func TestResolveClusterName_InvalidFallsToDefault(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterConfigFn: func(name string) (*config.ClusterConfig, error) {
			return nil, fmt.Errorf("not found")
		},
		DefaultClusterNameFn: func() string { return "default" },
	}
	s := newTestServerWithMock(t, mock)
	result := s.resolveClusterName("nonexistent")
	if result != "default" {
		t.Errorf("expected 'default', got %q", result)
	}
}

// ===================== marshalResult =====================

func TestMarshalResult_Success(t *testing.T) {
	t.Parallel()
	result, err := marshalResult(map[string]string{"status": "ok"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.IsError {
		t.Error("expected non-error result")
	}
	text := ""
	if len(result.Content) > 0 {
		tc, ok := result.Content[0].(mcp.TextContent)
		if ok {
			text = tc.Text
		}
	}
	var parsed map[string]string
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		t.Fatalf("result should be valid JSON: %v", err)
	}
	if parsed["status"] != "ok" {
		t.Errorf("expected status 'ok', got %q", parsed["status"])
	}
}

func TestMarshalResult_MarshalError(t *testing.T) {
	t.Parallel()
	// Channels cannot be marshaled to JSON
	result, err := marshalResult(make(chan int))
	if err != nil {
		t.Fatalf("marshalResult should not return error, got: %v", err)
	}
	if !result.IsError {
		t.Error("expected error result for unmarshalable value")
	}
}

// ===================== marshalResourceJSON =====================

func TestMarshalResourceJSON_Success(t *testing.T) {
	t.Parallel()
	contents, err := marshalResourceJSON("kafka://topics", map[string]string{"name": "orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}
	tc, ok := contents[0].(mcp.TextResourceContents)
	if !ok {
		t.Fatalf("expected TextResourceContents, got %T", contents[0])
	}
	if tc.URI != "kafka://topics" {
		t.Errorf("expected URI 'kafka://topics', got %q", tc.URI)
	}
	if tc.MIMEType != "application/json" {
		t.Errorf("expected MIME 'application/json', got %q", tc.MIMEType)
	}
	var parsed map[string]string
	if err := json.Unmarshal([]byte(tc.Text), &parsed); err != nil {
		t.Fatalf("expected valid JSON, got parse error: %v", err)
	}
}

func TestMarshalResourceJSON_MarshalError(t *testing.T) {
	t.Parallel()
	_, err := marshalResourceJSON("kafka://test", make(chan int))
	if err == nil {
		t.Fatal("expected error for unmarshalable value")
	}
}

// ===================== getSchemaClient =====================

func TestGetSchemaClient_NilClients(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	// schemaClients is empty by default
	client := s.getSchemaClient("nonexistent")
	if client != nil {
		t.Error("expected nil schema client")
	}
}

func TestGetSchemaClient_EmptyClusterFallback(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DefaultClusterNameFn: func() string { return "default" },
	}
	s := newTestServerWithMock(t, mock)
	client := s.getSchemaClient("")
	if client != nil {
		t.Error("expected nil client when no schema clients configured")
	}
}

// ===================== hasSchemaRegistry =====================

func TestHasSchemaRegistry_False(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	if s.hasSchemaRegistry() {
		t.Error("expected false when no schema clients configured")
	}
}
