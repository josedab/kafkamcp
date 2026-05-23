package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

func TestValidateConnectorConfig_NormalPassthrough(t *testing.T) {
	t.Parallel()
	config := map[string]string{
		"connector.class": "io.debezium.connector.mysql.MySqlConnector",
		"tasks.max":       "1",
	}
	deny, redacted := validateConnectorConfig(config)
	if deny != nil {
		t.Fatalf("unexpected denial: %v", deny)
	}
	if redacted["connector.class"] != "io.debezium.connector.mysql.MySqlConnector" {
		t.Errorf("expected passthrough, got %s", redacted["connector.class"])
	}
	if redacted["tasks.max"] != "1" {
		t.Errorf("expected passthrough, got %s", redacted["tasks.max"])
	}
}

func TestValidateConnectorConfig_RedactsSensitiveKeys(t *testing.T) {
	t.Parallel()
	sensitiveKeys := []string{
		"database.password",
		"aws.secret.access.key",
		"api.key.value",
		"sasl.jaas.config",
		"ssl.keystore.password",
		"ssl.truststore.password",
		"ssl.key.password",
		"my.secret.setting",
		"auth.token.value",
		"consumer.override.sasl.jaas.config",
		"producer.override.sasl.jaas.config",
	}

	for _, key := range sensitiveKeys {
		config := map[string]string{key: "supersecret123"}
		deny, redacted := validateConnectorConfig(config)
		if deny != nil {
			t.Fatalf("unexpected denial for key %s", key)
		}
		if redacted[key] != "***REDACTED***" {
			t.Errorf("key %q should be redacted, got %q", key, redacted[key])
		}
	}
}

func TestValidateConnectorConfig_CaseInsensitiveRedaction(t *testing.T) {
	t.Parallel()
	config := map[string]string{
		"Database.PASSWORD": "secret",
		"MY.SECRET.KEY":     "hidden",
	}
	deny, redacted := validateConnectorConfig(config)
	if deny != nil {
		t.Fatalf("unexpected denial")
	}
	for k, v := range redacted {
		if v != "***REDACTED***" {
			t.Errorf("key %q should be redacted (case insensitive), got %q", k, v)
		}
	}
}

func TestValidateConnectorConfig_OversizedKeyRejected(t *testing.T) {
	t.Parallel()
	longKey := strings.Repeat("x", maxInputLength+1)
	config := map[string]string{longKey: "value"}
	deny, _ := validateConnectorConfig(config)
	if deny == nil {
		t.Fatal("expected denial for oversized key")
	}
}

func TestValidateConnectorConfig_OversizedValueRejected(t *testing.T) {
	t.Parallel()
	longVal := strings.Repeat("v", maxInputLength+1)
	config := map[string]string{"key": longVal}
	deny, _ := validateConnectorConfig(config)
	if deny == nil {
		t.Fatal("expected denial for oversized value")
	}
}

func TestValidateConnectorConfig_EmptyConfig(t *testing.T) {
	t.Parallel()
	deny, redacted := validateConnectorConfig(map[string]string{})
	if deny != nil {
		t.Fatalf("unexpected denial for empty config")
	}
	if len(redacted) != 0 {
		t.Errorf("expected empty redacted map, got %v", redacted)
	}
}

func TestValidateConnectorConfig_MultipleSensitiveKeys(t *testing.T) {
	t.Parallel()
	config := map[string]string{
		"normal.key":        "visible",
		"database.password": "secret1",
		"api.key":           "secret2",
	}
	deny, redacted := validateConnectorConfig(config)
	if deny != nil {
		t.Fatalf("unexpected denial")
	}
	if redacted["normal.key"] != "visible" {
		t.Errorf("normal key should not be redacted, got %s", redacted["normal.key"])
	}
	if redacted["database.password"] != "***REDACTED***" {
		t.Errorf("password key should be redacted")
	}
	if redacted["api.key"] != "***REDACTED***" {
		t.Errorf("api.key should be redacted")
	}
}

// ===================== Handler-level tests with mock =====================

func TestHandleCreateConnector_MissingName_Connect(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleCreateConnector(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "name is required") {
		t.Error("expected 'name is required' error")
	}
}

func TestHandleCreateConnector_ConnectNotAvailable_Connect(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterConfigFn: func(name string) (*config.ClusterConfig, error) {
			return &config.ClusterConfig{Name: "default", BootstrapServers: "localhost:9092"}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	req := makeToolRequest(map[string]any{
		"name":   "my-connector",
		"config": map[string]any{"connector.class": "FileSource"},
	})

	result, err := s.handleCreateConnector(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "Kafka Connect is not available") {
		t.Errorf("expected 'not available' error, got: %s", resultText(t, result))
	}
}

func TestHandleCreateConnector_MissingConfig_Connect(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterConfigFn: func(name string) (*config.ClusterConfig, error) {
			return &config.ClusterConfig{Name: "default", ConnectURL: "http://localhost:8083"}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	req := makeToolRequest(map[string]any{"name": "my-connector"})

	result, err := s.handleCreateConnector(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "config is required") {
		t.Errorf("expected 'config is required', got: %s", resultText(t, result))
	}
}

func TestHandleUpdateConnector_MissingName_Connect(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleUpdateConnector(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleUpdateConnector_ConnectNotAvailable_Connect(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterConfigFn: func(name string) (*config.ClusterConfig, error) {
			return &config.ClusterConfig{Name: "default"}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	req := makeToolRequest(map[string]any{
		"name":   "my-connector",
		"config": map[string]any{"connector.class": "FileSource"},
	})

	result, err := s.handleUpdateConnector(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "Kafka Connect is not available") {
		t.Errorf("expected 'not available' error")
	}
}

func TestHandleDeleteConnector_MissingName_Connect(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleDeleteConnector(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDeleteConnector_ConnectNotAvailable_Connect(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterConfigFn: func(name string) (*config.ClusterConfig, error) {
			return &config.ClusterConfig{Name: "default"}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	req := makeToolRequest(map[string]any{"name": "my-connector"})

	result, err := s.handleDeleteConnector(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleRestartConnector_MissingName_Connect(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleRestartConnector(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleRestartConnector_ConnectNotAvailable_Connect(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterConfigFn: func(name string) (*config.ClusterConfig, error) {
			return &config.ClusterConfig{Name: "default"}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	req := makeToolRequest(map[string]any{"name": "my-connector"})

	result, err := s.handleRestartConnector(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleConnectorHealth_MissingName_Connect(t *testing.T) {
	t.Parallel()
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	req := makeToolRequest(map[string]any{})

	result, err := s.handleConnectorHealth(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleConnectorHealth_ConnectNotAvailable_Connect(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterConfigFn: func(name string) (*config.ClusterConfig, error) {
			return &config.ClusterConfig{Name: "default"}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	req := makeToolRequest(map[string]any{"name": "my-connector"})

	result, err := s.handleConnectorHealth(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCreateConnector_ClusterError_Connect(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterConfigFn: func(name string) (*config.ClusterConfig, error) {
			return nil, fmt.Errorf("cluster not found")
		},
	}
	s := newTestServerWithMock(t, mock)
	req := makeToolRequest(map[string]any{
		"name":   "connector",
		"config": map[string]any{"connector.class": "Source"},
	})

	result, err := s.handleCreateConnector(context.Background(), req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
