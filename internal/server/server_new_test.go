package server

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/kafka"
)

// TestNewServer_HappyPath exercises the New() constructor end-to-end with
// a minimal config so the full init pipeline (Analytics, Governance,
// Streaming, Platform, registerTools, registerResources) gets executed.
func TestNewServer_HappyPath(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{
		Server:      config.ServerConfig{Transport: "stdio"},
		Clusters:    []config.ClusterConfig{},
		SchemaCache: config.CacheConfig{MaxEntries: 100, TTLSeconds: 60},
		Audit:       config.AuditConfig{Enabled: false, MaxEntries: 100},
		Policies:    config.PolicyConfig{DefaultDeny: false},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mock := &kafka.MockKafkaManager{}

	srv, err := New(cfg, mock, logger, "test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if srv == nil {
		t.Fatal("expected non-nil server")
	}
	if srv.MCPServer() == nil {
		t.Error("expected non-nil MCP server")
	}
	if srv.Metrics() == nil {
		t.Error("expected non-nil metrics")
	}
	if srv.EngineBackend() == nil {
		t.Error("expected non-nil engine backend")
	}
	if got := srv.RegisteredToolCount(); got == 0 {
		t.Error("expected RegisteredToolCount > 0")
	}
	if tools := srv.RegisteredTools(); len(tools) == 0 {
		t.Error("expected non-empty registered tools list")
	}

	if err := srv.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck: %v", err)
	}
	if err := srv.ReadinessCheck(context.Background()); err != nil {
		t.Errorf("ReadinessCheck: %v", err)
	}

	if err := srv.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestNewServer_WithSchemaRegistry verifies the schema-client per-cluster
// branch in New().
func TestNewServer_WithSchemaRegistry(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{
		Server: config.ServerConfig{Transport: "stdio"},
		Clusters: []config.ClusterConfig{
			{
				Name:             "primary",
				BootstrapServers: "localhost:9092",
				SchemaRegistry: config.SchemaRegistryConfig{
					URL: "http://localhost:8081",
				},
			},
		},
		SchemaCache: config.CacheConfig{MaxEntries: 100, TTLSeconds: 60},
		Audit:       config.AuditConfig{Enabled: false, MaxEntries: 100},
		Policies:    config.PolicyConfig{DefaultDeny: false},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mock := &kafka.MockKafkaManager{}

	srv, err := New(cfg, mock, logger, "test")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !srv.hasSchemaRegistry() {
		t.Error("expected hasSchemaRegistry=true")
	}
	if sc := srv.getSchemaClient("primary"); sc == nil {
		t.Error("expected schema client for primary cluster")
	}
	// Fallback path: ask for an unknown cluster.
	if sc := srv.getSchemaClient("unknown"); sc != nil {
		// Falls back to default cluster client (kafkaManager.DefaultClusterName()
		// returns "default" from the mock, which has no schema client).
		t.Logf("fallback returned client=%v (mock default cluster has no schema client)", sc)
	}

	if err := srv.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

func TestRegisteredToolCount_Empty(t *testing.T) {
	t.Parallel()
	s := &Server{registeredTools: map[string]string{}}
	if got := s.RegisteredToolCount(); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
	if got := s.RegisteredTools(); len(got) != 0 {
		t.Errorf("expected empty, got %v", got)
	}
	if got := s.SkippedToolCount(); got != 0 {
		t.Errorf("expected 0, got %d", got)
	}
}
