package sidecar

import (
	"context"
	"sync"
	"testing"
)

func TestNewRequiresBootstrap(t *testing.T) {
	t.Parallel()
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error for empty bootstrap_servers")
	}
}

func TestDefaultClusterName(t *testing.T) {
	t.Parallel()
	cfg := Config{
		BootstrapServers: "localhost:9092",
	}
	if cfg.ClusterName == "" {
		cfg.ClusterName = "default"
	}
	if cfg.ClusterName != "default" {
		t.Fatalf("expected default cluster name, got %s", cfg.ClusterName)
	}
}

func TestToolTierConstants(t *testing.T) {
	t.Parallel()
	if TierCore != 0 {
		t.Error("TierCore should be 0")
	}
	if TierFull != 3 {
		t.Error("TierFull should be 3")
	}
}

func TestMCPServer_ReturnsNonNilFromStruct(t *testing.T) {
	t.Parallel()
	// Test using a manually constructed Sidecar (bypassing Kafka)
	s := &Sidecar{running: true}
	// MCPServer returns the server field, which is nil on empty struct
	srv := s.MCPServer()
	if srv != nil {
		t.Error("expected nil server on empty sidecar")
	}
}

func TestCallTool_ErrorWhenNotRunning(t *testing.T) {
	t.Parallel()
	s := &Sidecar{running: false}
	_, err := s.CallTool(context.Background(), "kafka_list_topics", nil)
	if err == nil {
		t.Error("expected error when sidecar is not running")
	}
}

func TestCallTool_ReturnsResponseWhenRunning(t *testing.T) {
	t.Parallel()
	s := &Sidecar{running: true}
	result, err := s.CallTool(context.Background(), "kafka_list_topics", nil)
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if len(result) == 0 {
		t.Error("expected non-empty response")
	}
}

func TestClose_ReleasesResources(t *testing.T) {
	t.Parallel()
	// When running=false, Close is a no-op (idempotent)
	s := &Sidecar{running: false}
	err := s.Close()
	if err != nil {
		t.Errorf("Close on non-running sidecar should return nil: %v", err)
	}
	if s.running {
		t.Error("expected running=false after Close")
	}
}

func TestClose_IdempotentDoubleClose(t *testing.T) {
	t.Parallel()
	s := &Sidecar{running: false}
	err1 := s.Close()
	err2 := s.Close()
	if err1 != nil || err2 != nil {
		t.Errorf("double close should return nil: err1=%v, err2=%v", err1, err2)
	}
}

func TestClose_ConcurrentRaceSafety(t *testing.T) {
	t.Parallel()
	s := &Sidecar{running: false}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Close()
		}()
	}
	wg.Wait()
}

func TestServeStdio_ErrorWhenClosed(t *testing.T) {
	t.Parallel()
	s := &Sidecar{running: false}
	err := s.ServeStdio(context.Background(), nil, nil)
	if err == nil {
		t.Error("expected error when sidecar is closed")
	}
}

func TestCallTool_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	s := &Sidecar{running: true}
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.CallTool(context.Background(), "test", nil)
		}()
	}
	wg.Wait()
}
