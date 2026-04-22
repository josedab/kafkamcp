package kafka

import (
	"strings"
	"testing"
)

func TestNewStreamManager(t *testing.T) {
	sm := newStreamSessionManager()
	if sm == nil {
		t.Fatal("expected non-nil stream manager")
	}
	if sm.sessions == nil {
		t.Error("expected non-nil sessions map")
	}
}

func TestGenerateSessionID(t *testing.T) {
	id := generateSessionID()
	if !strings.HasPrefix(id, "stream-") {
		t.Errorf("expected prefix 'stream-', got %q", id)
	}
	if len(id) < 20 {
		t.Errorf("expected longer session ID, got %q", id)
	}

	// Should be unique
	id2 := generateSessionID()
	if id == id2 {
		t.Error("expected unique session IDs")
	}
}

func TestStreamConfigDefaults(t *testing.T) {
	cfg := StreamConfig{}
	if cfg.BatchSize != 0 {
		t.Errorf("expected default BatchSize 0, got %d", cfg.BatchSize)
	}
	if cfg.AutoCommit {
		t.Error("expected default AutoCommit false")
	}
}

func TestResolveCluster(t *testing.T) {
	// Test the pure resolveCluster function on Manager
	m := &Manager{defClust: "default-cluster"}

	if m.resolveCluster("") != "default-cluster" {
		t.Errorf("expected 'default-cluster' for empty, got %q", m.resolveCluster(""))
	}
	if m.resolveCluster("custom") != "custom" {
		t.Errorf("expected 'custom', got %q", m.resolveCluster("custom"))
	}
}
