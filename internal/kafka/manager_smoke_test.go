package kafka

import (
	"io"
	"log/slog"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

func TestNewManager_EmptyClusters(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	m, err := NewManager(&config.Config{}, logger)
	if err != nil {
		t.Fatalf("expected ok with empty clusters, got error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil manager")
	}
	if len(m.ClusterNames()) != 0 {
		t.Errorf("expected zero clusters, got %d", len(m.ClusterNames()))
	}
	m.Close()
}

func TestManager_DefaultCluster(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{}
	m, err := NewManager(cfg, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := m.DefaultClusterName(); got != "" {
		t.Errorf("expected empty default cluster, got %q", got)
	}
	if _, err := m.GetClusterConfig("nope"); err == nil {
		t.Error("expected error for unknown cluster")
	}
	m.Close()
}
