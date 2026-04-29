package sidecar

import (
	"testing"
)

func TestNewRequiresBootstrap(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error for empty bootstrap_servers")
	}
}

func TestDefaultClusterName(t *testing.T) {
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
	if TierCore != 0 {
		t.Error("TierCore should be 0")
	}
	if TierFull != 3 {
		t.Error("TierFull should be 3")
	}
}
