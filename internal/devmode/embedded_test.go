package devmode

import (
	"log/slog"
	"testing"
)

func TestAllocatePort(t *testing.T) {
	port, err := AllocatePort()
	if err != nil {
		t.Fatalf("AllocatePort: %v", err)
	}
	if port <= 0 || port > 65535 {
		t.Errorf("invalid port: %d", port)
	}
}

func TestDefaultEmbeddedBrokerConfig(t *testing.T) {
	cfg := DefaultEmbeddedBrokerConfig()
	if cfg.Type != BrokerKRaft {
		t.Errorf("Type = %q, want kraft", cfg.Type)
	}
	if cfg.DataDir == "" {
		t.Error("DataDir should not be empty")
	}
}

func TestEmbeddedBrokerStatus(t *testing.T) {
	eb := NewEmbeddedBroker(DefaultEmbeddedBrokerConfig())
	status := eb.Status()
	if status.Running {
		t.Error("should not be running")
	}
	if status.Type != BrokerKRaft {
		t.Errorf("Type = %q, want kraft", status.Type)
	}
}

func TestGenerateIDEConfigs(t *testing.T) {
	configs := GenerateIDEConfigs("localhost:9092")
	if len(configs) != 3 {
		t.Errorf("expected 3 IDE configs, got %d", len(configs))
	}

	ides := map[string]bool{}
	for _, c := range configs {
		ides[c.IDE] = true
		if len(c.Instructions) == 0 {
			t.Errorf("IDE %q has no instructions", c.IDE)
		}
	}
	for _, ide := range []string{"vscode", "cursor", "claude-desktop"} {
		if !ides[ide] {
			t.Errorf("missing IDE config for %s", ide)
		}
	}
}

func TestStartDevSession(t *testing.T) {
	e := NewEngine(slog.Default())
	session := e.StartDevSession("localhost:9092")

	if session.Status != "ready" {
		t.Errorf("Status = %q, want ready", session.Status)
	}
	if session.BrokerAddr != "localhost:9092" {
		t.Errorf("BrokerAddr = %q", session.BrokerAddr)
	}
	if len(session.Topics) == 0 {
		t.Error("expected sample topics")
	}
	if len(session.IDEConfigs) == 0 {
		t.Error("expected IDE configs")
	}
	if len(session.QuickStart) == 0 {
		t.Error("expected quick start instructions")
	}
}

func TestStartDevSession_DefaultBroker(t *testing.T) {
	e := NewEngine(slog.Default())
	session := e.StartDevSession("")
	if session.BrokerAddr != "localhost:9092" {
		t.Errorf("BrokerAddr = %q, want localhost:9092", session.BrokerAddr)
	}
}
