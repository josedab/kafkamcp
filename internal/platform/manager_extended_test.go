package platform

import (
	"context"
	"log/slog"
	"os"
	"testing"
)

func TestGetBackend(t *testing.T) {
	m := NewManager(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	m.RegisterBackend("prod", NewKafkaBackend(PlatformConfig{Type: PlatformKafka}))

	b, err := m.GetBackend("prod")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.Type() != PlatformKafka {
		t.Errorf("expected kafka, got %s", b.Type())
	}

	_, err = m.GetBackend("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent backend")
	}
}

func TestCreateBackend(t *testing.T) {
	tests := []struct {
		name     string
		cfg      PlatformConfig
		wantType PlatformType
	}{
		{"kafka", PlatformConfig{Type: PlatformKafka}, PlatformKafka},
		{"redpanda", PlatformConfig{Type: PlatformRedpanda, AdminURL: "http://localhost:9644"}, PlatformRedpanda},
		{"confluent", PlatformConfig{Type: PlatformConfluentCloud, RESTURL: "https://api.confluent.cloud"}, PlatformConfluentCloud},
		{"default", PlatformConfig{Type: "unknown"}, PlatformKafka},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := CreateBackend(tt.cfg)
			if b.Type() != tt.wantType {
				t.Errorf("expected %s, got %s", tt.wantType, b.Type())
			}
		})
	}
}

func TestConfluentCloudBackendInfo(t *testing.T) {
	b := NewConfluentCloudBackend(PlatformConfig{
		Type:    PlatformConfluentCloud,
		RESTURL: "https://api.confluent.cloud",
	})

	info, err := b.Info(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Type != PlatformConfluentCloud {
		t.Errorf("expected confluent_cloud, got %s", info.Type)
	}
	if info.Version != "managed" {
		t.Errorf("expected version 'managed', got %q", info.Version)
	}
	if v, ok := info.Features["rest_api"]; !ok || v != true {
		t.Error("expected rest_api=true when RESTURL is set")
	}
	if v, ok := info.Features["tiered_storage"]; !ok || v != true {
		t.Error("expected tiered_storage=true")
	}

	// Test without RESTURL
	b2 := NewConfluentCloudBackend(PlatformConfig{Type: PlatformConfluentCloud})
	info2, _ := b2.Info(context.Background())
	if v, ok := info2.Features["rest_api"]; !ok || v != false {
		t.Error("expected rest_api=false when RESTURL is empty")
	}
}

func TestKafkaBackendHasCapabilityFalse(t *testing.T) {
	b := NewKafkaBackend(PlatformConfig{Type: PlatformKafka})
	if b.HasCapability(CapTieredStorage) {
		t.Error("kafka should not support tiered storage")
	}
}

func TestRedpandaBackendHasCapabilityFalse(t *testing.T) {
	b := NewRedpandaBackend(PlatformConfig{Type: PlatformRedpanda})
	if b.HasCapability(CapTieredStorage) {
		t.Error("redpanda should not support tiered storage")
	}
}

func TestConfluentCloudHasCapabilityFalse(t *testing.T) {
	b := NewConfluentCloudBackend(PlatformConfig{Type: PlatformConfluentCloud})
	if b.HasCapability(CapACLs) {
		t.Error("confluent cloud should not support ACLs")
	}
	if b.HasCapability(CapConnect) {
		t.Error("confluent cloud should not support Connect")
	}
}

func TestRedpandaBackendInfoAdminURL(t *testing.T) {
	// With admin URL
	b := NewRedpandaBackend(PlatformConfig{Type: PlatformRedpanda, AdminURL: "http://localhost:9644"})
	info, _ := b.Info(context.Background())
	if v, ok := info.Features["admin_api"]; !ok || v != true {
		t.Error("expected admin_api=true when AdminURL is set")
	}

	// Without admin URL
	b2 := NewRedpandaBackend(PlatformConfig{Type: PlatformRedpanda})
	info2, _ := b2.Info(context.Background())
	if v, ok := info2.Features["admin_api"]; !ok || v != false {
		t.Error("expected admin_api=false when AdminURL is empty")
	}
}
