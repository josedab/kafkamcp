package platform

import (
	"context"
	"testing"
)

func TestKafkaBackend(t *testing.T) {
	t.Parallel()
	b := NewKafkaBackend(PlatformConfig{Type: PlatformKafka})
	if b.Type() != PlatformKafka {
		t.Errorf("expected kafka, got %s", b.Type())
	}
	if !b.HasCapability(CapTopicCRUD) {
		t.Error("kafka should support topic CRUD")
	}
	info, err := b.Info(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Type != PlatformKafka {
		t.Error("wrong platform type in info")
	}
}

func TestRedpandaBackend(t *testing.T) {
	t.Parallel()
	b := NewRedpandaBackend(PlatformConfig{Type: PlatformRedpanda, AdminURL: "http://localhost:9644"})
	if b.Type() != PlatformRedpanda {
		t.Errorf("expected redpanda, got %s", b.Type())
	}
	if !b.HasCapability(CapSchemaRegistry) {
		t.Error("redpanda should support schema registry")
	}
	if b.HasCapability(CapConnect) {
		t.Error("redpanda should not support Kafka Connect natively")
	}
}

func TestConfluentCloudBackend(t *testing.T) {
	t.Parallel()
	b := NewConfluentCloudBackend(PlatformConfig{Type: PlatformConfluentCloud, RESTURL: "https://api.confluent.cloud"})
	if b.Type() != PlatformConfluentCloud {
		t.Errorf("expected confluent_cloud, got %s", b.Type())
	}
	if !b.HasCapability(CapTieredStorage) {
		t.Error("confluent cloud should support tiered storage")
	}
}

func TestDetectPlatform(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		props    map[string]string
		wantType PlatformType
	}{
		{"kafka", map[string]string{}, PlatformKafka},
		{"redpanda", map[string]string{"redpanda.admin.url": "http://localhost:9644"}, PlatformRedpanda},
		{"confluent", map[string]string{"confluent.cloud.rest.url": "https://api.confluent.cloud"}, PlatformConfluentCloud},
		{"msk", map[string]string{"aws.msk.serverless": "true"}, PlatformMSKServerless},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectPlatform("localhost:9092", tt.props)
			if got != tt.wantType {
				t.Errorf("expected %s, got %s", tt.wantType, got)
			}
		})
	}
}

func TestPlatformManager(t *testing.T) {
	t.Parallel()
	m := NewManager(nil)
	m.RegisterBackend("prod", NewKafkaBackend(PlatformConfig{Type: PlatformKafka}))
	m.RegisterBackend("rp-dev", NewRedpandaBackend(PlatformConfig{Type: PlatformRedpanda}))

	if !m.CheckCapability("prod", CapTopicCRUD) {
		t.Error("prod should support topic CRUD")
	}
	if m.CheckCapability("nonexistent", CapTopicCRUD) {
		t.Error("nonexistent should not have capabilities")
	}

	platforms := m.ListPlatforms(context.Background())
	if len(platforms) != 2 {
		t.Errorf("expected 2 platforms, got %d", len(platforms))
	}
}
