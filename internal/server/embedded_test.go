package server

import (
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

func TestAvailableCategories(t *testing.T) {
	cats := AvailableCategories()
	if len(cats) == 0 {
		t.Error("expected categories")
	}
	required := []ToolCategory{ToolCategoryCore, ToolCategoryAdvanced, ToolCategoryGovernance, ToolCategoryStreaming, ToolCategoryAnalytics, ToolCategoryPlatform, ToolCategoryObservability, ToolCategoryAll}
	for _, c := range required {
		if _, ok := cats[c]; !ok {
			t.Errorf("missing category: %s", c)
		}
	}
}

func TestToolCategoryConstants(t *testing.T) {
	if ToolCategoryCore != "core" {
		t.Errorf("ToolCategoryCore = %q, want core", ToolCategoryCore)
	}
	if ToolCategoryAll != "all" {
		t.Errorf("ToolCategoryAll = %q, want all", ToolCategoryAll)
	}
}

func TestNewEmbedded_NilConfig(t *testing.T) {
	_, err := NewEmbedded(nil)
	if err == nil {
		t.Error("expected error for nil config")
	}
}

func TestNewEmbedded_MissingKafkaConfig(t *testing.T) {
	_, err := NewEmbedded(&EmbeddedConfig{})
	if err == nil {
		t.Error("expected error for missing KafkaConfig")
	}
}

func TestNewEmbedded_MissingKafkaManager(t *testing.T) {
	_, err := NewEmbedded(&EmbeddedConfig{
		KafkaConfig: &config.Config{},
	})
	if err == nil {
		t.Error("expected error for missing KafkaManager")
	}
}
