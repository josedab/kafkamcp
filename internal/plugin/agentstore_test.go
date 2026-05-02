package plugin

import (
	"testing"
)

func TestAgentConfigStoreSearchConfigs(t *testing.T) {
	t.Parallel()
	store := NewAgentConfigStore()

	tests := []struct {
		name         string
		query        string
		category     string
		verifiedOnly bool
		wantMin      int
	}{
		{"all", "", "", false, 4},
		{"verified only", "", "", true, 3},
		{"by category", "", "operations", false, 1},
		{"by query", "pipeline", "", false, 1},
		{"no results", "nonexistent", "", false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := store.SearchConfigs(tt.query, tt.category, tt.verifiedOnly)
			if len(results) < tt.wantMin {
				t.Errorf("expected at least %d results, got %d", tt.wantMin, len(results))
			}
		})
	}
}

func TestAgentConfigStorePublishAndGet(t *testing.T) {
	t.Parallel()
	store := NewAgentConfigStore()

	cfg := AgentConfig{
		Name:        "test-agent",
		Version:     "1.0.0",
		Description: "Test agent config",
		Author:      "test",
		Category:    "testing",
	}

	if err := store.PublishConfig(cfg); err != nil {
		t.Fatal(err)
	}

	got, err := store.GetConfig("test-agent")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "test-agent" {
		t.Errorf("got name %q, want test-agent", got.Name)
	}

	_, err = store.GetConfig("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent config")
	}
}

func TestAgentConfigStorePublishValidation(t *testing.T) {
	t.Parallel()
	store := NewAgentConfigStore()

	if err := store.PublishConfig(AgentConfig{}); err == nil {
		t.Error("expected error for empty name")
	}
	if err := store.PublishConfig(AgentConfig{Name: "x"}); err == nil {
		t.Error("expected error for empty version")
	}
}

func TestToolBundleSearchAndPublish(t *testing.T) {
	t.Parallel()
	store := NewAgentConfigStore()

	results := store.SearchBundles("", "")
	if len(results) < 3 {
		t.Errorf("expected at least 3 default bundles, got %d", len(results))
	}

	results = store.SearchBundles("monitoring", "")
	if len(results) == 0 {
		t.Error("expected monitoring bundle results")
	}

	bundle := ToolBundle{
		Name:        "test-bundle",
		Version:     "1.0.0",
		Description: "Test bundle",
		Tools:       []string{"tool_a", "tool_b"},
	}
	if err := store.PublishBundle(bundle); err != nil {
		t.Fatal(err)
	}

	got, err := store.GetBundle("test-bundle")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Tools) != 2 {
		t.Errorf("got %d tools, want 2", len(got.Tools))
	}

	// Validation
	if err := store.PublishBundle(ToolBundle{}); err == nil {
		t.Error("expected error for empty bundle name")
	}
	if err := store.PublishBundle(ToolBundle{Name: "x", Version: "1.0"}); err == nil {
		t.Error("expected error for bundle without tools")
	}
}

func TestToolBundleGetNotFound(t *testing.T) {
	t.Parallel()
	store := NewAgentConfigStore()
	_, err := store.GetBundle("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent bundle")
	}
}
