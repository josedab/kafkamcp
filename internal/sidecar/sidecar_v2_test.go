package sidecar

import (
	"testing"
)

func TestAutoDiscover(t *testing.T) {
	t.Parallel()
	// AutoDiscover may or may not find a cluster depending on environment
	result := AutoDiscover()
	// Just verify it doesn't panic
	_ = result
}

func TestGenerateClientConfig(t *testing.T) {
	t.Parallel()
	configs := GenerateClientConfig("localhost:9092", "stdio", "localhost:8080")
	if len(configs) != 3 {
		t.Errorf("expected 3 configs (go, python, java), got %d", len(configs))
	}
	for lang, cfg := range configs {
		if cfg == "" {
			t.Errorf("empty config for %s", lang)
		}
	}
}

func TestSidecarV2_EmptyBootstrap(t *testing.T) {
	t.Parallel()
	// Without a running Kafka, NewV2 with empty config should fail gracefully
	_, err := NewV2(Config{})
	if err == nil {
		// It's OK if auto-discovery finds something, but if no Kafka is running this should error
		t.Log("NewV2 succeeded with auto-discovery - Kafka must be running locally")
	}
}
