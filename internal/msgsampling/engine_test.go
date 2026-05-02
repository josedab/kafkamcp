package msgsampling

import (
	"log/slog"
	"testing"
)

func TestProfile_Empty(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	result := e.Profile("test-topic", nil)
	if result.SampleCount != 0 {
		t.Errorf("expected 0 samples, got %d", result.SampleCount)
	}
}

func TestProfile_WithSamples(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	samples := []MessageSample{
		{KeySize: 10, ValueSize: 100, HasKey: true, KeyHash: "key1", SchemaType: "json"},
		{KeySize: 10, ValueSize: 200, HasKey: true, KeyHash: "key2", SchemaType: "json"},
		{KeySize: 0, ValueSize: 50, HasKey: false, SchemaType: "string"},
		{KeySize: 10, ValueSize: 300, HasKey: true, KeyHash: "key1", SchemaType: "json"},
		{KeySize: 10, ValueSize: 500, HasKey: true, KeyHash: "key3", SchemaType: "avro"},
	}

	result := e.Profile("test-topic", samples)
	if result.SampleCount != 5 {
		t.Errorf("expected 5 samples, got %d", result.SampleCount)
	}
	if result.SizeStats.MinBytes > result.SizeStats.MaxBytes {
		t.Error("min should be <= max")
	}
	if result.KeyAnalysis.UniqueKeys != 3 {
		t.Errorf("expected 3 unique keys, got %d", result.KeyAnalysis.UniqueKeys)
	}
	if result.KeyAnalysis.NullKeyPercent != 20 {
		t.Errorf("expected 20%% null keys, got %f", result.KeyAnalysis.NullKeyPercent)
	}
	if len(result.SizeHistogram) == 0 {
		t.Error("expected non-empty histogram")
	}
	if result.SchemaDistribution["json"] != 3 {
		t.Errorf("expected 3 json schemas, got %d", result.SchemaDistribution["json"])
	}
}

func TestGetCachedProfile(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	samples := []MessageSample{
		{KeySize: 10, ValueSize: 100, HasKey: true, KeyHash: "k", SchemaType: "json"},
	}
	e.Profile("cached-topic", samples)

	cached := e.GetCachedProfile("cached-topic")
	if cached == nil {
		t.Error("expected cached profile")
	}
	if e.GetCachedProfile("nonexistent") != nil {
		t.Error("expected nil for nonexistent topic")
	}
}
