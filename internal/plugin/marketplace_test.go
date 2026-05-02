package plugin

import (
	"context"
	"testing"
)

func newTestMarketplace(t *testing.T) *Marketplace {
	t.Helper()
	r := NewRegistry(nil)
	t.Cleanup(func() { r.Close() })
	return NewMarketplace(r)
}

func TestMarketplaceSearch(t *testing.T) {
	t.Parallel()
	m := newTestMarketplace(t)

	t.Run("empty query returns all entries", func(t *testing.T) {
		results := m.Search("", "", false)
		if len(results) != 8 {
			t.Fatalf("expected 8 results, got %d", len(results))
		}
	})

	t.Run("search by query matches name", func(t *testing.T) {
		results := m.Search("slack", "", false)
		if len(results) != 1 {
			t.Fatalf("expected 1 result, got %d", len(results))
		}
		if results[0].Name != "slack-notifier" {
			t.Fatalf("expected slack-notifier, got %s", results[0].Name)
		}
	})

	t.Run("search by category", func(t *testing.T) {
		results := m.Search("", "security", false)
		for _, r := range results {
			if r.Category != "security" {
				t.Fatalf("expected category security, got %s", r.Category)
			}
		}
		if len(results) != 3 {
			t.Fatalf("expected 3 security results, got %d", len(results))
		}
	})

	t.Run("verified only", func(t *testing.T) {
		results := m.Search("", "", true)
		for _, r := range results {
			if !r.Verified {
				t.Fatalf("expected only verified plugins, got unverified: %s", r.Name)
			}
		}
		if len(results) != 5 {
			t.Fatalf("expected 5 verified results, got %d", len(results))
		}
	})

	t.Run("results sorted by rating descending", func(t *testing.T) {
		results := m.Search("", "", false)
		for i := 1; i < len(results); i++ {
			if results[i].Rating > results[i-1].Rating {
				t.Fatalf("results not sorted by rating: %.1f > %.1f at index %d",
					results[i].Rating, results[i-1].Rating, i)
			}
		}
	})

	t.Run("search by tag", func(t *testing.T) {
		results := m.Search("prometheus", "", false)
		if len(results) != 1 {
			t.Fatalf("expected 1 result for tag search, got %d", len(results))
		}
		if results[0].Name != "kafka-metrics-exporter" {
			t.Fatalf("expected kafka-metrics-exporter, got %s", results[0].Name)
		}
	})

	t.Run("no matches returns empty", func(t *testing.T) {
		results := m.Search("nonexistent-plugin-xyz", "", false)
		if len(results) != 0 {
			t.Fatalf("expected 0 results, got %d", len(results))
		}
	})
}

func TestMarketplaceInstall(t *testing.T) {
	t.Parallel()
	t.Run("successful install", func(t *testing.T) {
		m := newTestMarketplace(t)

		entry, err := m.Install("slack-notifier")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if entry.Name != "slack-notifier" {
			t.Fatalf("expected slack-notifier, got %s", entry.Name)
		}
		if !entry.Installed {
			t.Fatal("expected Installed to be true")
		}
		if entry.InstalledVer != entry.Version {
			t.Fatalf("expected InstalledVer %s, got %s", entry.Version, entry.InstalledVer)
		}
		if entry.Downloads < 1 {
			t.Fatalf("expected downloads >= 1, got %d", entry.Downloads)
		}
	})

	t.Run("not found", func(t *testing.T) {
		m := newTestMarketplace(t)

		_, err := m.Install("does-not-exist")
		if err == nil {
			t.Fatal("expected error for missing plugin")
		}
	})

	t.Run("dependency missing", func(t *testing.T) {
		m := newTestMarketplace(t)

		// Publish a plugin with an unmet dependency.
		err := m.Publish(MarketplaceEntry{
			Name:         "dep-plugin",
			Version:      "1.0.0",
			Dependencies: []string{"missing-dep"},
		})
		if err != nil {
			t.Fatalf("unexpected publish error: %v", err)
		}

		_, err = m.Install("dep-plugin")
		if err == nil {
			t.Fatal("expected error for missing dependency")
		}
	})

	t.Run("dependency satisfied", func(t *testing.T) {
		m := newTestMarketplace(t)

		// Register the dependency in the registry so it's found.
		p := &testPlugin{name: "slack-notifier", version: "1.1.0"}
		if err := m.registry.Register(context.Background(), p, nil); err != nil {
			t.Fatalf("unexpected register error: %v", err)
		}

		err := m.Publish(MarketplaceEntry{
			Name:         "dependent-plugin",
			Version:      "1.0.0",
			Dependencies: []string{"slack-notifier"},
		})
		if err != nil {
			t.Fatalf("unexpected publish error: %v", err)
		}

		entry, err := m.Install("dependent-plugin")
		if err != nil {
			t.Fatalf("unexpected install error: %v", err)
		}
		if !entry.Installed {
			t.Fatal("expected Installed to be true")
		}
	})
}

func TestMarketplaceGetEntry(t *testing.T) {
	t.Parallel()
	m := newTestMarketplace(t)

	t.Run("found", func(t *testing.T) {
		entry, err := m.GetEntry("schema-validator")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if entry.Name != "schema-validator" {
			t.Fatalf("expected schema-validator, got %s", entry.Name)
		}
		if entry.Version != "1.2.0" {
			t.Fatalf("expected version 1.2.0, got %s", entry.Version)
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, err := m.GetEntry("no-such-plugin")
		if err == nil {
			t.Fatal("expected error for missing plugin")
		}
	})
}

func TestMarketplacePublish(t *testing.T) {
	t.Parallel()
	m := newTestMarketplace(t)

	t.Run("valid publish", func(t *testing.T) {
		err := m.Publish(MarketplaceEntry{
			Name:        "my-plugin",
			Version:     "0.1.0",
			Description: "A custom plugin",
			Author:      "tester",
			Category:    "utility",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		entry, err := m.GetEntry("my-plugin")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if entry.Version != "0.1.0" {
			t.Fatalf("expected version 0.1.0, got %s", entry.Version)
		}
		if entry.PublishedAt == "" {
			t.Fatal("expected PublishedAt to be set")
		}
	})

	t.Run("missing name", func(t *testing.T) {
		err := m.Publish(MarketplaceEntry{Version: "1.0.0"})
		if err == nil {
			t.Fatal("expected error for missing name")
		}
	})

	t.Run("missing version", func(t *testing.T) {
		err := m.Publish(MarketplaceEntry{Name: "no-version"})
		if err == nil {
			t.Fatal("expected error for missing version")
		}
	})
}

func TestMarketplaceSeedCatalog(t *testing.T) {
	t.Parallel()
	m := newTestMarketplace(t)

	expected := []string{
		"kafka-metrics-exporter",
		"schema-validator",
		"kafka-cost-optimizer",
		"slack-notifier",
		"kafka-encrypt",
		"topic-archiver",
		"kafka-ml-pipeline",
		"compliance-auditor",
	}

	for _, name := range expected {
		entry, err := m.GetEntry(name)
		if err != nil {
			t.Errorf("expected built-in entry %q to exist: %v", name, err)
			continue
		}
		if entry.Name != name {
			t.Errorf("expected name %q, got %q", name, entry.Name)
		}
		if entry.Version == "" {
			t.Errorf("expected version for %q to be set", name)
		}
	}
}
