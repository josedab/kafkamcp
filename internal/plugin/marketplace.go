package plugin

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// MarketplaceEntry represents a plugin available in the marketplace.
type MarketplaceEntry struct {
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	Description  string   `json:"description"`
	Author       string   `json:"author"`
	License      string   `json:"license"`
	Category     string   `json:"category"` // "monitoring", "security", "analytics", "integration", "utility"
	Tags         []string `json:"tags"`
	Downloads    int      `json:"downloads"`
	Rating       float64  `json:"rating"` // 0.0-5.0
	Verified     bool     `json:"verified"`
	Type         string   `json:"type"` // "native", "grpc", "wasm"
	Homepage     string   `json:"homepage,omitempty"`
	Dependencies []string `json:"dependencies,omitempty"`
	Installed    bool     `json:"installed"`
	InstalledVer string   `json:"installed_version,omitempty"`
	PublishedAt  string   `json:"published_at"`
}

// Marketplace manages discoverable plugins with search, install, and versioning.
type Marketplace struct {
	mu       sync.RWMutex
	catalog  map[string]*MarketplaceEntry
	registry *Registry
}

// NewMarketplace creates a marketplace backed by a plugin registry.
func NewMarketplace(registry *Registry) *Marketplace {
	m := &Marketplace{
		catalog:  make(map[string]*MarketplaceEntry),
		registry: registry,
	}
	m.seedBuiltinCatalog()
	return m
}

// seedBuiltinCatalog populates the marketplace with built-in plugin listings.
func (m *Marketplace) seedBuiltinCatalog() {
	builtins := []MarketplaceEntry{
		{Name: "kafka-metrics-exporter", Version: "1.0.0", Description: "Export Kafka metrics to Prometheus, Datadog, or CloudWatch", Author: "kafkamcp", License: "Apache-2.0", Category: "monitoring", Tags: []string{"metrics", "prometheus", "datadog"}, Rating: 4.8, Verified: true, Type: "native", PublishedAt: "2025-01-15T00:00:00Z"},
		{Name: "schema-validator", Version: "1.2.0", Description: "Advanced schema validation with custom rules and policy enforcement", Author: "kafkamcp", License: "Apache-2.0", Category: "security", Tags: []string{"schema", "validation", "governance"}, Rating: 4.5, Verified: true, Type: "native", PublishedAt: "2025-02-01T00:00:00Z"},
		{Name: "kafka-cost-optimizer", Version: "0.9.0", Description: "Automated cost optimization recommendations and waste detection", Author: "community", License: "MIT", Category: "analytics", Tags: []string{"cost", "optimization", "finops"}, Rating: 4.2, Verified: false, Type: "native", PublishedAt: "2025-03-10T00:00:00Z"},
		{Name: "slack-notifier", Version: "1.1.0", Description: "Send Kafka alerts and notifications to Slack channels", Author: "kafkamcp", License: "Apache-2.0", Category: "integration", Tags: []string{"slack", "notifications", "alerts"}, Rating: 4.6, Verified: true, Type: "grpc", PublishedAt: "2025-01-20T00:00:00Z"},
		{Name: "kafka-encrypt", Version: "0.8.0", Description: "Field-level encryption/decryption for sensitive Kafka messages", Author: "community", License: "MIT", Category: "security", Tags: []string{"encryption", "security", "pii"}, Rating: 4.0, Verified: false, Type: "wasm", PublishedAt: "2025-04-01T00:00:00Z"},
		{Name: "topic-archiver", Version: "1.0.0", Description: "Archive old Kafka topics to S3/GCS/Azure Blob with retention policies", Author: "kafkamcp", License: "Apache-2.0", Category: "utility", Tags: []string{"archive", "storage", "retention"}, Rating: 4.4, Verified: true, Type: "native", PublishedAt: "2025-02-15T00:00:00Z"},
		{Name: "kafka-ml-pipeline", Version: "0.5.0", Description: "ML model serving and feature store integration for Kafka streams", Author: "community", License: "Apache-2.0", Category: "analytics", Tags: []string{"ml", "ai", "pipeline"}, Rating: 3.8, Verified: false, Type: "grpc", PublishedAt: "2025-04-20T00:00:00Z"},
		{Name: "compliance-auditor", Version: "1.0.0", Description: "GDPR/CCPA/HIPAA compliance auditing for Kafka data flows", Author: "kafkamcp", License: "Apache-2.0", Category: "security", Tags: []string{"compliance", "gdpr", "ccpa", "hipaa"}, Rating: 4.7, Verified: true, Type: "native", PublishedAt: "2025-03-01T00:00:00Z"},
	}
	for i := range builtins {
		m.catalog[builtins[i].Name] = &builtins[i]
	}
}

// Search finds plugins matching a query string against name, description, tags, and category.
func (m *Marketplace) Search(query, category string, verifiedOnly bool) []MarketplaceEntry {
	m.mu.RLock()
	query = strings.ToLower(query)
	var results []MarketplaceEntry

	for _, entry := range m.catalog {
		if verifiedOnly && !entry.Verified {
			continue
		}
		if category != "" && entry.Category != category {
			continue
		}

		if query == "" {
			results = append(results, *entry)
			continue
		}

		// Match against name, description, tags
		if strings.Contains(strings.ToLower(entry.Name), query) ||
			strings.Contains(strings.ToLower(entry.Description), query) ||
			m.matchTags(entry.Tags, query) {
			results = append(results, *entry)
		}
	}
	m.mu.RUnlock()

	// Enrich install status outside of marketplace lock to avoid nested locking
	for i := range results {
		results[i] = m.enrichInstallStatus(results[i])
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Rating > results[j].Rating
	})

	return results
}

func (m *Marketplace) matchTags(tags []string, query string) bool {
	for _, t := range tags {
		if strings.Contains(strings.ToLower(t), query) {
			return true
		}
	}
	return false
}

func (m *Marketplace) enrichInstallStatus(entry MarketplaceEntry) MarketplaceEntry {
	if info, err := m.registry.Get(entry.Name); err == nil {
		entry.Installed = true
		entry.InstalledVer = info.Version
	}
	return entry
}

// Install simulates installing a plugin from the marketplace.
func (m *Marketplace) Install(name string) (*MarketplaceEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry, ok := m.catalog[name]
	if !ok {
		return nil, fmt.Errorf("plugin %q not found in marketplace", name)
	}

	// Check dependencies (registry.Get uses its own lock, safe since we hold marketplace Write lock)
	for _, dep := range entry.Dependencies {
		if _, err := m.registry.Get(dep); err != nil {
			return nil, fmt.Errorf("missing dependency %q — install it first", dep)
		}
	}

	entry.Downloads++
	entry.Installed = true
	entry.InstalledVer = entry.Version

	result := *entry // return a copy
	return &result, nil
}

// GetEntry returns a marketplace entry by name.
func (m *Marketplace) GetEntry(name string) (*MarketplaceEntry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entry, ok := m.catalog[name]
	if !ok {
		return nil, fmt.Errorf("plugin %q not found in marketplace", name)
	}
	enriched := m.enrichInstallStatus(*entry)
	return &enriched, nil
}

// Publish adds or updates a plugin in the marketplace catalog.
func (m *Marketplace) Publish(entry MarketplaceEntry) error {
	if entry.Name == "" {
		return fmt.Errorf("plugin name is required")
	}
	if entry.Version == "" {
		return fmt.Errorf("plugin version is required")
	}
	if entry.PublishedAt == "" {
		entry.PublishedAt = time.Now().UTC().Format(time.RFC3339)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.catalog[entry.Name] = &entry
	return nil
}
