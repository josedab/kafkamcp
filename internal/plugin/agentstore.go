package plugin

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// AgentConfig represents a shareable agent configuration.
type AgentConfig struct {
	Name        string          `json:"name"`
	Version     string          `json:"version"`
	Description string          `json:"description"`
	Author      string          `json:"author"`
	Category    string          `json:"category"` // "monitoring", "operations", "analytics", "pipeline", "security"
	Tags        []string        `json:"tags"`
	Tools       []string        `json:"tools"`
	Prompts     []AgentPrompt   `json:"prompts,omitempty"`
	Workflows   []AgentWorkflow `json:"workflows,omitempty"`
	Config      map[string]any  `json:"config,omitempty"`
	Downloads   int             `json:"downloads"`
	Rating      float64         `json:"rating"`
	Verified    bool            `json:"verified"`
	PublishedAt string          `json:"published_at"`
}

// AgentPrompt is a reusable prompt template in an agent configuration.
type AgentPrompt struct {
	Name        string `json:"name"`
	Template    string `json:"template"`
	Description string `json:"description,omitempty"`
}

// AgentWorkflow is a pre-built workflow in an agent configuration.
type AgentWorkflow struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Steps       []string `json:"steps"`
}

// ToolBundle groups related tools into an installable bundle.
type ToolBundle struct {
	Name        string   `json:"name"`
	Version     string   `json:"version"`
	Description string   `json:"description"`
	Author      string   `json:"author"`
	Tools       []string `json:"tools"`
	Plugins     []string `json:"plugins,omitempty"`
	Category    string   `json:"category"`
	Tags        []string `json:"tags"`
	Downloads   int      `json:"downloads"`
	Rating      float64  `json:"rating"`
	Verified    bool     `json:"verified"`
	PublishedAt string   `json:"published_at"`
}

// AgentConfigStore manages shareable agent configurations and tool bundles.
type AgentConfigStore struct {
	mu      sync.RWMutex
	configs map[string]*AgentConfig
	bundles map[string]*ToolBundle
}

// NewAgentConfigStore creates a new agent config store with built-in entries.
func NewAgentConfigStore() *AgentConfigStore {
	s := &AgentConfigStore{
		configs: make(map[string]*AgentConfig),
		bundles: make(map[string]*ToolBundle),
	}
	s.seedDefaults()
	return s
}

func (s *AgentConfigStore) seedDefaults() {
	defaultConfigs := []AgentConfig{
		{
			Name: "kafka-ops-agent", Version: "1.0.0",
			Description: "Production Kafka operations agent with monitoring, alerting, and remediation capabilities",
			Author:      "kafkamcp", Category: "operations",
			Tags:  []string{"ops", "monitoring", "production"},
			Tools: []string{"kafka_list_topics", "kafka_describe_topic", "kafka_consumer_groups", "kafka_cluster_info", "kafka_anomaly_detect"},
			Prompts: []AgentPrompt{
				{Name: "health-check", Template: "Check the health of all Kafka clusters and report any issues", Description: "Cluster health check"},
				{Name: "lag-alert", Template: "Check consumer lag for all groups and alert if any exceed {threshold}", Description: "Consumer lag monitoring"},
			},
			Workflows: []AgentWorkflow{
				{Name: "daily-health", Description: "Daily cluster health check", Steps: []string{"kafka_cluster_info", "kafka_list_topics", "kafka_consumer_groups", "kafka_anomaly_detect"}},
			},
			Rating: 4.8, Verified: true, PublishedAt: "2025-01-15T00:00:00Z",
		},
		{
			Name: "schema-governance-agent", Version: "1.0.0",
			Description: "Schema governance agent for managing schema evolution, compatibility, and compliance",
			Author:      "kafkamcp", Category: "security",
			Tags:   []string{"schema", "governance", "compliance"},
			Tools:  []string{"kafka_schema_list", "kafka_schema_get", "kafka_check_compatibility", "kafka_schema_evolution_plan"},
			Rating: 4.5, Verified: true, PublishedAt: "2025-02-01T00:00:00Z",
		},
		{
			Name: "data-pipeline-agent", Version: "1.0.0",
			Description: "Agent for building and managing data pipelines with natural language",
			Author:      "kafkamcp", Category: "pipeline",
			Tags:   []string{"pipeline", "etl", "streaming"},
			Tools:  []string{"kafka_pipeline_create", "kafka_pipeline_run", "kafka_pipeline_list", "kafka_nl_query"},
			Rating: 4.3, Verified: true, PublishedAt: "2025-03-01T00:00:00Z",
		},
		{
			Name: "cost-optimizer-agent", Version: "0.9.0",
			Description: "Agent for analyzing Kafka costs and providing optimization recommendations",
			Author:      "community", Category: "analytics",
			Tags:   []string{"cost", "finops", "optimization"},
			Tools:  []string{"kafka_cost_analyze", "kafka_cost_waste_detect", "kafka_cost_whatif"},
			Rating: 4.1, Verified: false, PublishedAt: "2025-04-01T00:00:00Z",
		},
	}
	for i := range defaultConfigs {
		s.configs[defaultConfigs[i].Name] = &defaultConfigs[i]
	}

	defaultBundles := []ToolBundle{
		{
			Name: "monitoring-essentials", Version: "1.0.0",
			Description: "Essential monitoring tools: cluster health, consumer lag, anomaly detection, and alerting",
			Author:      "kafkamcp", Category: "monitoring",
			Tools: []string{"kafka_cluster_info", "kafka_list_topics", "kafka_consumer_groups", "kafka_anomaly_detect", "kafka_consumer_lag_analyze"},
			Tags:  []string{"monitoring", "health", "alerting"}, Rating: 4.7, Verified: true, PublishedAt: "2025-01-20T00:00:00Z",
		},
		{
			Name: "security-toolkit", Version: "1.0.0",
			Description: "Security and governance tools: ACLs, masking, compliance, and audit",
			Author:      "kafkamcp", Category: "security",
			Tools: []string{"kafka_manage_acls", "kafka_mask_message", "kafka_compliance_report", "kafka_audit_trail"},
			Tags:  []string{"security", "acl", "compliance"}, Rating: 4.6, Verified: true, PublishedAt: "2025-02-15T00:00:00Z",
		},
		{
			Name: "developer-toolkit", Version: "1.0.0",
			Description: "Developer tools: topic CRUD, message produce/consume, schema management",
			Author:      "kafkamcp", Category: "development",
			Tools: []string{"kafka_create_topic", "kafka_produce", "kafka_consume", "kafka_schema_list", "kafka_schema_get"},
			Tags:  []string{"development", "testing", "debugging"}, Rating: 4.5, Verified: true, PublishedAt: "2025-03-10T00:00:00Z",
		},
	}
	for i := range defaultBundles {
		s.bundles[defaultBundles[i].Name] = &defaultBundles[i]
	}
}

// SearchConfigs searches agent configurations by query, category, and verified status.
func (s *AgentConfigStore) SearchConfigs(query, category string, verifiedOnly bool) []AgentConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query = strings.ToLower(query)
	var results []AgentConfig
	for _, cfg := range s.configs {
		if verifiedOnly && !cfg.Verified {
			continue
		}
		if category != "" && cfg.Category != category {
			continue
		}
		if query == "" || strings.Contains(strings.ToLower(cfg.Name), query) ||
			strings.Contains(strings.ToLower(cfg.Description), query) ||
			matchTagSlice(cfg.Tags, query) {
			results = append(results, *cfg)
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Rating > results[j].Rating })
	return results
}

// GetConfig returns an agent configuration by name.
func (s *AgentConfigStore) GetConfig(name string) (*AgentConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cfg, ok := s.configs[name]
	if !ok {
		return nil, fmt.Errorf("agent config %q not found", name)
	}
	result := *cfg
	return &result, nil
}

// PublishConfig adds or updates an agent configuration.
func (s *AgentConfigStore) PublishConfig(cfg AgentConfig) error {
	if cfg.Name == "" {
		return fmt.Errorf("agent config name is required")
	}
	if cfg.Version == "" {
		return fmt.Errorf("agent config version is required")
	}
	if cfg.PublishedAt == "" {
		cfg.PublishedAt = time.Now().UTC().Format(time.RFC3339)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs[cfg.Name] = &cfg
	return nil
}

// SearchBundles searches tool bundles.
func (s *AgentConfigStore) SearchBundles(query, category string) []ToolBundle {
	s.mu.RLock()
	defer s.mu.RUnlock()

	query = strings.ToLower(query)
	var results []ToolBundle
	for _, b := range s.bundles {
		if category != "" && b.Category != category {
			continue
		}
		if query == "" || strings.Contains(strings.ToLower(b.Name), query) ||
			strings.Contains(strings.ToLower(b.Description), query) ||
			matchTagSlice(b.Tags, query) {
			results = append(results, *b)
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Rating > results[j].Rating })
	return results
}

// GetBundle returns a tool bundle by name.
func (s *AgentConfigStore) GetBundle(name string) (*ToolBundle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.bundles[name]
	if !ok {
		return nil, fmt.Errorf("tool bundle %q not found", name)
	}
	result := *b
	return &result, nil
}

// PublishBundle adds or updates a tool bundle.
func (s *AgentConfigStore) PublishBundle(bundle ToolBundle) error {
	if bundle.Name == "" {
		return fmt.Errorf("bundle name is required")
	}
	if bundle.Version == "" {
		return fmt.Errorf("bundle version is required")
	}
	if len(bundle.Tools) == 0 {
		return fmt.Errorf("bundle must contain at least one tool")
	}
	if bundle.PublishedAt == "" {
		bundle.PublishedAt = time.Now().UTC().Format(time.RFC3339)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bundles[bundle.Name] = &bundle
	return nil
}

func matchTagSlice(tags []string, query string) bool {
	for _, t := range tags {
		if strings.Contains(strings.ToLower(t), query) {
			return true
		}
	}
	return false
}
