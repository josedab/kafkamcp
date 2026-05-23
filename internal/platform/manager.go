package platform

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// PlatformType identifies the Kafka-compatible platform.
type PlatformType string

// PlatformKafka and related values define constants for this package.
const (
	PlatformKafka          PlatformType = "kafka"
	PlatformRedpanda       PlatformType = "redpanda"
	PlatformConfluentCloud PlatformType = "confluent_cloud"
	PlatformMSKServerless  PlatformType = "msk_serverless"
)

// Capability flags for platform feature support.
type Capability string

// CapTopicCRUD and related values define constants for this package.
const (
	CapTopicCRUD         Capability = "topic_crud"
	CapConsumerGroups    Capability = "consumer_groups"
	CapACLs              Capability = "acls"
	CapSchemaRegistry    Capability = "schema_registry"
	CapTransactions      Capability = "transactions"
	CapAdminAPI          Capability = "admin_api"
	CapConnect           Capability = "connect"
	CapIdempotentProduce Capability = "idempotent_produce"
	CapExactlyOnce       Capability = "exactly_once"
	CapTieredStorage     Capability = "tiered_storage"
)

// PlatformConfig holds platform-specific configuration.
type PlatformConfig struct {
	Type         PlatformType      `json:"type" yaml:"type"`
	Capabilities []Capability      `json:"capabilities" yaml:"capabilities"`
	AdminURL     string            `json:"admin_url,omitempty" yaml:"admin_url,omitempty"`
	RESTURL      string            `json:"rest_url,omitempty" yaml:"rest_url,omitempty"`
	Properties   map[string]string `json:"properties,omitempty" yaml:"properties,omitempty"`
}

// PlatformInfo describes a platform's capabilities and version.
type PlatformInfo struct {
	Type         PlatformType   `json:"type"`
	Version      string         `json:"version"`
	Capabilities []Capability   `json:"capabilities"`
	Features     map[string]any `json:"features,omitempty"`
}

// Backend defines the interface for platform-specific operations.
type Backend interface {
	Type() PlatformType
	Capabilities() []Capability
	HasCapability(cap Capability) bool
	Info(ctx context.Context) (*PlatformInfo, error)
}

// KafkaBackend implements Backend for standard Apache Kafka.
type KafkaBackend struct {
	config PlatformConfig
}

// NewKafkaBackend creates a standard Kafka backend.
func NewKafkaBackend(cfg PlatformConfig) *KafkaBackend {
	return &KafkaBackend{config: cfg}
}

// Type is auto-documented.
func (b *KafkaBackend) Type() PlatformType { return PlatformKafka }

// Capabilities is auto-documented.
func (b *KafkaBackend) Capabilities() []Capability {
	return []Capability{
		CapTopicCRUD, CapConsumerGroups, CapACLs, CapSchemaRegistry,
		CapTransactions, CapAdminAPI, CapConnect, CapIdempotentProduce,
		CapExactlyOnce,
	}
}

// HasCapability is auto-documented.
func (b *KafkaBackend) HasCapability(cap Capability) bool {
	for _, c := range b.Capabilities() {
		if c == cap {
			return true
		}
	}
	return false
}

// Info is auto-documented.
func (b *KafkaBackend) Info(ctx context.Context) (*PlatformInfo, error) {
	return &PlatformInfo{
		Type:         PlatformKafka,
		Version:      "3.x",
		Capabilities: b.Capabilities(),
		Features:     map[string]any{"kraft": true, "tiered_storage": false},
	}, nil
}

// RedpandaBackend implements Backend for Redpanda.
type RedpandaBackend struct {
	config   PlatformConfig
	adminURL string
}

// NewRedpandaBackend creates a Redpanda backend.
func NewRedpandaBackend(cfg PlatformConfig) *RedpandaBackend {
	return &RedpandaBackend{config: cfg, adminURL: cfg.AdminURL}
}

// Type is auto-documented.
func (b *RedpandaBackend) Type() PlatformType { return PlatformRedpanda }

// Capabilities is auto-documented.
func (b *RedpandaBackend) Capabilities() []Capability {
	return []Capability{
		CapTopicCRUD, CapConsumerGroups, CapACLs, CapSchemaRegistry,
		CapTransactions, CapAdminAPI, CapIdempotentProduce,
		CapExactlyOnce,
	}
}

// HasCapability is auto-documented.
func (b *RedpandaBackend) HasCapability(cap Capability) bool {
	for _, c := range b.Capabilities() {
		if c == cap {
			return true
		}
	}
	return false
}

// Info is auto-documented.
func (b *RedpandaBackend) Info(ctx context.Context) (*PlatformInfo, error) {
	return &PlatformInfo{
		Type:         PlatformRedpanda,
		Version:      "latest",
		Capabilities: b.Capabilities(),
		Features: map[string]any{
			"admin_api":       b.adminURL != "",
			"schema_registry": true,
			"wasm_transforms": true,
		},
	}, nil
}

// ConfluentCloudBackend implements Backend for Confluent Cloud.
type ConfluentCloudBackend struct {
	config  PlatformConfig
	restURL string
}

// NewConfluentCloudBackend creates a Confluent Cloud backend.
func NewConfluentCloudBackend(cfg PlatformConfig) *ConfluentCloudBackend {
	return &ConfluentCloudBackend{config: cfg, restURL: cfg.RESTURL}
}

// Type is auto-documented.
func (b *ConfluentCloudBackend) Type() PlatformType { return PlatformConfluentCloud }

// Capabilities is auto-documented.
func (b *ConfluentCloudBackend) Capabilities() []Capability {
	return []Capability{
		CapTopicCRUD, CapConsumerGroups, CapSchemaRegistry,
		CapTransactions, CapIdempotentProduce, CapExactlyOnce,
		CapTieredStorage,
	}
}

// HasCapability is auto-documented.
func (b *ConfluentCloudBackend) HasCapability(cap Capability) bool {
	for _, c := range b.Capabilities() {
		if c == cap {
			return true
		}
	}
	return false
}

// Info is auto-documented.
func (b *ConfluentCloudBackend) Info(ctx context.Context) (*PlatformInfo, error) {
	return &PlatformInfo{
		Type:         PlatformConfluentCloud,
		Version:      "managed",
		Capabilities: b.Capabilities(),
		Features: map[string]any{
			"rest_api":        b.restURL != "",
			"tiered_storage":  true,
			"cluster_linking": true,
		},
	}, nil
}

// Manager manages platform backends and routing.
type Manager struct {
	backends map[string]Backend
	mu       sync.RWMutex
	logger   *slog.Logger
}

// NewManager creates a new platform manager.
func NewManager(logger *slog.Logger) *Manager {
	return &Manager{
		backends: make(map[string]Backend),
		logger:   logger,
	}
}

// RegisterBackend registers a platform backend for a cluster.
func (m *Manager) RegisterBackend(clusterName string, backend Backend) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backends[clusterName] = backend
}

// GetBackend returns the backend for a cluster.
func (m *Manager) GetBackend(clusterName string) (Backend, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.backends[clusterName]
	if !ok {
		return nil, fmt.Errorf("no backend registered for cluster %q", clusterName)
	}
	return b, nil
}

// DetectPlatform auto-detects the platform type from cluster configuration.
func DetectPlatform(bootstrapServers string, properties map[string]string) PlatformType {
	// Heuristic detection
	if _, ok := properties["redpanda.admin.url"]; ok {
		return PlatformRedpanda
	}
	if _, ok := properties["confluent.cloud.rest.url"]; ok {
		return PlatformConfluentCloud
	}
	if _, ok := properties["aws.msk.serverless"]; ok {
		return PlatformMSKServerless
	}
	return PlatformKafka
}

// CreateBackend creates a backend based on platform type.
func CreateBackend(cfg PlatformConfig) Backend {
	switch cfg.Type {
	case PlatformRedpanda:
		return NewRedpandaBackend(cfg)
	case PlatformConfluentCloud:
		return NewConfluentCloudBackend(cfg)
	default:
		return NewKafkaBackend(cfg)
	}
}

// ListPlatforms returns info about all registered platforms.
func (m *Manager) ListPlatforms(ctx context.Context) []PlatformInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []PlatformInfo
	for _, b := range m.backends {
		if info, err := b.Info(ctx); err == nil {
			result = append(result, *info)
		}
	}
	return result
}

// CheckCapability checks if a cluster supports a capability.
func (m *Manager) CheckCapability(clusterName string, cap Capability) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.backends[clusterName]
	if !ok {
		return false
	}
	return b.HasCapability(cap)
}
