// Package controlplane provides a multi-tenant SaaS control plane for managing
// KafkaMCP instances across organizations with centralized auth, billing, and lifecycle management.
package controlplane

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// InstanceStatus represents the lifecycle state of a KafkaMCP instance.
type InstanceStatus string

// StatusProvisioning and related values define constants for this package.
const (
	StatusProvisioning InstanceStatus = "provisioning"
	StatusRunning      InstanceStatus = "running"
	StatusStopped      InstanceStatus = "stopped"
	StatusError        InstanceStatus = "error"
	StatusTerminated   InstanceStatus = "terminated"
)

// Instance represents a managed KafkaMCP instance.
type Instance struct {
	ID           string         `json:"id"`
	OrgID        string         `json:"org_id"`
	Name         string         `json:"name"`
	Status       InstanceStatus `json:"status"`
	Tier         string         `json:"tier"`
	Region       string         `json:"region"`
	Endpoints    Endpoints      `json:"endpoints"`
	Config       InstanceConfig `json:"config"`
	CreatedAt    time.Time      `json:"created_at"`
	UpdatedAt    time.Time      `json:"updated_at"`
	LastHealthAt time.Time      `json:"last_health_at,omitempty"`
}

// Endpoints for accessing the instance.
type Endpoints struct {
	MCP     string `json:"mcp,omitempty"`
	Metrics string `json:"metrics,omitempty"`
	Health  string `json:"health,omitempty"`
}

// InstanceConfig is the per-instance configuration.
type InstanceConfig struct {
	BootstrapServers string `json:"bootstrap_servers"`
	SchemaRegistry   string `json:"schema_registry,omitempty"`
	Transport        string `json:"transport"`
	MaxToolsPerMin   int    `json:"max_tools_per_min"`
}

// Organization represents a tenant organization.
type Organization struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Plan      string    `json:"plan"`
	APIKeys   []APIKey  `json:"api_keys"`
	Members   []Member  `json:"members"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// APIKey for authenticating to the control plane.
type APIKey struct {
	ID        string    `json:"id"`
	Key       string    `json:"key"`
	Name      string    `json:"name"`
	Scopes    []string  `json:"scopes"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	Active    bool      `json:"active"`
}

// Member represents an org member.
type Member struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	Role   string `json:"role"`
}

// UsageRecord tracks billable usage.
type UsageRecord struct {
	OrgID      string    `json:"org_id"`
	InstanceID string    `json:"instance_id"`
	Metric     string    `json:"metric"`
	Value      float64   `json:"value"`
	Timestamp  time.Time `json:"timestamp"`
}

// UsageSummary aggregates usage for billing.
type UsageSummary struct {
	OrgID           string  `json:"org_id"`
	Period          string  `json:"period"`
	ToolCalls       int64   `json:"tool_calls"`
	MessagesRead    int64   `json:"messages_read"`
	MessagesWritten int64   `json:"messages_written"`
	InstanceHours   float64 `json:"instance_hours"`
	EstimatedCost   float64 `json:"estimated_cost_usd"`
}

// Manager manages the multi-tenant control plane.
type Manager struct {
	mu        sync.RWMutex
	orgs      map[string]*Organization
	instances map[string]*Instance
	usage     map[string][]UsageRecord
	apiKeys   map[string]string // key -> orgID lookup
	logger    *slog.Logger
}

// NewManager creates a new control plane manager.
func NewManager(logger *slog.Logger) *Manager {
	return &Manager{
		orgs:      make(map[string]*Organization),
		instances: make(map[string]*Instance),
		usage:     make(map[string][]UsageRecord),
		apiKeys:   make(map[string]string),
		logger:    logger,
	}
}

// CreateOrganization registers a new tenant organization.
func (m *Manager) CreateOrganization(name, plan string) (*Organization, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("organization name is required")
	}

	randPart, err := generateID(8)
	if err != nil {
		return nil, fmt.Errorf("generate organization ID: %w", err)
	}
	id := fmt.Sprintf("org_%s", randPart)
	org := &Organization{
		ID:        id,
		Name:      name,
		Plan:      plan,
		APIKeys:   make([]APIKey, 0),
		Members:   make([]Member, 0),
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	m.orgs[id] = org
	return org, nil
}

// GetOrganization retrieves an organization by ID.
func (m *Manager) GetOrganization(orgID string) (*Organization, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	org, ok := m.orgs[orgID]
	if !ok {
		return nil, fmt.Errorf("organization %q not found", orgID)
	}
	return org, nil
}

// ListOrganizations returns all organizations.
func (m *Manager) ListOrganizations() []*Organization {
	m.mu.RLock()
	defer m.mu.RUnlock()

	orgs := make([]*Organization, 0, len(m.orgs))
	for _, org := range m.orgs {
		orgs = append(orgs, org)
	}
	return orgs
}

// CreateAPIKey generates a new API key for an organization.
func (m *Manager) CreateAPIKey(orgID, name string, scopes []string) (*APIKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	org, ok := m.orgs[orgID]
	if !ok {
		return nil, fmt.Errorf("organization %q not found", orgID)
	}

	keyIDPart, err := generateID(8)
	if err != nil {
		return nil, fmt.Errorf("generate API key ID: %w", err)
	}
	keyPart, err := generateID(32)
	if err != nil {
		return nil, fmt.Errorf("generate API key: %w", err)
	}

	key := &APIKey{
		ID:        fmt.Sprintf("key_%s", keyIDPart),
		Key:       fmt.Sprintf("kmcp_%s", keyPart),
		Name:      name,
		Scopes:    scopes,
		CreatedAt: time.Now(),
		Active:    true,
	}

	org.APIKeys = append(org.APIKeys, *key)
	org.UpdatedAt = time.Now()
	m.apiKeys[key.Key] = orgID

	return key, nil
}

// ValidateAPIKey checks if an API key is valid and returns the org ID.
func (m *Manager) ValidateAPIKey(key string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	orgID, ok := m.apiKeys[key]
	if !ok {
		return "", fmt.Errorf("invalid API key")
	}
	return orgID, nil
}

// ProvisionInstance creates a new KafkaMCP instance for an organization.
func (m *Manager) ProvisionInstance(orgID, name, tier, region string, cfg InstanceConfig) (*Instance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.orgs[orgID]; !ok {
		return nil, fmt.Errorf("organization %q not found", orgID)
	}

	if name == "" {
		return nil, fmt.Errorf("instance name is required")
	}
	if cfg.BootstrapServers == "" {
		return nil, fmt.Errorf("bootstrap_servers is required")
	}
	if tier == "" {
		tier = "standard"
	}
	if region == "" {
		region = "us-east-1"
	}
	if cfg.Transport == "" {
		cfg.Transport = "streamable-http"
	}
	if cfg.MaxToolsPerMin == 0 {
		cfg.MaxToolsPerMin = 1000
	}

	instIDPart, err := generateID(8)
	if err != nil {
		return nil, fmt.Errorf("generate instance ID: %w", err)
	}
	id := fmt.Sprintf("inst_%s", instIDPart)
	inst := &Instance{
		ID:        id,
		OrgID:     orgID,
		Name:      name,
		Status:    StatusProvisioning,
		Tier:      tier,
		Region:    region,
		Config:    cfg,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	m.instances[id] = inst

	// Simulate async provisioning
	go func() {
		time.Sleep(100 * time.Millisecond)
		m.mu.Lock()
		defer m.mu.Unlock()
		if i, ok := m.instances[id]; ok && i.Status == StatusProvisioning {
			i.Status = StatusRunning
			i.Endpoints = Endpoints{
				MCP:     fmt.Sprintf("https://%s.%s.kafkamcp.io/mcp", id, region),
				Metrics: fmt.Sprintf("https://%s.%s.kafkamcp.io/metrics", id, region),
				Health:  fmt.Sprintf("https://%s.%s.kafkamcp.io/healthz", id, region),
			}
			i.UpdatedAt = time.Now()
			i.LastHealthAt = time.Now()
		}
	}()

	// Return a defensive snapshot so callers can read fields without racing
	// with the asynchronous provisioning goroutine above.
	snapshot := *inst
	return &snapshot, nil
}

// GetInstance retrieves an instance. The returned pointer references a copy of
// the instance state captured under the manager lock, so callers may safely
// read its fields without coordinating with concurrent provisioning goroutines.
func (m *Manager) GetInstance(instanceID string) (*Instance, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	inst, ok := m.instances[instanceID]
	if !ok {
		return nil, fmt.Errorf("instance %q not found", instanceID)
	}
	snapshot := *inst
	return &snapshot, nil
}

// ListInstances returns instances for an organization. Each returned pointer is
// a defensive copy of the underlying instance.
func (m *Manager) ListInstances(orgID string) []*Instance {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*Instance
	for _, inst := range m.instances {
		if orgID == "" || inst.OrgID == orgID {
			snapshot := *inst
			result = append(result, &snapshot)
		}
	}
	return result
}

// StopInstance stops a running instance.
func (m *Manager) StopInstance(instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, ok := m.instances[instanceID]
	if !ok {
		return fmt.Errorf("instance %q not found", instanceID)
	}
	if inst.Status != StatusRunning {
		return fmt.Errorf("instance %q is not running (status: %s)", instanceID, inst.Status)
	}
	inst.Status = StatusStopped
	inst.UpdatedAt = time.Now()
	return nil
}

// StartInstance starts a stopped instance.
func (m *Manager) StartInstance(instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, ok := m.instances[instanceID]
	if !ok {
		return fmt.Errorf("instance %q not found", instanceID)
	}
	if inst.Status != StatusStopped {
		return fmt.Errorf("instance %q is not stopped (status: %s)", instanceID, inst.Status)
	}
	inst.Status = StatusRunning
	inst.UpdatedAt = time.Now()
	inst.LastHealthAt = time.Now()
	return nil
}

// TerminateInstance permanently removes an instance.
func (m *Manager) TerminateInstance(instanceID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	inst, ok := m.instances[instanceID]
	if !ok {
		return fmt.Errorf("instance %q not found", instanceID)
	}
	inst.Status = StatusTerminated
	inst.UpdatedAt = time.Now()
	return nil
}

// RecordUsage records a usage metric for billing.
func (m *Manager) RecordUsage(orgID, instanceID, metric string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	record := UsageRecord{
		OrgID:      orgID,
		InstanceID: instanceID,
		Metric:     metric,
		Value:      value,
		Timestamp:  time.Now(),
	}
	m.usage[orgID] = append(m.usage[orgID], record)
}

// GetUsageSummary returns aggregated usage for billing.
func (m *Manager) GetUsageSummary(orgID, period string) *UsageSummary {
	m.mu.RLock()
	defer m.mu.RUnlock()

	summary := &UsageSummary{
		OrgID:  orgID,
		Period: period,
	}

	records := m.usage[orgID]
	for _, r := range records {
		switch r.Metric {
		case "tool_calls":
			summary.ToolCalls += int64(r.Value)
		case "messages_read":
			summary.MessagesRead += int64(r.Value)
		case "messages_written":
			summary.MessagesWritten += int64(r.Value)
		case "instance_hours":
			summary.InstanceHours += r.Value
		}
	}

	// Simple pricing model
	summary.EstimatedCost = float64(summary.ToolCalls)*0.001 +
		float64(summary.MessagesRead)*0.0001 +
		float64(summary.MessagesWritten)*0.0002 +
		summary.InstanceHours*0.10

	return summary
}

// AddMember adds a member to an organization.
func (m *Manager) AddMember(orgID, userID, email, role string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	org, ok := m.orgs[orgID]
	if !ok {
		return fmt.Errorf("organization %q not found", orgID)
	}

	org.Members = append(org.Members, Member{
		UserID: userID,
		Email:  email,
		Role:   role,
	})
	org.UpdatedAt = time.Now()
	return nil
}

func generateID(length int) (string, error) {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b)[:length], nil
}
