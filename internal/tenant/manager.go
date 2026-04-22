package tenant

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Tenant represents a tenant in the multi-tenant system.
type Tenant struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Email       string            `json:"email"`
	Plan        string            `json:"plan"` // "free", "pro", "enterprise"
	APIKeys     []APIKey          `json:"api_keys"`
	Clusters    []TenantCluster   `json:"clusters"`
	Usage       UsageMetrics      `json:"usage"`
	Limits      TenantLimits      `json:"limits"`
	CreatedAt   time.Time         `json:"created_at"`
	Status      string            `json:"status"` // "active", "suspended", "deleted"
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// APIKey represents a tenant API key.
type APIKey struct {
	ID        string    `json:"id"`
	KeyHash   string    `json:"-"` // stored hashed
	Prefix    string    `json:"prefix"` // first 8 chars for identification
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	LastUsed  time.Time `json:"last_used,omitempty"`
	Active    bool      `json:"active"`
}

// TenantCluster represents a tenant's Kafka cluster registration.
type TenantCluster struct {
	Name             string `json:"name"`
	BootstrapServers string `json:"bootstrap_servers"`
	SecurityProtocol string `json:"security_protocol"`
	Region           string `json:"region,omitempty"`
}

// UsageMetrics tracks tenant usage for billing.
type UsageMetrics struct {
	ToolCalls        int64     `json:"tool_calls"`
	MessagesConsumed int64     `json:"messages_consumed"`
	MessagesProduced int64     `json:"messages_produced"`
	BytesTransferred int64     `json:"bytes_transferred"`
	ActiveAgents     int       `json:"active_agents"`
	PeriodStart      time.Time `json:"period_start"`
}

// TenantLimits defines usage limits per plan.
type TenantLimits struct {
	MaxToolCallsPerMonth int64 `json:"max_tool_calls_per_month"`
	MaxClusters          int   `json:"max_clusters"`
	MaxAgents            int   `json:"max_agents"`
	MaxMessagesPerDay    int64 `json:"max_messages_per_day"`
	RateLimit            int   `json:"rate_limit_rpm"`
}

// PlanLimits returns default limits for a plan tier.
func PlanLimits(plan string) TenantLimits {
	switch plan {
	case "free":
		return TenantLimits{
			MaxToolCallsPerMonth: 10000,
			MaxClusters:          1,
			MaxAgents:            2,
			MaxMessagesPerDay:    1000,
			RateLimit:            60,
		}
	case "pro":
		return TenantLimits{
			MaxToolCallsPerMonth: 1000000,
			MaxClusters:          5,
			MaxAgents:            20,
			MaxMessagesPerDay:    100000,
			RateLimit:            600,
		}
	case "enterprise":
		return TenantLimits{
			MaxToolCallsPerMonth: -1, // unlimited
			MaxClusters:          -1,
			MaxAgents:            -1,
			MaxMessagesPerDay:    -1,
			RateLimit:            6000,
		}
	default:
		return PlanLimits("free")
	}
}

// Manager handles tenant lifecycle and isolation.
type Manager struct {
	mu      sync.RWMutex
	tenants map[string]*Tenant
	byKey   map[string]string // key prefix -> tenant ID
}

// NewManager creates a new tenant manager.
func NewManager() *Manager {
	return &Manager{
		tenants: make(map[string]*Tenant),
		byKey:   make(map[string]string),
	}
}

// CreateTenant registers a new tenant.
func (m *Manager) CreateTenant(name, email, plan string) (*Tenant, string, error) {
	if name == "" || email == "" {
		return nil, "", fmt.Errorf("name and email are required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check for duplicate email
	for _, t := range m.tenants {
		if t.Email == email {
			return nil, "", fmt.Errorf("tenant with email %q already exists", email)
		}
	}

	id := generateID("tenant")
	if plan == "" {
		plan = "free"
	}

	apiKey, keyPlaintext, err := generateAPIKey(name)
	if err != nil {
		return nil, "", fmt.Errorf("generating API key: %w", err)
	}

	tenant := &Tenant{
		ID:        id,
		Name:      name,
		Email:     email,
		Plan:      plan,
		APIKeys:   []APIKey{apiKey},
		Limits:    PlanLimits(plan),
		CreatedAt: time.Now(),
		Status:    "active",
		Usage: UsageMetrics{
			PeriodStart: time.Now().Truncate(24 * time.Hour),
		},
	}

	m.tenants[id] = tenant
	m.byKey[apiKey.Prefix] = id

	return tenant, keyPlaintext, nil
}

// GetTenant returns a tenant by ID.
func (m *Manager) GetTenant(id string) (*Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, ok := m.tenants[id]
	if !ok {
		return nil, fmt.Errorf("tenant %q not found", id)
	}
	return t, nil
}

// AuthenticateByKey resolves a tenant from an API key prefix.
func (m *Manager) AuthenticateByKey(keyPrefix string) (*Tenant, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tenantID, ok := m.byKey[keyPrefix]
	if !ok {
		return nil, fmt.Errorf("invalid API key")
	}

	t, ok := m.tenants[tenantID]
	if !ok {
		return nil, fmt.Errorf("tenant not found")
	}
	if t.Status != "active" {
		return nil, fmt.Errorf("tenant account is %s", t.Status)
	}

	return t, nil
}

// RecordUsage increments usage counters for a tenant.
func (m *Manager) RecordUsage(tenantID string, toolCalls, messagesConsumed, messagesProduced, bytes int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.tenants[tenantID]
	if !ok {
		return fmt.Errorf("tenant %q not found", tenantID)
	}

	t.Usage.ToolCalls += toolCalls
	t.Usage.MessagesConsumed += messagesConsumed
	t.Usage.MessagesProduced += messagesProduced
	t.Usage.BytesTransferred += bytes

	return nil
}

// CheckLimits verifies a tenant hasn't exceeded their plan limits.
func (m *Manager) CheckLimits(tenantID string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, ok := m.tenants[tenantID]
	if !ok {
		return fmt.Errorf("tenant %q not found", tenantID)
	}

	if t.Limits.MaxToolCallsPerMonth > 0 && t.Usage.ToolCalls >= t.Limits.MaxToolCallsPerMonth {
		return fmt.Errorf("monthly tool call limit exceeded (%d/%d)", t.Usage.ToolCalls, t.Limits.MaxToolCallsPerMonth)
	}

	return nil
}

// ListTenants returns all tenants.
func (m *Manager) ListTenants() []*Tenant {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*Tenant, 0, len(m.tenants))
	for _, t := range m.tenants {
		result = append(result, t)
	}
	return result
}

// AddCluster registers a Kafka cluster for a tenant.
func (m *Manager) AddCluster(tenantID string, cluster TenantCluster) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.tenants[tenantID]
	if !ok {
		return fmt.Errorf("tenant %q not found", tenantID)
	}

	if t.Limits.MaxClusters > 0 && len(t.Clusters) >= t.Limits.MaxClusters {
		return fmt.Errorf("cluster limit exceeded (%d/%d)", len(t.Clusters), t.Limits.MaxClusters)
	}

	t.Clusters = append(t.Clusters, cluster)
	return nil
}

func generateID(prefix string) string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// Fallback: use timestamp-based ID if crypto/rand fails
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s_%s", prefix, hex.EncodeToString(b))
}

func generateAPIKey(name string) (APIKey, string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return APIKey{}, "", err
	}

	keyPlaintext := fmt.Sprintf("kmcp_%s", hex.EncodeToString(b))
	prefix := keyPlaintext[:12]

	return APIKey{
		ID:        generateID("key"),
		Prefix:    prefix,
		Name:      fmt.Sprintf("%s-default", name),
		CreatedAt: time.Now(),
		Active:    true,
	}, keyPlaintext, nil
}
