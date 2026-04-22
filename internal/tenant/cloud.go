package tenant

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// SSOConfig holds SSO/OIDC authentication configuration.
type SSOConfig struct {
	Provider     string `json:"provider" yaml:"provider"`         // "oidc", "saml", "github", "google"
	ClientID     string `json:"client_id" yaml:"client_id"`
	ClientSecret string `json:"-" yaml:"client_secret"`
	IssuerURL    string `json:"issuer_url" yaml:"issuer_url"`
	CallbackURL  string `json:"callback_url" yaml:"callback_url"`
	Scopes       []string `json:"scopes,omitempty" yaml:"scopes,omitempty"`
}

// BillingInfo holds tenant billing information.
type BillingInfo struct {
	StripeCustomerID   string    `json:"stripe_customer_id,omitempty"`
	StripeSubscriptionID string  `json:"stripe_subscription_id,omitempty"`
	PlanID             string    `json:"plan_id"`
	BillingEmail       string    `json:"billing_email"`
	CurrentPeriodStart time.Time `json:"current_period_start"`
	CurrentPeriodEnd   time.Time `json:"current_period_end"`
	Status             string    `json:"status"` // "active", "past_due", "cancelled"
}

// UsageBillingRecord records usage for billing purposes.
type UsageBillingRecord struct {
	TenantID         string    `json:"tenant_id"`
	PeriodStart      time.Time `json:"period_start"`
	PeriodEnd        time.Time `json:"period_end"`
	ToolCalls        int64     `json:"tool_calls"`
	MessagesConsumed int64     `json:"messages_consumed"`
	MessagesProduced int64     `json:"messages_produced"`
	BytesTransferred int64     `json:"bytes_transferred"`
	ComputedCostUSD  float64   `json:"computed_cost_usd"`
}

// ResourceQuota defines per-tenant resource quotas.
type ResourceQuota struct {
	MaxCPUMillicores    int   `json:"max_cpu_millicores" yaml:"max_cpu_millicores"`
	MaxMemoryMB         int   `json:"max_memory_mb" yaml:"max_memory_mb"`
	MaxConcurrentAgents int   `json:"max_concurrent_agents" yaml:"max_concurrent_agents"`
	MaxStorageMB        int64 `json:"max_storage_mb" yaml:"max_storage_mb"`
	MaxBandwidthMBps    int   `json:"max_bandwidth_mbps" yaml:"max_bandwidth_mbps"`
}

// RegionConfig defines a deployment region.
type RegionConfig struct {
	Name          string `json:"name" yaml:"name"`
	Provider      string `json:"provider" yaml:"provider"` // "aws", "gcp", "azure"
	Zone          string `json:"zone" yaml:"zone"`
	Endpoint      string `json:"endpoint" yaml:"endpoint"`
	IsActive      bool   `json:"is_active" yaml:"is_active"`
	IsPrimary     bool   `json:"is_primary,omitempty" yaml:"is_primary,omitempty"`
}

// NetworkIsolation defines network isolation for a tenant.
type NetworkIsolation struct {
	VPCEnabled     bool     `json:"vpc_enabled" yaml:"vpc_enabled"`
	AllowedCIDRs   []string `json:"allowed_cidrs,omitempty" yaml:"allowed_cidrs,omitempty"`
	PrivateLink    bool     `json:"private_link" yaml:"private_link"`
	TLSRequired    bool     `json:"tls_required" yaml:"tls_required"`
}

// ControlPlane manages the multi-tenant SaaS control plane.
type ControlPlane struct {
	mu          sync.RWMutex
	tenantMgr   *Manager
	regions     []RegionConfig
	billing     map[string]*BillingInfo
	quotas      map[string]*ResourceQuota
	ssoConfigs  map[string]*SSOConfig
	isolation   map[string]*NetworkIsolation
	usageRecords []UsageBillingRecord
}

// NewControlPlane creates a new control plane for the SaaS offering.
func NewControlPlane(tenantMgr *Manager) *ControlPlane {
	return &ControlPlane{
		tenantMgr:  tenantMgr,
		billing:    make(map[string]*BillingInfo),
		quotas:     make(map[string]*ResourceQuota),
		ssoConfigs: make(map[string]*SSOConfig),
		isolation:  make(map[string]*NetworkIsolation),
	}
}

// AddRegion registers a deployment region.
func (cp *ControlPlane) AddRegion(region RegionConfig) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.regions = append(cp.regions, region)
}

// ListRegions returns available deployment regions.
func (cp *ControlPlane) ListRegions() []RegionConfig {
	cp.mu.RLock()
	defer cp.mu.RUnlock()
	result := make([]RegionConfig, len(cp.regions))
	copy(result, cp.regions)
	return result
}

// SetupBilling configures billing for a tenant.
func (cp *ControlPlane) SetupBilling(tenantID string, billing BillingInfo) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if _, err := cp.tenantMgr.GetTenant(tenantID); err != nil {
		return err
	}

	cp.billing[tenantID] = &billing
	return nil
}

// GetBilling returns billing info for a tenant.
func (cp *ControlPlane) GetBilling(tenantID string) (*BillingInfo, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	b, ok := cp.billing[tenantID]
	if !ok {
		return nil, fmt.Errorf("no billing info for tenant %q", tenantID)
	}
	return b, nil
}

// SetResourceQuota sets resource quotas for a tenant.
func (cp *ControlPlane) SetResourceQuota(tenantID string, quota ResourceQuota) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if _, err := cp.tenantMgr.GetTenant(tenantID); err != nil {
		return err
	}

	cp.quotas[tenantID] = &quota
	return nil
}

// GetResourceQuota returns resource quotas for a tenant.
func (cp *ControlPlane) GetResourceQuota(tenantID string) *ResourceQuota {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if q, ok := cp.quotas[tenantID]; ok {
		return q
	}

	// Return default quotas based on plan
	tenant, err := cp.tenantMgr.GetTenant(tenantID)
	if err != nil {
		return &ResourceQuota{MaxCPUMillicores: 500, MaxMemoryMB: 256, MaxConcurrentAgents: 2}
	}

	switch tenant.Plan {
	case "free":
		return &ResourceQuota{MaxCPUMillicores: 500, MaxMemoryMB: 256, MaxConcurrentAgents: 2, MaxStorageMB: 100}
	case "pro":
		return &ResourceQuota{MaxCPUMillicores: 2000, MaxMemoryMB: 1024, MaxConcurrentAgents: 20, MaxStorageMB: 1024}
	case "enterprise":
		return &ResourceQuota{MaxCPUMillicores: 8000, MaxMemoryMB: 4096, MaxConcurrentAgents: 100, MaxStorageMB: 10240}
	default:
		return &ResourceQuota{MaxCPUMillicores: 500, MaxMemoryMB: 256, MaxConcurrentAgents: 2, MaxStorageMB: 100}
	}
}

// ConfigureSSO sets up SSO/OIDC for a tenant.
func (cp *ControlPlane) ConfigureSSO(tenantID string, sso SSOConfig) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if _, err := cp.tenantMgr.GetTenant(tenantID); err != nil {
		return err
	}

	cp.ssoConfigs[tenantID] = &sso
	return nil
}

// GetSSO returns SSO configuration for a tenant.
func (cp *ControlPlane) GetSSO(tenantID string) (*SSOConfig, error) {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	sso, ok := cp.ssoConfigs[tenantID]
	if !ok {
		return nil, fmt.Errorf("no SSO configured for tenant %q", tenantID)
	}
	return sso, nil
}

// ConfigureNetworkIsolation sets network isolation for a tenant.
func (cp *ControlPlane) ConfigureNetworkIsolation(tenantID string, isolation NetworkIsolation) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if _, err := cp.tenantMgr.GetTenant(tenantID); err != nil {
		return err
	}

	cp.isolation[tenantID] = &isolation
	return nil
}

// GetNetworkIsolation returns network isolation config for a tenant.
func (cp *ControlPlane) GetNetworkIsolation(tenantID string) *NetworkIsolation {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	if iso, ok := cp.isolation[tenantID]; ok {
		return iso
	}
	return &NetworkIsolation{TLSRequired: true}
}

// RecordUsagePeriod records a billing usage period.
func (cp *ControlPlane) RecordUsagePeriod(record UsageBillingRecord) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	// Compute cost based on plan
	record.ComputedCostUSD = cp.computeCost(record)
	cp.usageRecords = append(cp.usageRecords, record)

	if len(cp.usageRecords) > 10000 {
		cp.usageRecords = cp.usageRecords[len(cp.usageRecords)-10000:]
	}
}

func (cp *ControlPlane) computeCost(record UsageBillingRecord) float64 {
	// Usage-based pricing
	const (
		costPerToolCall    = 0.0001  // $0.01 per 100 calls
		costPerMessage     = 0.00001 // $0.01 per 1000 messages
		costPerGB          = 0.10    // $0.10 per GB transferred
	)

	cost := float64(record.ToolCalls) * costPerToolCall
	cost += float64(record.MessagesConsumed+record.MessagesProduced) * costPerMessage
	cost += float64(record.BytesTransferred) / (1024 * 1024 * 1024) * costPerGB

	return cost
}

// GetUsageRecords returns usage records for a tenant.
func (cp *ControlPlane) GetUsageRecords(tenantID string) []UsageBillingRecord {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	var result []UsageBillingRecord
	for _, r := range cp.usageRecords {
		if r.TenantID == tenantID {
			result = append(result, r)
		}
	}
	return result
}

// ControlPlaneAPI provides HTTP handlers for the control plane REST API.
type ControlPlaneAPI struct {
	cp     *ControlPlane
	apiKey string // Admin API key for control plane operations
}

// NewControlPlaneAPI creates a new control plane API handler.
func NewControlPlaneAPI(cp *ControlPlane, adminAPIKey string) *ControlPlaneAPI {
	return &ControlPlaneAPI{cp: cp, apiKey: adminAPIKey}
}

// Handler returns the HTTP handler for the control plane API.
func (api *ControlPlaneAPI) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/tenants", api.authMiddleware(api.handleTenants))
	mux.HandleFunc("/api/v1/tenants/", api.authMiddleware(api.handleTenantByID))
	mux.HandleFunc("/api/v1/regions", api.authMiddleware(api.handleRegions))
	mux.HandleFunc("/api/v1/billing/", api.authMiddleware(api.handleBilling))
	mux.HandleFunc("/api/v1/quotas/", api.authMiddleware(api.handleQuotas))

	// Wrap with security headers and body size limit (10 MB)
	return controlPlaneSecurityHeaders(http.MaxBytesHandler(mux, 10<<20))
}

// controlPlaneSecurityHeaders adds standard security headers to control plane HTTP responses.
func controlPlaneSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		next.ServeHTTP(w, r)
	})
}

func (api *ControlPlaneAPI) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if api.apiKey != "" {
			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			provided := strings.TrimPrefix(authHeader, "Bearer ")
			if subtle.ConstantTimeCompare([]byte(provided), []byte(api.apiKey)) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func (api *ControlPlaneAPI) handleTenants(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		tenants := api.cp.tenantMgr.ListTenants()
		api.writeJSON(w, map[string]any{"tenants": tenants, "total": len(tenants)})

	case http.MethodPost:
		var req struct {
			Name  string `json:"name"`
			Email string `json:"email"`
			Plan  string `json:"plan"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		tenant, apiKey, err := api.cp.tenantMgr.CreateTenant(req.Name, req.Email, req.Plan)
		if err != nil {
			slog.Error("failed to create tenant", "error", err)
			http.Error(w, "failed to create tenant", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		api.writeJSON(w, map[string]any{"tenant": tenant, "api_key": apiKey})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *ControlPlaneAPI) handleTenantByID(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/tenants/")
	if tenantID == "" {
		http.Error(w, "tenant ID required", http.StatusBadRequest)
		return
	}

	tenant, err := api.cp.tenantMgr.GetTenant(tenantID)
	if err != nil {
		slog.Error("tenant not found", "tenant_id", tenantID, "error", err)
		http.Error(w, "tenant not found", http.StatusNotFound)
		return
	}

	api.writeJSON(w, map[string]any{
		"tenant":   tenant,
		"quota":    api.cp.GetResourceQuota(tenantID),
		"network":  api.cp.GetNetworkIsolation(tenantID),
	})
}

func (api *ControlPlaneAPI) handleRegions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	regions := api.cp.ListRegions()
	api.writeJSON(w, map[string]any{"regions": regions, "total": len(regions)})
}

func (api *ControlPlaneAPI) handleBilling(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/billing/")
	if tenantID == "" {
		http.Error(w, "tenant ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		billing, err := api.cp.GetBilling(tenantID)
		if err != nil {
			slog.Error("billing not found", "tenant_id", tenantID, "error", err)
			http.Error(w, "billing information not found", http.StatusNotFound)
			return
		}
		usage := api.cp.GetUsageRecords(tenantID)
		api.writeJSON(w, map[string]any{"billing": billing, "usage_records": usage})

	case http.MethodPost:
		var billing BillingInfo
		if err := json.NewDecoder(r.Body).Decode(&billing); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if err := api.cp.SetupBilling(tenantID, billing); err != nil {
			slog.Error("failed to setup billing", "tenant_id", tenantID, "error", err)
			http.Error(w, "failed to configure billing", http.StatusBadRequest)
			return
		}
		api.writeJSON(w, map[string]any{"status": "billing configured"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *ControlPlaneAPI) handleQuotas(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/quotas/")
	if tenantID == "" {
		http.Error(w, "tenant ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		quota := api.cp.GetResourceQuota(tenantID)
		api.writeJSON(w, map[string]any{"quota": quota})

	case http.MethodPut:
		var quota ResourceQuota
		if err := json.NewDecoder(r.Body).Decode(&quota); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if err := api.cp.SetResourceQuota(tenantID, quota); err != nil {
			slog.Error("failed to set quota", "tenant_id", tenantID, "error", err)
			http.Error(w, "failed to update quota", http.StatusBadRequest)
			return
		}
		api.writeJSON(w, map[string]any{"status": "quota updated"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *ControlPlaneAPI) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// GenerateAdminAPIKey generates a new admin API key.
func GenerateAdminAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("kmcp_admin_%s", hex.EncodeToString(b)), nil
}

// AutoScaleConfig defines auto-scaling parameters for tenant instances.
type AutoScaleConfig struct {
	Enabled         bool    `json:"enabled" yaml:"enabled"`
	MinInstances    int     `json:"min_instances" yaml:"min_instances"`
	MaxInstances    int     `json:"max_instances" yaml:"max_instances"`
	TargetCPUPercent float64 `json:"target_cpu_percent" yaml:"target_cpu_percent"`
	ScaleUpCooldown  int    `json:"scale_up_cooldown_secs" yaml:"scale_up_cooldown_secs"`
	ScaleDownCooldown int   `json:"scale_down_cooldown_secs" yaml:"scale_down_cooldown_secs"`
}

// TenantInstance represents a running KafkaMCP instance for a tenant.
type TenantInstance struct {
	TenantID    string    `json:"tenant_id"`
	InstanceID  string    `json:"instance_id"`
	Region      string    `json:"region"`
	Status      string    `json:"status"` // "running", "scaling", "stopped"
	StartedAt   time.Time `json:"started_at"`
	CPU         float64   `json:"cpu_percent"`
	MemoryMB    int       `json:"memory_mb"`
	Connections int       `json:"connections"`
}

// InstanceManager manages per-tenant KafkaMCP instances.
type InstanceManager struct {
	mu        sync.RWMutex
	instances map[string][]TenantInstance
	configs   map[string]*AutoScaleConfig
}

// NewInstanceManager creates a new instance manager.
func NewInstanceManager() *InstanceManager {
	return &InstanceManager{
		instances: make(map[string][]TenantInstance),
		configs:   make(map[string]*AutoScaleConfig),
	}
}

// RegisterInstance registers a running instance for a tenant.
func (im *InstanceManager) RegisterInstance(instance TenantInstance) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.instances[instance.TenantID] = append(im.instances[instance.TenantID], instance)
}

// GetInstances returns all instances for a tenant.
func (im *InstanceManager) GetInstances(tenantID string) []TenantInstance {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.instances[tenantID]
}

// SetAutoScaleConfig configures auto-scaling for a tenant.
func (im *InstanceManager) SetAutoScaleConfig(tenantID string, config AutoScaleConfig) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.configs[tenantID] = &config
}

// EvaluateAutoScale checks if a tenant needs scaling based on current metrics.
func (im *InstanceManager) EvaluateAutoScale(tenantID string) (action string, count int) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	config, ok := im.configs[tenantID]
	if !ok || !config.Enabled {
		return "none", 0
	}

	instances := im.instances[tenantID]
	if len(instances) == 0 {
		return "scale_up", config.MinInstances
	}

	var avgCPU float64
	for _, inst := range instances {
		avgCPU += inst.CPU
	}
	avgCPU /= float64(len(instances))

	if avgCPU > config.TargetCPUPercent && len(instances) < config.MaxInstances {
		return "scale_up", 1
	}
	if avgCPU < config.TargetCPUPercent*0.5 && len(instances) > config.MinInstances {
		return "scale_down", 1
	}

	return "none", 0
}
