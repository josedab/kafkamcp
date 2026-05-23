package tenant

import (
	"sync"
	"time"
)

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
	Name      string `json:"name" yaml:"name"`
	Provider  string `json:"provider" yaml:"provider"` // "aws", "gcp", "azure"
	Zone      string `json:"zone" yaml:"zone"`
	Endpoint  string `json:"endpoint" yaml:"endpoint"`
	IsActive  bool   `json:"is_active" yaml:"is_active"`
	IsPrimary bool   `json:"is_primary,omitempty" yaml:"is_primary,omitempty"`
}

// NetworkIsolation defines network isolation for a tenant.
type NetworkIsolation struct {
	VPCEnabled   bool     `json:"vpc_enabled" yaml:"vpc_enabled"`
	AllowedCIDRs []string `json:"allowed_cidrs,omitempty" yaml:"allowed_cidrs,omitempty"`
	PrivateLink  bool     `json:"private_link" yaml:"private_link"`
	TLSRequired  bool     `json:"tls_required" yaml:"tls_required"`
}

// AutoScaleConfig defines auto-scaling parameters for tenant instances.
type AutoScaleConfig struct {
	Enabled           bool    `json:"enabled" yaml:"enabled"`
	MinInstances      int     `json:"min_instances" yaml:"min_instances"`
	MaxInstances      int     `json:"max_instances" yaml:"max_instances"`
	TargetCPUPercent  float64 `json:"target_cpu_percent" yaml:"target_cpu_percent"`
	ScaleUpCooldown   int     `json:"scale_up_cooldown_secs" yaml:"scale_up_cooldown_secs"`
	ScaleDownCooldown int     `json:"scale_down_cooldown_secs" yaml:"scale_down_cooldown_secs"`
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
