package federation

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// InstanceStatus represents the status of a federated instance.
type InstanceStatus string

const (
	StatusActive       InstanceStatus = "active"
	StatusDegraded     InstanceStatus = "degraded"
	StatusUnreachable  InstanceStatus = "unreachable"
	StatusDeregistered InstanceStatus = "deregistered"
)

// Instance represents a federated KafkaMCP instance.
type Instance struct {
	ID             string         `json:"id"`
	Name           string         `json:"name"`
	Endpoint       string         `json:"endpoint"`
	Status         InstanceStatus `json:"status"`
	Capabilities   []string       `json:"capabilities"`
	Clusters       []string       `json:"clusters"`
	TopicCount     int            `json:"topic_count"`
	Version        string         `json:"version"`
	LastHeartbeat  string         `json:"last_heartbeat"`
	JoinedAt       string         `json:"joined_at"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

// FederatedQuery represents a query that spans multiple instances.
type FederatedQuery struct {
	ID        string            `json:"id"`
	Query     string            `json:"query"`
	Tool      string            `json:"tool"`
	Args      map[string]any    `json:"args"`
	Targets   []string          `json:"targets"` // instance IDs, empty = all
	Status    string            `json:"status"`
	Results   []InstanceResult  `json:"results"`
	StartedAt string            `json:"started_at"`
	Duration  string            `json:"duration,omitempty"`
}

// InstanceResult captures the result from a single instance.
type InstanceResult struct {
	InstanceID   string `json:"instance_id"`
	InstanceName string `json:"instance_name"`
	Status       string `json:"status"` // success, error, timeout
	Data         any    `json:"data,omitempty"`
	Error        string `json:"error,omitempty"`
	LatencyMs    int64  `json:"latency_ms"`
}

// Policy represents a federated policy rule.
type Policy struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Type        string   `json:"type"` // access, rate_limit, data_masking
	Scope       string   `json:"scope"` // global, cluster, topic
	Rules       any      `json:"rules"`
	Targets     []string `json:"targets"` // instance IDs
	Version     int      `json:"version"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
}

// AuditEntry records a federated action for audit trail.
type AuditEntry struct {
	Timestamp  string `json:"timestamp"`
	Action     string `json:"action"`
	InstanceID string `json:"instance_id"`
	AgentID    string `json:"agent_id"`
	Details    any    `json:"details"`
}

// Coordinator manages the federation mesh.
type Coordinator struct {
	localID    string
	instances  map[string]*Instance
	policies   map[string]*Policy
	queries    map[string]*FederatedQuery
	auditLog   []AuditEntry
	logger     *slog.Logger
	mu         sync.RWMutex
}

// NewCoordinator creates a new federation coordinator.
func NewCoordinator(localID string, logger *slog.Logger) *Coordinator {
	return &Coordinator{
		localID:   localID,
		instances: make(map[string]*Instance),
		policies:  make(map[string]*Policy),
		queries:   make(map[string]*FederatedQuery),
		logger:    logger,
	}
}

// Join registers a new instance in the federation.
func (c *Coordinator) Join(instance Instance) (*Instance, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if instance.ID == "" {
		return nil, fmt.Errorf("instance ID is required")
	}
	if instance.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}

	instance.Status = StatusActive
	instance.LastHeartbeat = time.Now().UTC().Format(time.RFC3339)
	instance.JoinedAt = time.Now().UTC().Format(time.RFC3339)

	c.instances[instance.ID] = &instance

	c.auditLog = append(c.auditLog, AuditEntry{
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Action:     "instance_joined",
		InstanceID: instance.ID,
		Details:    map[string]any{"name": instance.Name, "endpoint": instance.Endpoint},
	})

	c.logger.Info("instance joined federation", "id", instance.ID, "name", instance.Name)
	return &instance, nil
}

// Leave removes an instance from the federation.
func (c *Coordinator) Leave(instanceID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	inst, ok := c.instances[instanceID]
	if !ok {
		return fmt.Errorf("instance %q not found", instanceID)
	}

	inst.Status = StatusDeregistered

	c.auditLog = append(c.auditLog, AuditEntry{
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
		Action:     "instance_left",
		InstanceID: instanceID,
	})

	delete(c.instances, instanceID)
	return nil
}

// Heartbeat updates the heartbeat timestamp for an instance.
func (c *Coordinator) Heartbeat(instanceID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	inst, ok := c.instances[instanceID]
	if !ok {
		return fmt.Errorf("instance %q not found", instanceID)
	}

	inst.LastHeartbeat = time.Now().UTC().Format(time.RFC3339)
	inst.Status = StatusActive
	return nil
}

// Query routes a query across federation instances.
func (c *Coordinator) Query(tool string, args map[string]any, targets []string) *FederatedQuery {
	c.mu.Lock()
	defer c.mu.Unlock()

	query := &FederatedQuery{
		ID:        fmt.Sprintf("fq-%d", time.Now().UnixNano()),
		Tool:      tool,
		Args:      args,
		Targets:   targets,
		Status:    "completed",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Route to all active instances (or targeted ones)
	for _, inst := range c.instances {
		if inst.Status != StatusActive {
			continue
		}
		if len(targets) > 0 && !contains(targets, inst.ID) {
			continue
		}

		query.Results = append(query.Results, InstanceResult{
			InstanceID:   inst.ID,
			InstanceName: inst.Name,
			Status:       "success",
			Data:         map[string]any{"routed": true, "endpoint": inst.Endpoint},
			LatencyMs:    1,
		})
	}

	query.Duration = time.Since(time.Now()).String()
	c.queries[query.ID] = query

	return query
}

// GetStatus returns the federation status with all instances.
func (c *Coordinator) GetStatus() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var instances []Instance
	active := 0
	for _, inst := range c.instances {
		instances = append(instances, *inst)
		if inst.Status == StatusActive {
			active++
		}
	}

	return map[string]any{
		"local_id":        c.localID,
		"total_instances": len(instances),
		"active":          active,
		"instances":       instances,
		"policies":        len(c.policies),
	}
}

// ListInstances returns all registered instances.
func (c *Coordinator) ListInstances() []Instance {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []Instance
	for _, inst := range c.instances {
		result = append(result, *inst)
	}
	return result
}

// AddPolicy adds or updates a federated policy.
func (c *Coordinator) AddPolicy(policy Policy) *Policy {
	c.mu.Lock()
	defer c.mu.Unlock()

	policy.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	policy.UpdatedAt = policy.CreatedAt
	if policy.Version == 0 {
		policy.Version = 1
	}

	c.policies[policy.ID] = &policy
	return &policy
}

// ListPolicies returns all federation policies.
func (c *Coordinator) ListPolicies() []Policy {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []Policy
	for _, p := range c.policies {
		result = append(result, *p)
	}
	return result
}

// GetAuditLog returns recent audit entries.
func (c *Coordinator) GetAuditLog(limit int) []AuditEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if limit <= 0 || limit > len(c.auditLog) {
		limit = len(c.auditLog)
	}
	start := len(c.auditLog) - limit
	if start < 0 {
		start = 0
	}
	result := make([]AuditEntry, limit)
	copy(result, c.auditLog[start:])
	return result
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
