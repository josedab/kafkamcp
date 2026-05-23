package collaboration

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Coordinator manages distributed locks, vector clocks, and agent coordination.
type Coordinator struct {
	mu       sync.RWMutex
	locks    map[string]*DistributedLock
	clocks   map[string]*VectorClock
	agents   map[string]*AgentState
	messages []AgentMessage
	logger   *slog.Logger
}

// DistributedLock represents a lock on a shared resource.
type DistributedLock struct {
	Resource   string            `json:"resource"`
	Owner      string            `json:"owner"`
	AcquiredAt time.Time         `json:"acquired_at"`
	ExpiresAt  time.Time         `json:"expires_at"`
	TTLSec     int               `json:"ttl_seconds"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// VectorClock tracks causal ordering across agents.
type VectorClock struct {
	Resource string           `json:"resource"`
	Counters map[string]int64 `json:"counters"`
	Updated  time.Time        `json:"updated_at"`
}

// AgentState describes the current state of a connected agent.
type AgentState struct {
	AgentID      string            `json:"agent_id"`
	Status       string            `json:"status"` // active, idle, busy, disconnected
	Capabilities []string          `json:"capabilities,omitempty"`
	Resources    []string          `json:"resources,omitempty"`
	LastSeen     time.Time         `json:"last_seen"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// AgentMessage is a message between agents.
type AgentMessage struct {
	ID        string    `json:"id"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Type      string    `json:"type"` // request, response, broadcast, notification
	Subject   string    `json:"subject"`
	Body      string    `json:"body"`
	Timestamp time.Time `json:"timestamp"`
	Read      bool      `json:"read"`
}

// LockResult is returned by lock operations.
type LockResult struct {
	Acquired bool             `json:"acquired"`
	Lock     *DistributedLock `json:"lock,omitempty"`
	Error    string           `json:"error,omitempty"`
}

// NewCoordinator creates a new collaboration coordinator.
func NewCoordinator(logger *slog.Logger) *Coordinator {
	return &Coordinator{
		locks:    make(map[string]*DistributedLock),
		clocks:   make(map[string]*VectorClock),
		agents:   make(map[string]*AgentState),
		messages: make([]AgentMessage, 0),
		logger:   logger,
	}
}

// AcquireLock attempts to acquire a distributed lock on a resource.
func (c *Coordinator) AcquireLock(agentID, resource string, ttlSec int, metadata map[string]string) LockResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ttlSec <= 0 {
		ttlSec = 30
	}
	if ttlSec > 3600 {
		ttlSec = 3600
	}

	now := time.Now()

	// Check if lock exists and is still valid
	if existing, ok := c.locks[resource]; ok {
		if existing.ExpiresAt.After(now) && existing.Owner != agentID {
			return LockResult{
				Acquired: false,
				Lock:     existing,
				Error:    fmt.Sprintf("resource locked by %s until %s", existing.Owner, existing.ExpiresAt.Format(time.RFC3339)),
			}
		}
	}

	lock := &DistributedLock{
		Resource:   resource,
		Owner:      agentID,
		AcquiredAt: now,
		ExpiresAt:  now.Add(time.Duration(ttlSec) * time.Second),
		TTLSec:     ttlSec,
		Metadata:   metadata,
	}
	c.locks[resource] = lock

	c.logger.Info("lock acquired", "agent", agentID, "resource", resource, "ttl", ttlSec)
	return LockResult{Acquired: true, Lock: lock}
}

// ReleaseLock releases a distributed lock.
func (c *Coordinator) ReleaseLock(agentID, resource string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	lock, ok := c.locks[resource]
	if !ok {
		return fmt.Errorf("no lock on resource %q", resource)
	}
	if lock.Owner != agentID {
		return fmt.Errorf("lock on %q owned by %s, not %s", resource, lock.Owner, agentID)
	}

	delete(c.locks, resource)
	c.logger.Info("lock released", "agent", agentID, "resource", resource)
	return nil
}

// ListLocks returns all active locks.
func (c *Coordinator) ListLocks() []DistributedLock {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	var result []DistributedLock
	for _, l := range c.locks {
		if l.ExpiresAt.After(now) {
			result = append(result, *l)
		}
	}
	return result
}

// IncrementClock increments the vector clock for a resource.
func (c *Coordinator) IncrementClock(agentID, resource string) *VectorClock {
	c.mu.Lock()
	defer c.mu.Unlock()

	vc, ok := c.clocks[resource]
	if !ok {
		vc = &VectorClock{
			Resource: resource,
			Counters: make(map[string]int64),
		}
		c.clocks[resource] = vc
	}

	vc.Counters[agentID]++
	vc.Updated = time.Now()
	return vc
}

// GetClock returns the vector clock for a resource.
func (c *Coordinator) GetClock(resource string) *VectorClock {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if vc, ok := c.clocks[resource]; ok {
		return vc
	}
	return &VectorClock{Resource: resource, Counters: make(map[string]int64)}
}

// MergeClocks merges two vector clocks (takes max of each counter).
func (c *Coordinator) MergeClocks(resource string, incoming map[string]int64) *VectorClock {
	c.mu.Lock()
	defer c.mu.Unlock()

	vc, ok := c.clocks[resource]
	if !ok {
		vc = &VectorClock{
			Resource: resource,
			Counters: make(map[string]int64),
		}
		c.clocks[resource] = vc
	}

	for agent, count := range incoming {
		if count > vc.Counters[agent] {
			vc.Counters[agent] = count
		}
	}
	vc.Updated = time.Now()
	return vc
}

// RegisterAgent registers or updates an agent's state.
func (c *Coordinator) RegisterAgent(agentID, status string, capabilities []string, metadata map[string]string) *AgentState {
	c.mu.Lock()
	defer c.mu.Unlock()

	state, ok := c.agents[agentID]
	if !ok {
		state = &AgentState{
			AgentID: agentID,
		}
		c.agents[agentID] = state
	}

	state.Status = status
	state.Capabilities = capabilities
	state.Metadata = metadata
	state.LastSeen = time.Now()

	return state
}

// ListAgents returns all registered agents.
func (c *Coordinator) ListAgents() []AgentState {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []AgentState
	for _, a := range c.agents {
		result = append(result, *a)
	}
	return result
}

// SendMessage sends a message from one agent to another.
func (c *Coordinator) SendMessage(from, to, msgType, subject, body string) (*AgentMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if from == "" || to == "" {
		return nil, fmt.Errorf("from and to are required")
	}

	msg := AgentMessage{
		ID:        fmt.Sprintf("msg_%d", len(c.messages)+1),
		From:      from,
		To:        to,
		Type:      msgType,
		Subject:   subject,
		Body:      body,
		Timestamp: time.Now(),
	}

	c.messages = append(c.messages, msg)

	// Keep last 10000 messages
	if len(c.messages) > 10000 {
		c.messages = c.messages[len(c.messages)-10000:]
	}

	return &msg, nil
}

// GetMessages returns messages for an agent.
func (c *Coordinator) GetMessages(agentID string, unreadOnly bool) []AgentMessage {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result []AgentMessage
	for _, m := range c.messages {
		if m.To == agentID || m.To == "*" {
			if unreadOnly && m.Read {
				continue
			}
			result = append(result, m)
		}
	}
	return result
}

// MarkRead marks a message as read.
func (c *Coordinator) MarkRead(agentID, messageID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.messages {
		if c.messages[i].ID == messageID && (c.messages[i].To == agentID || c.messages[i].To == "*") {
			c.messages[i].Read = true
			return nil
		}
	}
	return fmt.Errorf("message %q not found for agent %s", messageID, agentID)
}

// CleanExpiredLocks removes expired locks.
func (c *Coordinator) CleanExpiredLocks() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	cleaned := 0
	for resource, lock := range c.locks {
		if lock.ExpiresAt.Before(now) {
			delete(c.locks, resource)
			cleaned++
		}
	}
	return cleaned
}

// TopicLease represents a lease-based ownership claim on a Kafka topic.
type TopicLease struct {
	Topic       string            `json:"topic"`
	Owner       string            `json:"owner"`
	LeaseType   string            `json:"lease_type"` // exclusive, shared
	AcquiredAt  time.Time         `json:"acquired_at"`
	ExpiresAt   time.Time         `json:"expires_at"`
	RenewCount  int               `json:"renew_count"`
	MaxRenewals int               `json:"max_renewals"`
	Permissions []string          `json:"permissions"` // read, write, admin
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// LeaseResult is returned by lease operations.
type LeaseResult struct {
	Acquired bool        `json:"acquired"`
	Lease    *TopicLease `json:"lease,omitempty"`
	Error    string      `json:"error,omitempty"`
}

// AcquireTopicLease acquires a lease on a topic for an agent.
func (c *Coordinator) AcquireTopicLease(agentID, topic, leaseType string, ttlSec, maxRenewals int, permissions []string, metadata map[string]string) LeaseResult {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ttlSec <= 0 {
		ttlSec = 60
	}
	if ttlSec > 3600 {
		ttlSec = 3600
	}
	if maxRenewals <= 0 {
		maxRenewals = 10
	}
	if leaseType == "" {
		leaseType = "exclusive"
	}
	if len(permissions) == 0 {
		permissions = []string{"read", "write"}
	}

	now := time.Now()
	exclusiveKey := "lease:" + topic

	// Check for existing exclusive lease held by another agent
	if existing, ok := c.locks[exclusiveKey]; ok && existing.ExpiresAt.After(now) && existing.Owner != agentID {
		return LeaseResult{
			Acquired: false,
			Error:    fmt.Sprintf("topic %q exclusively leased by %s until %s", topic, existing.Owner, existing.ExpiresAt.Format(time.RFC3339)),
		}
	}

	// For exclusive acquisition, check if any shared leases exist from other agents
	if leaseType == "exclusive" {
		for key, lock := range c.locks {
			if strings.HasPrefix(key, exclusiveKey+":") && lock.ExpiresAt.After(now) && lock.Owner != agentID {
				return LeaseResult{
					Acquired: false,
					Error:    fmt.Sprintf("topic %q has active shared lease by %s until %s", topic, lock.Owner, lock.ExpiresAt.Format(time.RFC3339)),
				}
			}
		}
	}

	meta := map[string]string{
		"lease_type":   leaseType,
		"topic":        topic,
		"permissions":  strings.Join(permissions, ","),
		"max_renewals": strconv.Itoa(maxRenewals),
		"renew_count":  "0",
	}

	if leaseType == "exclusive" {
		c.locks[exclusiveKey] = &DistributedLock{
			Resource:   exclusiveKey,
			Owner:      agentID,
			AcquiredAt: now,
			ExpiresAt:  now.Add(time.Duration(ttlSec) * time.Second),
			TTLSec:     ttlSec,
			Metadata:   meta,
		}
	} else {
		sharedKey := exclusiveKey + ":" + agentID
		c.locks[sharedKey] = &DistributedLock{
			Resource:   sharedKey,
			Owner:      agentID,
			AcquiredAt: now,
			ExpiresAt:  now.Add(time.Duration(ttlSec) * time.Second),
			TTLSec:     ttlSec,
			Metadata:   meta,
		}
	}

	lease := &TopicLease{
		Topic:       topic,
		Owner:       agentID,
		LeaseType:   leaseType,
		AcquiredAt:  now,
		ExpiresAt:   now.Add(time.Duration(ttlSec) * time.Second),
		RenewCount:  0,
		MaxRenewals: maxRenewals,
		Permissions: permissions,
		Metadata:    metadata,
	}

	c.logger.Info("topic lease acquired", "agent", agentID, "topic", topic, "type", leaseType, "ttl", ttlSec)
	return LeaseResult{Acquired: true, Lease: lease}
}

// ReleaseTopicLease releases a topic lease.
func (c *Coordinator) ReleaseTopicLease(agentID, topic string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	exclusiveKey := "lease:" + topic
	sharedKey := "lease:" + topic + ":" + agentID

	if lock, ok := c.locks[exclusiveKey]; ok && lock.Owner == agentID {
		delete(c.locks, exclusiveKey)
		c.logger.Info("topic lease released", "agent", agentID, "topic", topic)
		return nil
	}
	if lock, ok := c.locks[sharedKey]; ok && lock.Owner == agentID {
		delete(c.locks, sharedKey)
		c.logger.Info("topic lease released", "agent", agentID, "topic", topic)
		return nil
	}

	return fmt.Errorf("no lease on topic %q for agent %s", topic, agentID)
}

// RenewTopicLease extends a topic lease TTL.
func (c *Coordinator) RenewTopicLease(agentID, topic string, ttlSec int) (*TopicLease, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ttlSec <= 0 {
		ttlSec = 60
	}

	exclusiveKey := "lease:" + topic
	sharedKey := "lease:" + topic + ":" + agentID

	var lock *DistributedLock
	if l, ok := c.locks[exclusiveKey]; ok && l.Owner == agentID {
		lock = l
	} else if l, ok := c.locks[sharedKey]; ok && l.Owner == agentID {
		lock = l
	}

	if lock == nil {
		return nil, fmt.Errorf("no lease on topic %q for agent %s", topic, agentID)
	}

	now := time.Now()
	maxRenewals := 10
	if lock.Metadata != nil {
		if mr, ok := lock.Metadata["max_renewals"]; ok {
			if v, err := strconv.Atoi(mr); err == nil {
				maxRenewals = v
			}
		}
	}

	renewCount := 0
	if lock.Metadata != nil {
		if rc, ok := lock.Metadata["renew_count"]; ok {
			if v, err := strconv.Atoi(rc); err == nil {
				renewCount = v
			}
		}
	}

	if renewCount >= maxRenewals {
		return nil, fmt.Errorf("max renewals (%d) reached for topic %q", maxRenewals, topic)
	}

	lock.ExpiresAt = now.Add(time.Duration(ttlSec) * time.Second)
	renewCount++
	if lock.Metadata == nil {
		lock.Metadata = make(map[string]string)
	}
	lock.Metadata["renew_count"] = strconv.Itoa(renewCount)

	lease := &TopicLease{
		Topic:       topic,
		Owner:       agentID,
		LeaseType:   lock.Metadata["lease_type"],
		AcquiredAt:  lock.AcquiredAt,
		ExpiresAt:   lock.ExpiresAt,
		RenewCount:  renewCount,
		MaxRenewals: maxRenewals,
		Permissions: strings.Split(lock.Metadata["permissions"], ","),
		Metadata:    lock.Metadata,
	}

	c.logger.Info("topic lease renewed", "agent", agentID, "topic", topic, "renewals", renewCount)
	return lease, nil
}

// ListTopicLeases returns all active topic leases.
func (c *Coordinator) ListTopicLeases() []TopicLease {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	var result []TopicLease
	for _, lock := range c.locks {
		if lock.ExpiresAt.Before(now) || lock.Metadata == nil || lock.Metadata["topic"] == "" {
			continue
		}
		result = append(result, TopicLease{
			Topic:       lock.Metadata["topic"],
			Owner:       lock.Owner,
			LeaseType:   lock.Metadata["lease_type"],
			AcquiredAt:  lock.AcquiredAt,
			ExpiresAt:   lock.ExpiresAt,
			Permissions: strings.Split(lock.Metadata["permissions"], ","),
			Metadata:    lock.Metadata,
		})
	}
	return result
}

func joinStrings(ss []string) string {
	return strings.Join(ss, ",")
}

func splitPermissions(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}
