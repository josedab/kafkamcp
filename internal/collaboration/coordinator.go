package collaboration

import (
	"fmt"
	"log/slog"
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
	Resource  string    `json:"resource"`
	Owner     string    `json:"owner"`
	AcquiredAt time.Time `json:"acquired_at"`
	ExpiresAt time.Time `json:"expires_at"`
	TTLSec    int       `json:"ttl_seconds"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// VectorClock tracks causal ordering across agents.
type VectorClock struct {
	Resource string           `json:"resource"`
	Counters map[string]int64 `json:"counters"`
	Updated  time.Time        `json:"updated_at"`
}

// AgentState describes the current state of a connected agent.
type AgentState struct {
	AgentID     string            `json:"agent_id"`
	Status      string            `json:"status"` // active, idle, busy, disconnected
	Capabilities []string         `json:"capabilities,omitempty"`
	Resources   []string          `json:"resources,omitempty"`
	LastSeen    time.Time         `json:"last_seen"`
	Metadata    map[string]string `json:"metadata,omitempty"`
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
	Acquired bool              `json:"acquired"`
	Lock     *DistributedLock  `json:"lock,omitempty"`
	Error    string            `json:"error,omitempty"`
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
