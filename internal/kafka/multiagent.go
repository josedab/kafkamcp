package kafka

import (
	"fmt"
	"sync"
	"time"
)

// AgentInfo represents a registered agent in the multi-agent system.
type AgentInfo struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Status        string            `json:"status"` // active, inactive
	RegisteredAt  string            `json:"registered_at"`
	LastHeartbeat string            `json:"last_heartbeat"`
	Partitions    map[string][]int32 `json:"claimed_partitions"` // topic -> partitions
}

// SharedCursor represents a shared offset cursor between agents.
type SharedCursor struct {
	AgentID   string `json:"agent_id"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	UpdatedAt string `json:"updated_at"`
}

// HandoffMessage represents a message queued for handoff between agents.
type HandoffMessage struct {
	ID          string `json:"id"`
	FromAgent   string `json:"from_agent"`
	ToAgent     string `json:"to_agent"`
	Data        string `json:"data"`
	CreatedAt   string `json:"created_at"`
	Status      string `json:"status"` // pending, delivered
}

// AgentRegistry manages multi-agent collaboration primitives.
type AgentRegistry struct {
	mu        sync.RWMutex
	agents    map[string]*AgentInfo
	cursors   map[string]*SharedCursor // key: "topic:partition"
	handoffs  []HandoffMessage
	leases    map[string]string // key: "topic:partition" -> agentID
	leaseTTL  time.Duration
	leaseExp  map[string]time.Time // key: "topic:partition" -> expiry
}

// NewAgentRegistry creates a new agent registry.
func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{
		agents:   make(map[string]*AgentInfo),
		cursors:  make(map[string]*SharedCursor),
		leases:   make(map[string]string),
		leaseExp: make(map[string]time.Time),
		leaseTTL: 30 * time.Second,
	}
}

// RegisterAgent registers an agent in the registry.
func (r *AgentRegistry) RegisterAgent(agentID, name string) *AgentInfo {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)
	agent := &AgentInfo{
		ID:            agentID,
		Name:          name,
		Status:        "active",
		RegisteredAt:  now,
		LastHeartbeat: now,
		Partitions:    make(map[string][]int32),
	}
	r.agents[agentID] = agent
	return agent
}

// Heartbeat updates agent heartbeat timestamp.
func (r *AgentRegistry) Heartbeat(agentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	agent, ok := r.agents[agentID]
	if !ok {
		return fmt.Errorf("agent %q not registered", agentID)
	}
	agent.LastHeartbeat = time.Now().UTC().Format(time.RFC3339)
	agent.Status = "active"
	return nil
}

// ClaimPartition claims a topic partition for an agent with lease-based ownership.
func (r *AgentRegistry) ClaimPartition(agentID, topic string, partition int32) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.agents[agentID]; !ok {
		return fmt.Errorf("agent %q not registered", agentID)
	}

	key := fmt.Sprintf("%s:%d", topic, partition)

	// Check existing lease
	if owner, ok := r.leases[key]; ok && owner != agentID {
		if exp, ok := r.leaseExp[key]; ok && time.Now().Before(exp) {
			return fmt.Errorf("partition %s:%d is claimed by agent %q (lease expires %s)",
				topic, partition, owner, exp.Format(time.RFC3339))
		}
	}

	r.leases[key] = agentID
	r.leaseExp[key] = time.Now().Add(r.leaseTTL)

	agent := r.agents[agentID]
	agent.Partitions[topic] = appendUnique(agent.Partitions[topic], partition)

	return nil
}

// ReleasePartition releases a claimed partition.
func (r *AgentRegistry) ReleasePartition(agentID, topic string, partition int32) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := fmt.Sprintf("%s:%d", topic, partition)
	if owner, ok := r.leases[key]; ok && owner != agentID {
		return fmt.Errorf("partition %s:%d is owned by agent %q, not %q", topic, partition, owner, agentID)
	}

	delete(r.leases, key)
	delete(r.leaseExp, key)

	if agent, ok := r.agents[agentID]; ok {
		agent.Partitions[topic] = removePartition(agent.Partitions[topic], partition)
	}
	return nil
}

// ShareCursor shares an offset cursor for cross-agent coordination.
func (r *AgentRegistry) ShareCursor(agentID, topic string, partition int32, offset int64) *SharedCursor {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := fmt.Sprintf("%s:%d", topic, partition)
	cursor := &SharedCursor{
		AgentID:   agentID,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}
	r.cursors[key] = cursor
	return cursor
}

// GetCursors returns all shared cursors.
func (r *AgentRegistry) GetCursors() []*SharedCursor {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var cursors []*SharedCursor
	for _, c := range r.cursors {
		cursors = append(cursors, c)
	}
	return cursors
}

// QueueHandoff queues a message for handoff between agents.
func (r *AgentRegistry) QueueHandoff(fromAgent, toAgent, data string) (*HandoffMessage, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.agents[toAgent]; !ok {
		return nil, fmt.Errorf("target agent %q not registered", toAgent)
	}

	msg := HandoffMessage{
		ID:        fmt.Sprintf("handoff-%d", len(r.handoffs)+1),
		FromAgent: fromAgent,
		ToAgent:   toAgent,
		Data:      data,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Status:    "pending",
	}
	r.handoffs = append(r.handoffs, msg)
	return &msg, nil
}

// ListAgents returns all registered agents.
func (r *AgentRegistry) ListAgents() []*AgentInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Expire stale agents
	now := time.Now()
	var agents []*AgentInfo
	for _, a := range r.agents {
		if t, err := time.Parse(time.RFC3339, a.LastHeartbeat); err == nil {
			if now.Sub(t) > 60*time.Second {
				a.Status = "inactive"
			}
		}
		agents = append(agents, a)
	}
	return agents
}

// PendingHandoffs returns pending handoff messages for an agent.
func (r *AgentRegistry) PendingHandoffs(agentID string) []HandoffMessage {
	r.mu.Lock()
	defer r.mu.Unlock()

	var pending []HandoffMessage
	for i := range r.handoffs {
		if r.handoffs[i].ToAgent == agentID && r.handoffs[i].Status == "pending" {
			r.handoffs[i].Status = "delivered"
			pending = append(pending, r.handoffs[i])
		}
	}
	return pending
}

func appendUnique(slice []int32, val int32) []int32 {
	for _, v := range slice {
		if v == val {
			return slice
		}
	}
	return append(slice, val)
}

func removePartition(slice []int32, val int32) []int32 {
	result := make([]int32, 0, len(slice))
	for _, v := range slice {
		if v != val {
			result = append(result, v)
		}
	}
	return result
}
