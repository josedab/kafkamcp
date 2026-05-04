package eventbus

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// EventType categorizes bus events.
type EventType string

const (
	EventToolInvocation EventType = "tool_invocation"
	EventToolResult     EventType = "tool_result"
	EventAgentConnect   EventType = "agent_connect"
	EventAgentDisconnect EventType = "agent_disconnect"
	EventKafkaProduce   EventType = "kafka_produce"
	EventKafkaConsume   EventType = "kafka_consume"
	EventKafkaAdmin     EventType = "kafka_admin"
	EventPolicyViolation EventType = "policy_violation"
	EventAnomaly        EventType = "anomaly"
	EventCustom         EventType = "custom"
)

// Event is a structured event on the bus.
type Event struct {
	ID        string         `json:"id"`
	Type      EventType      `json:"type"`
	Source    string         `json:"source"`
	AgentID   string         `json:"agent_id,omitempty"`
	Tool      string         `json:"tool,omitempty"`
	Cluster   string         `json:"cluster,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
	Sequence  uint64         `json:"sequence"`
}

// SubscriptionFilter defines what events a subscriber wants.
type SubscriptionFilter struct {
	EventTypes []EventType `json:"event_types,omitempty"`
	AgentIDs   []string    `json:"agent_ids,omitempty"`
	Tools      []string    `json:"tools,omitempty"`
	Clusters   []string    `json:"clusters,omitempty"`
	SourcePattern string   `json:"source_pattern,omitempty"`
}

// Subscription represents a subscriber to the event bus.
type Subscription struct {
	ID        string             `json:"id"`
	Filter    SubscriptionFilter `json:"filter"`
	CreatedAt time.Time          `json:"created_at"`
	Events    chan Event         `json:"-"`
	dropped   atomic.Int64
	delivered atomic.Int64
}

// Stats returns subscription delivery stats.
func (sub *Subscription) Stats() SubscriptionStats {
	return SubscriptionStats{
		ID:        sub.ID,
		Delivered: sub.delivered.Load(),
		Dropped:   sub.dropped.Load(),
		CreatedAt: sub.CreatedAt,
	}
}

// SubscriptionStats tracks delivery metrics for a subscription.
type SubscriptionStats struct {
	ID        string    `json:"id"`
	Delivered int64     `json:"delivered"`
	Dropped   int64     `json:"dropped"`
	CreatedAt time.Time `json:"created_at"`
}

// BusConfig configures the event bus.
type BusConfig struct {
	RingBufferSize    int `json:"ring_buffer_size" yaml:"ring_buffer_size"`
	SubscriberBuffer  int `json:"subscriber_buffer" yaml:"subscriber_buffer"`
	MaxSubscribers    int `json:"max_subscribers" yaml:"max_subscribers"`
	MaxReplayEvents   int `json:"max_replay_events" yaml:"max_replay_events"`
}

// DefaultBusConfig returns sensible defaults.
func DefaultBusConfig() BusConfig {
	return BusConfig{
		RingBufferSize:   8192,
		SubscriberBuffer: 256,
		MaxSubscribers:   100,
		MaxReplayEvents:  1000,
	}
}

// BusStats tracks aggregate bus metrics.
type BusStats struct {
	TotalPublished   uint64               `json:"total_published"`
	TotalSubscribers int                  `json:"total_subscribers"`
	BufferUsed       int                  `json:"buffer_used"`
	BufferCapacity   int                  `json:"buffer_capacity"`
	Subscriptions    []SubscriptionStats  `json:"subscriptions"`
}

// Bus is the internal event bus with ring buffer storage.
type Bus struct {
	config        BusConfig
	mu            sync.RWMutex
	ring          []Event
	ringHead      int
	ringCount     int
	sequence      atomic.Uint64
	subscriptions map[string]*Subscription
	nextSubID     atomic.Int64
	logger        *slog.Logger
}

// NewBus creates a new event bus.
func NewBus(cfg BusConfig, logger *slog.Logger) *Bus {
	if cfg.RingBufferSize <= 0 {
		cfg.RingBufferSize = 8192
	}
	return &Bus{
		config:        cfg,
		ring:          make([]Event, cfg.RingBufferSize),
		subscriptions: make(map[string]*Subscription),
		logger:        logger,
	}
}

// Publish adds an event to the bus and delivers to all matching subscribers.
func (b *Bus) Publish(event Event) {
	seq := b.sequence.Add(1)
	event.Sequence = seq
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	if event.ID == "" {
		event.ID = fmt.Sprintf("evt_%d", seq)
	}

	// Store in ring buffer
	b.mu.Lock()
	b.ring[b.ringHead] = event
	b.ringHead = (b.ringHead + 1) % len(b.ring)
	if b.ringCount < len(b.ring) {
		b.ringCount++
	}

	// Snapshot subscribers to avoid holding lock during delivery
	subs := make([]*Subscription, 0, len(b.subscriptions))
	for _, sub := range b.subscriptions {
		subs = append(subs, sub)
	}
	b.mu.Unlock()

	// Deliver to matching subscribers with backpressure (non-blocking)
	for _, sub := range subs {
		if matchesFilter(event, sub.Filter) {
			select {
			case sub.Events <- event:
				sub.delivered.Add(1)
			default:
				// Backpressure: drop event for slow subscriber
				sub.dropped.Add(1)
			}
		}
	}
}

// Subscribe creates a new subscription with the given filter.
func (b *Bus) Subscribe(filter SubscriptionFilter) (*Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.subscriptions) >= b.config.MaxSubscribers {
		return nil, fmt.Errorf("maximum subscribers (%d) reached", b.config.MaxSubscribers)
	}

	bufSize := b.config.SubscriberBuffer
	if bufSize <= 0 {
		bufSize = 256
	}

	id := fmt.Sprintf("sub_%d", b.nextSubID.Add(1))
	sub := &Subscription{
		ID:        id,
		Filter:    filter,
		CreatedAt: time.Now(),
		Events:    make(chan Event, bufSize),
	}

	b.subscriptions[id] = sub
	b.logger.Info("subscription created", "id", id)
	return sub, nil
}

// Unsubscribe removes a subscription.
func (b *Bus) Unsubscribe(id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	sub, ok := b.subscriptions[id]
	if !ok {
		return fmt.Errorf("subscription %q not found", id)
	}

	close(sub.Events)
	delete(b.subscriptions, id)
	b.logger.Info("subscription removed", "id", id)
	return nil
}

// Replay returns historical events matching the filter, up to limit.
func (b *Bus) Replay(filter SubscriptionFilter, limit int) []Event {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if limit <= 0 || limit > b.config.MaxReplayEvents {
		limit = b.config.MaxReplayEvents
	}

	var result []Event
	count := b.ringCount
	start := b.ringHead - count
	if start < 0 {
		start += len(b.ring)
	}

	for i := 0; i < count && len(result) < limit; i++ {
		idx := (start + i) % len(b.ring)
		if matchesFilter(b.ring[idx], filter) {
			result = append(result, b.ring[idx])
		}
	}

	return result
}

// Stats returns current bus metrics.
func (b *Bus) Stats() BusStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	stats := BusStats{
		TotalPublished:   b.sequence.Load(),
		TotalSubscribers: len(b.subscriptions),
		BufferUsed:       b.ringCount,
		BufferCapacity:   len(b.ring),
	}

	for _, sub := range b.subscriptions {
		stats.Subscriptions = append(stats.Subscriptions, sub.Stats())
	}
	return stats
}

// ListSubscriptions returns all active subscriptions.
func (b *Bus) ListSubscriptions() []SubscriptionStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]SubscriptionStats, 0, len(b.subscriptions))
	for _, sub := range b.subscriptions {
		result = append(result, sub.Stats())
	}
	return result
}

func matchesFilter(event Event, filter SubscriptionFilter) bool {
	if len(filter.EventTypes) > 0 {
		matched := false
		for _, t := range filter.EventTypes {
			if t == event.Type {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	if len(filter.AgentIDs) > 0 {
		matched := false
		for _, id := range filter.AgentIDs {
			if id == event.AgentID {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	if len(filter.Tools) > 0 {
		matched := false
		for _, tool := range filter.Tools {
			if tool == event.Tool {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	if len(filter.Clusters) > 0 {
		matched := false
		for _, c := range filter.Clusters {
			if c == event.Cluster {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	if filter.SourcePattern != "" {
		if !strings.Contains(event.Source, filter.SourcePattern) {
			return false
		}
	}

	return true
}
