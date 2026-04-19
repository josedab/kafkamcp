package watch

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/match"
)

// FilterExpression defines a structured filter for subscription matching.
type FilterExpression struct {
	Field    string `json:"field"`              // "key", "value", "header.<name>"
	Operator string `json:"operator"`           // "contains", "equals", "regex", "exists"
	Value    string `json:"value,omitempty"`
	compiled *regexp.Regexp
}

// Compile prepares the filter for evaluation.
func (f *FilterExpression) Compile() error {
	if f.Operator == "regex" && f.Value != "" {
		r, err := regexp.Compile(f.Value)
		if err != nil {
			return fmt.Errorf("invalid regex filter %q: %w", f.Value, err)
		}
		f.compiled = r
	}
	return nil
}

// Match evaluates the filter against a message event.
func (f *FilterExpression) Match(event MessageEvent) bool {
	var fieldValue string
	switch {
	case f.Field == "key":
		fieldValue = fmt.Sprintf("%v", event.Key)
	case f.Field == "value":
		fieldValue = fmt.Sprintf("%v", event.Value)
	case strings.HasPrefix(f.Field, "header."):
		headerName := strings.TrimPrefix(f.Field, "header.")
		fieldValue = event.Headers[headerName]
		if f.Operator == "exists" {
			_, exists := event.Headers[headerName]
			return exists
		}
	default:
		return false
	}

	switch f.Operator {
	case "contains":
		return containsString(fieldValue, f.Value)
	case "equals":
		return fieldValue == f.Value
	case "regex":
		if f.compiled != nil {
			return f.compiled.MatchString(fieldValue)
		}
		return false
	case "exists":
		return fieldValue != ""
	default:
		return containsString(fieldValue, f.Value)
	}
}

// ConsumerPool manages background consumers for watch subscriptions.
type ConsumerPool struct {
	mu              sync.RWMutex
	consumers       map[string]*topicConsumer
	manager         *Manager
	kafkaManager    kafka.KafkaManager
	cluster         string
	maxConsumers    int
	activeConsumers atomic.Int32
	logger          interface{ Info(string, ...any) }
	ctx             context.Context
	cancel          context.CancelFunc
}

type topicConsumer struct {
	topic     string
	cancel    context.CancelFunc
	startedAt time.Time
}

// NewConsumerPool creates a pool of background consumers for subscription delivery.
func NewConsumerPool(mgr *Manager, km kafka.KafkaManager, cluster string, maxConsumers int) *ConsumerPool {
	if maxConsumers <= 0 {
		maxConsumers = 100
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &ConsumerPool{
		consumers:    make(map[string]*topicConsumer),
		manager:      mgr,
		kafkaManager: km,
		cluster:      cluster,
		maxConsumers: maxConsumers,
		logger:       mgr.logger,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// EnsureConsumer starts a background consumer for a topic if one isn't already running.
func (p *ConsumerPool) EnsureConsumer(topic string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.consumers[topic]; exists {
		return nil
	}

	if int(p.activeConsumers.Load()) >= p.maxConsumers {
		return fmt.Errorf("consumer pool limit reached (%d/%d)", p.activeConsumers.Load(), p.maxConsumers)
	}

	ctx, cancel := context.WithCancel(p.ctx)
	tc := &topicConsumer{
		topic:     topic,
		cancel:    cancel,
		startedAt: time.Now(),
	}
	p.consumers[topic] = tc
	p.activeConsumers.Add(1)

	go p.runConsumer(ctx, topic)
	return nil
}

func (p *ConsumerPool) runConsumer(ctx context.Context, topic string) {
	defer func() {
		p.mu.Lock()
		delete(p.consumers, topic)
		p.mu.Unlock()
		p.activeConsumers.Add(-1)
	}()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !p.hasSubscriptionsForTopic(topic) {
				return
			}

			msgs, err := p.kafkaManager.Consume(ctx, p.cluster, topic, -1, "latest", 100, 1000, "auto")
			if err != nil {
				continue
			}

			for _, msg := range msgs {
				event := MessageEvent{
					Topic:     topic,
					Partition: msg.Partition,
					Offset:    msg.Offset,
					Timestamp: time.Now(),
					Key:       msg.Key,
					Value:     msg.Value,
					Headers:   msg.Headers,
				}
				p.manager.ProcessMessage(ctx, event)
			}
		}
	}
}

func (p *ConsumerPool) hasSubscriptionsForTopic(topic string) bool {
	subs := p.manager.ListSubscriptions("")
	for _, sub := range subs {
		if match.Glob(sub.TopicPattern, topic) {
			return true
		}
	}
	return false
}

// ActiveCount returns the number of active background consumers.
func (p *ConsumerPool) ActiveCount() int {
	return int(p.activeConsumers.Load())
}

// Stop shuts down all background consumers.
func (p *ConsumerPool) Stop() {
	p.cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	for topic, tc := range p.consumers {
		tc.cancel()
		delete(p.consumers, topic)
	}
}

// DeliveryTracker tracks notification delivery statistics.
type DeliveryTracker struct {
	mu           sync.RWMutex
	delivered    atomic.Int64
	failed       atomic.Int64
	retried      atomic.Int64
	bySubscription map[string]*SubscriptionDeliveryStats
}

// SubscriptionDeliveryStats tracks per-subscription delivery metrics.
type SubscriptionDeliveryStats struct {
	Delivered    int64     `json:"delivered"`
	Failed       int64     `json:"failed"`
	Retried      int64     `json:"retried"`
	LastDelivery time.Time `json:"last_delivery,omitempty"`
	LastError    string    `json:"last_error,omitempty"`
}

// NewDeliveryTracker creates a new delivery tracker.
func NewDeliveryTracker() *DeliveryTracker {
	return &DeliveryTracker{
		bySubscription: make(map[string]*SubscriptionDeliveryStats),
	}
}

// Record records a delivery result.
func (dt *DeliveryTracker) Record(result DeliveryResult) {
	dt.mu.Lock()
	defer dt.mu.Unlock()

	stats, ok := dt.bySubscription[result.SubscriptionID]
	if !ok {
		stats = &SubscriptionDeliveryStats{}
		dt.bySubscription[result.SubscriptionID] = stats
	}

	switch result.Status {
	case "delivered":
		stats.Delivered++
		stats.LastDelivery = time.Now()
		dt.delivered.Add(1)
	case "failed":
		stats.Failed++
		stats.LastError = result.LastError
		dt.failed.Add(1)
	case "retrying":
		stats.Retried++
		dt.retried.Add(1)
	}
}

// Stats returns overall delivery statistics.
func (dt *DeliveryTracker) Stats() map[string]any {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	return map[string]any{
		"total_delivered": dt.delivered.Load(),
		"total_failed":   dt.failed.Load(),
		"total_retried":  dt.retried.Load(),
		"subscriptions":  len(dt.bySubscription),
	}
}

// GetSubscriptionStats returns stats for a specific subscription.
func (dt *DeliveryTracker) GetSubscriptionStats(subID string) *SubscriptionDeliveryStats {
	dt.mu.RLock()
	defer dt.mu.RUnlock()
	if s, ok := dt.bySubscription[subID]; ok {
		cp := *s
		return &cp
	}
	return nil
}
