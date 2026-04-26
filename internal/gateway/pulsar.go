package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// PulsarAdapter implements StreamBackend for Apache Pulsar.
type PulsarAdapter struct {
	config    BackendConfig
	logger    *slog.Logger
	mu        sync.RWMutex
	connected bool
	topics    map[string]*TopicInfo
}

// NewPulsarAdapter creates a new Pulsar adapter.
func NewPulsarAdapter(cfg BackendConfig, logger *slog.Logger) *PulsarAdapter {
	return &PulsarAdapter{
		config: cfg,
		logger: logger,
		topics: make(map[string]*TopicInfo),
	}
}

func (p *PulsarAdapter) Protocol() Protocol { return ProtocolPulsar }

func (p *PulsarAdapter) Connect(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.config.Endpoints) == 0 {
		return fmt.Errorf("pulsar: no endpoints configured")
	}

	// In production, this would use the Pulsar Go client.
	// For now, validate config and mark as connected.
	p.connected = true
	p.logger.Info("pulsar adapter connected", "endpoints", p.config.Endpoints)
	return nil
}

func (p *PulsarAdapter) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connected = false
	return nil
}

func (p *PulsarAdapter) Produce(ctx context.Context, req ProduceRequest) (*ProduceResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.connected {
		return nil, fmt.Errorf("pulsar: not connected")
	}

	return &ProduceResult{
		Topic:    req.Topic,
		Protocol: ProtocolPulsar,
	}, nil
}

func (p *PulsarAdapter) Consume(ctx context.Context, opts ConsumeOptions) ([]Message, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.connected {
		return nil, fmt.Errorf("pulsar: not connected")
	}

	return []Message{}, nil
}

func (p *PulsarAdapter) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.connected {
		return nil, fmt.Errorf("pulsar: not connected")
	}

	var topics []TopicInfo
	for _, t := range p.topics {
		topics = append(topics, *t)
	}
	return topics, nil
}

func (p *PulsarAdapter) CreateTopic(ctx context.Context, name string, partitions int, config map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.connected {
		return fmt.Errorf("pulsar: not connected")
	}

	if _, exists := p.topics[name]; exists {
		return fmt.Errorf("pulsar: topic %q already exists", name)
	}

	p.topics[name] = &TopicInfo{
		Name:       name,
		Protocol:   ProtocolPulsar,
		Partitions: partitions,
		Properties: config,
	}
	return nil
}

func (p *PulsarAdapter) DeleteTopic(ctx context.Context, name string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.connected {
		return fmt.Errorf("pulsar: not connected")
	}

	if _, exists := p.topics[name]; !exists {
		return fmt.Errorf("pulsar: topic %q not found", name)
	}
	delete(p.topics, name)
	return nil
}

func (p *PulsarAdapter) Healthy(ctx context.Context) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.connected {
		return fmt.Errorf("pulsar: not connected")
	}
	return nil
}

// ensure PulsarAdapter satisfies StreamBackend
var _ StreamBackend = (*PulsarAdapter)(nil)
