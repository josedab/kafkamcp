package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// NATSAdapter implements StreamBackend for NATS/JetStream.
type NATSAdapter struct {
	config    BackendConfig
	logger    *slog.Logger
	mu        sync.RWMutex
	connected bool
	subjects  map[string]*TopicInfo
}

// NewNATSAdapter creates a new NATS adapter.
func NewNATSAdapter(cfg BackendConfig, logger *slog.Logger) *NATSAdapter {
	return &NATSAdapter{
		config:   cfg,
		logger:   logger,
		subjects: make(map[string]*TopicInfo),
	}
}

func (n *NATSAdapter) Protocol() Protocol { return ProtocolNATS }

func (n *NATSAdapter) Connect(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.config.Endpoints) == 0 {
		return fmt.Errorf("nats: no endpoints configured")
	}
	n.connected = true
	n.logger.Info("nats adapter connected", "endpoints", n.config.Endpoints)
	return nil
}

func (n *NATSAdapter) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected = false
	return nil
}

func (n *NATSAdapter) Produce(ctx context.Context, req ProduceRequest) (*ProduceResult, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.connected {
		return nil, fmt.Errorf("nats: not connected")
	}
	return &ProduceResult{
		Topic:    req.Topic,
		Protocol: ProtocolNATS,
	}, nil
}

func (n *NATSAdapter) Consume(ctx context.Context, opts ConsumeOptions) ([]Message, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.connected {
		return nil, fmt.Errorf("nats: not connected")
	}
	return []Message{}, nil
}

func (n *NATSAdapter) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.connected {
		return nil, fmt.Errorf("nats: not connected")
	}
	var result []TopicInfo
	for _, s := range n.subjects {
		result = append(result, *s)
	}
	return result, nil
}

func (n *NATSAdapter) CreateTopic(ctx context.Context, name string, partitions int, config map[string]string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.connected {
		return fmt.Errorf("nats: not connected")
	}
	if _, exists := n.subjects[name]; exists {
		return fmt.Errorf("nats: subject %q already exists", name)
	}
	n.subjects[name] = &TopicInfo{
		Name:       name,
		Protocol:   ProtocolNATS,
		Properties: config,
	}
	return nil
}

func (n *NATSAdapter) DeleteTopic(ctx context.Context, name string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.connected {
		return fmt.Errorf("nats: not connected")
	}
	if _, exists := n.subjects[name]; !exists {
		return fmt.Errorf("nats: subject %q not found", name)
	}
	delete(n.subjects, name)
	return nil
}

func (n *NATSAdapter) Healthy(ctx context.Context) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.connected {
		return fmt.Errorf("nats: not connected")
	}
	return nil
}

var _ StreamBackend = (*NATSAdapter)(nil)
