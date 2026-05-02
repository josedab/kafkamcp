//go:build gateway_rabbitmq

package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// RabbitMQAdapter implements StreamBackend for RabbitMQ.
type RabbitMQAdapter struct {
	config    BackendConfig
	logger    *slog.Logger
	mu        sync.RWMutex
	connected bool
	queues    map[string]*TopicInfo
}

// NewRabbitMQAdapter creates a new RabbitMQ adapter.
func NewRabbitMQAdapter(cfg BackendConfig, logger *slog.Logger) *RabbitMQAdapter {
	return &RabbitMQAdapter{
		config: cfg,
		logger: logger,
		queues: make(map[string]*TopicInfo),
	}
}

func (r *RabbitMQAdapter) Protocol() Protocol { return ProtocolRabbitMQ }

func (r *RabbitMQAdapter) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.config.Endpoints) == 0 {
		return fmt.Errorf("rabbitmq: no endpoints configured")
	}
	r.connected = true
	r.logger.Info("rabbitmq adapter connected", "endpoints", r.config.Endpoints)
	return nil
}

func (r *RabbitMQAdapter) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connected = false
	return nil
}

func (r *RabbitMQAdapter) Produce(ctx context.Context, req ProduceRequest) (*ProduceResult, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.connected {
		return nil, fmt.Errorf("rabbitmq: not connected")
	}
	return &ProduceResult{
		Topic:    req.Topic,
		Protocol: ProtocolRabbitMQ,
	}, nil
}

func (r *RabbitMQAdapter) Consume(ctx context.Context, opts ConsumeOptions) ([]Message, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.connected {
		return nil, fmt.Errorf("rabbitmq: not connected")
	}
	return []Message{}, nil
}

func (r *RabbitMQAdapter) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.connected {
		return nil, fmt.Errorf("rabbitmq: not connected")
	}
	var result []TopicInfo
	for _, q := range r.queues {
		result = append(result, *q)
	}
	return result, nil
}

func (r *RabbitMQAdapter) CreateTopic(ctx context.Context, name string, partitions int, config map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.connected {
		return fmt.Errorf("rabbitmq: not connected")
	}
	if _, exists := r.queues[name]; exists {
		return fmt.Errorf("rabbitmq: queue %q already exists", name)
	}
	props := map[string]string{"type": "queue"}
	for k, v := range config {
		props[k] = v
	}
	r.queues[name] = &TopicInfo{
		Name:       name,
		Protocol:   ProtocolRabbitMQ,
		Properties: props,
	}
	return nil
}

func (r *RabbitMQAdapter) DeleteTopic(ctx context.Context, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.connected {
		return fmt.Errorf("rabbitmq: not connected")
	}
	if _, exists := r.queues[name]; !exists {
		return fmt.Errorf("rabbitmq: queue %q not found", name)
	}
	delete(r.queues, name)
	return nil
}

func (r *RabbitMQAdapter) Healthy(ctx context.Context) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.connected {
		return fmt.Errorf("rabbitmq: not connected")
	}
	return nil
}

var _ StreamBackend = (*RabbitMQAdapter)(nil)

func init() {
	RegisterAdapterFactory(ProtocolRabbitMQ, func(cfg BackendConfig, logger *slog.Logger) StreamBackend {
		return NewRabbitMQAdapter(cfg, logger)
	})
}
