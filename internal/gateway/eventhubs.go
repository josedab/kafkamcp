package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// EventHubsAdapter implements StreamBackend for Azure Event Hubs.
type EventHubsAdapter struct {
	config    BackendConfig
	logger    *slog.Logger
	mu        sync.RWMutex
	connected bool
	hubs      map[string]*TopicInfo
}

// NewEventHubsAdapter creates a new Azure Event Hubs adapter.
func NewEventHubsAdapter(cfg BackendConfig, logger *slog.Logger) *EventHubsAdapter {
	return &EventHubsAdapter{
		config: cfg,
		logger: logger,
		hubs:   make(map[string]*TopicInfo),
	}
}

func (e *EventHubsAdapter) Protocol() Protocol { return ProtocolEventHubs }

func (e *EventHubsAdapter) Connect(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.config.Endpoints) == 0 {
		return fmt.Errorf("eventhubs: no endpoints configured")
	}
	e.connected = true
	e.logger.Info("event hubs adapter connected", "endpoints", e.config.Endpoints)
	return nil
}

func (e *EventHubsAdapter) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.connected = false
	return nil
}

func (e *EventHubsAdapter) Produce(ctx context.Context, req ProduceRequest) (*ProduceResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.connected {
		return nil, fmt.Errorf("eventhubs: not connected")
	}
	return &ProduceResult{
		Topic:    req.Topic,
		Protocol: ProtocolEventHubs,
	}, nil
}

func (e *EventHubsAdapter) Consume(ctx context.Context, opts ConsumeOptions) ([]Message, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.connected {
		return nil, fmt.Errorf("eventhubs: not connected")
	}
	return []Message{}, nil
}

func (e *EventHubsAdapter) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.connected {
		return nil, fmt.Errorf("eventhubs: not connected")
	}
	var result []TopicInfo
	for _, h := range e.hubs {
		result = append(result, *h)
	}
	return result, nil
}

func (e *EventHubsAdapter) CreateTopic(ctx context.Context, name string, partitions int, config map[string]string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.connected {
		return fmt.Errorf("eventhubs: not connected")
	}
	if _, exists := e.hubs[name]; exists {
		return fmt.Errorf("eventhubs: hub %q already exists", name)
	}
	if partitions < 1 {
		partitions = 2
	}
	props := map[string]string{"type": "event_hub"}
	for k, v := range config {
		props[k] = v
	}
	e.hubs[name] = &TopicInfo{
		Name:       name,
		Protocol:   ProtocolEventHubs,
		Partitions: partitions,
		Properties: props,
	}
	return nil
}

func (e *EventHubsAdapter) DeleteTopic(ctx context.Context, name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.connected {
		return fmt.Errorf("eventhubs: not connected")
	}
	if _, exists := e.hubs[name]; !exists {
		return fmt.Errorf("eventhubs: hub %q not found", name)
	}
	delete(e.hubs, name)
	return nil
}

func (e *EventHubsAdapter) Healthy(ctx context.Context) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if !e.connected {
		return fmt.Errorf("eventhubs: not connected")
	}
	return nil
}

var _ StreamBackend = (*EventHubsAdapter)(nil)
