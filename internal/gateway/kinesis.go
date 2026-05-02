//go:build gateway_kinesis

package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// KinesisAdapter implements StreamBackend for AWS Kinesis.
type KinesisAdapter struct {
	config    BackendConfig
	logger    *slog.Logger
	mu        sync.RWMutex
	connected bool
	streams   map[string]*TopicInfo
}

// NewKinesisAdapter creates a new Kinesis adapter.
func NewKinesisAdapter(cfg BackendConfig, logger *slog.Logger) *KinesisAdapter {
	return &KinesisAdapter{
		config:  cfg,
		logger:  logger,
		streams: make(map[string]*TopicInfo),
	}
}

func (k *KinesisAdapter) Protocol() Protocol { return ProtocolKinesis }

func (k *KinesisAdapter) Connect(ctx context.Context) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(k.config.Endpoints) == 0 {
		return fmt.Errorf("kinesis: no endpoints configured")
	}
	k.connected = true
	k.logger.Info("kinesis adapter connected", "endpoints", k.config.Endpoints)
	return nil
}

func (k *KinesisAdapter) Close() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.connected = false
	return nil
}

func (k *KinesisAdapter) Produce(ctx context.Context, req ProduceRequest) (*ProduceResult, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if !k.connected {
		return nil, fmt.Errorf("kinesis: not connected")
	}
	return &ProduceResult{
		Topic:    req.Topic,
		Protocol: ProtocolKinesis,
	}, nil
}

func (k *KinesisAdapter) Consume(ctx context.Context, opts ConsumeOptions) ([]Message, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if !k.connected {
		return nil, fmt.Errorf("kinesis: not connected")
	}
	return []Message{}, nil
}

func (k *KinesisAdapter) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if !k.connected {
		return nil, fmt.Errorf("kinesis: not connected")
	}
	var result []TopicInfo
	for _, s := range k.streams {
		result = append(result, *s)
	}
	return result, nil
}

func (k *KinesisAdapter) CreateTopic(ctx context.Context, name string, partitions int, config map[string]string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.connected {
		return fmt.Errorf("kinesis: not connected")
	}
	if _, exists := k.streams[name]; exists {
		return fmt.Errorf("kinesis: stream %q already exists", name)
	}
	shards := partitions
	if shards < 1 {
		shards = 1
	}
	props := map[string]string{"type": "stream", "shard_count": fmt.Sprintf("%d", shards)}
	for kk, v := range config {
		props[kk] = v
	}
	k.streams[name] = &TopicInfo{
		Name:       name,
		Protocol:   ProtocolKinesis,
		Partitions: shards,
		Properties: props,
	}
	return nil
}

func (k *KinesisAdapter) DeleteTopic(ctx context.Context, name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.connected {
		return fmt.Errorf("kinesis: not connected")
	}
	if _, exists := k.streams[name]; !exists {
		return fmt.Errorf("kinesis: stream %q not found", name)
	}
	delete(k.streams, name)
	return nil
}

func (k *KinesisAdapter) Healthy(ctx context.Context) error {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if !k.connected {
		return fmt.Errorf("kinesis: not connected")
	}
	return nil
}

var _ StreamBackend = (*KinesisAdapter)(nil)

func init() {
	RegisterAdapterFactory(ProtocolKinesis, func(cfg BackendConfig, logger *slog.Logger) StreamBackend {
		return NewKinesisAdapter(cfg, logger)
	})
}
