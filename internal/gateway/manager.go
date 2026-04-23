package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

// Manager orchestrates multiple streaming backends behind a unified interface.
type Manager struct {
	mu       sync.RWMutex
	backends map[string]StreamBackend
	configs  map[string]*BackendConfig
	logger   *slog.Logger
}

// NewManager creates a new gateway manager.
func NewManager(logger *slog.Logger) *Manager {
	return &Manager{
		backends: make(map[string]StreamBackend),
		configs:  make(map[string]*BackendConfig),
		logger:   logger,
	}
}

// RegisterBackend adds a configured backend to the gateway.
func (m *Manager) RegisterBackend(cfg BackendConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if cfg.Name == "" {
		return fmt.Errorf("backend name is required")
	}
	if _, exists := m.backends[cfg.Name]; exists {
		return fmt.Errorf("backend %q already registered", cfg.Name)
	}

	var backend StreamBackend
	switch cfg.Protocol {
	case ProtocolPulsar:
		backend = NewPulsarAdapter(cfg, m.logger)
	case ProtocolRabbitMQ:
		backend = NewRabbitMQAdapter(cfg, m.logger)
	case ProtocolNATS:
		backend = NewNATSAdapter(cfg, m.logger)
	default:
		return fmt.Errorf("unsupported protocol: %s", cfg.Protocol)
	}

	m.backends[cfg.Name] = backend
	m.configs[cfg.Name] = &cfg
	m.logger.Info("registered streaming backend", "name", cfg.Name, "protocol", cfg.Protocol)
	return nil
}

// Connect initializes all registered backends.
func (m *Manager) Connect(ctx context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for name, backend := range m.backends {
		if err := backend.Connect(ctx); err != nil {
			m.logger.Error("failed to connect backend", "name", name, "error", err)
			return fmt.Errorf("connecting backend %q: %w", name, err)
		}
		m.logger.Info("connected streaming backend", "name", name, "protocol", backend.Protocol())
	}
	return nil
}

// GetBackend returns a named backend.
func (m *Manager) GetBackend(name string) (StreamBackend, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	b, ok := m.backends[name]
	if !ok {
		return nil, fmt.Errorf("backend %q not found", name)
	}
	return b, nil
}

// ListBackends returns information about all registered backends.
func (m *Manager) ListBackends() []BackendStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []BackendStatus
	for name, backend := range m.backends {
		status := BackendStatus{
			Name:     name,
			Protocol: backend.Protocol(),
			Status:   "unknown",
		}
		if cfg, ok := m.configs[name]; ok {
			status.Endpoints = cfg.Endpoints
		}

		ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
		if err := backend.Healthy(ctx); err != nil {
			status.Status = "unhealthy"
			status.Error = err.Error()
		} else {
			status.Status = "healthy"
		}
		cancel()
		result = append(result, status)
	}
	return result
}

// Produce sends a message through the named backend.
func (m *Manager) Produce(ctx context.Context, backendName string, req ProduceRequest) (*ProduceResult, error) {
	backend, err := m.GetBackend(backendName)
	if err != nil {
		return nil, err
	}
	return backend.Produce(ctx, req)
}

// Consume reads messages from the named backend.
func (m *Manager) Consume(ctx context.Context, backendName string, opts ConsumeOptions) ([]Message, error) {
	backend, err := m.GetBackend(backendName)
	if err != nil {
		return nil, err
	}
	return backend.Consume(ctx, opts)
}

// ListTopics lists topics from the named backend.
func (m *Manager) ListTopics(ctx context.Context, backendName string) ([]TopicInfo, error) {
	backend, err := m.GetBackend(backendName)
	if err != nil {
		return nil, err
	}
	return backend.ListTopics(ctx)
}

// CreateTopic creates a topic on the named backend.
func (m *Manager) CreateTopic(ctx context.Context, backendName, name string, partitions int, config map[string]string) error {
	backend, err := m.GetBackend(backendName)
	if err != nil {
		return err
	}
	return backend.CreateTopic(ctx, name, partitions, config)
}

// Close shuts down all backends.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for name, backend := range m.backends {
		if err := backend.Close(); err != nil {
			m.logger.Error("failed to close backend", "name", name, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// BackendStatus describes the health of a backend.
type BackendStatus struct {
	Name      string   `json:"name"`
	Protocol  Protocol `json:"protocol"`
	Endpoints []string `json:"endpoints"`
	Status    string   `json:"status"`
	Error     string   `json:"error,omitempty"`
}

const healthCheckTimeout = 5e9 // 5 seconds
