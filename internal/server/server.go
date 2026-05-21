package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/engine"
	enginebackend "github.com/kafkamcp/kafkamcp/internal/engine/backend"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/match"
	"github.com/kafkamcp/kafkamcp/internal/metrics"
	"github.com/kafkamcp/kafkamcp/internal/schema"
	"github.com/kafkamcp/kafkamcp/internal/telemetry"

	"github.com/mark3labs/mcp-go/server"
)

// Server is the KafkaMCP MCP server.
type Server struct {
	// Core fields
	config         *config.Config
	kafkaManager   kafka.KafkaManager
	schemaClients  map[string]*schema.Client // per-cluster schema clients
	connectMu      sync.RWMutex
	connectClients map[string]*kafka.ConnectClient // cached per-cluster Connect clients
	metrics        *metrics.Metrics
	logger         *slog.Logger
	mcpServer      *server.MCPServer
	expensiveOps   chan struct{} // semaphore for expensive operations (aggregate, join, search)

	// Tool surface management
	toolPackFilter   *ToolPackFilter
	registeredTools  map[string]string // name -> tier, populated as tools are registered
	skippedToolCount int               // count of tools filtered out by the active pack

	// Persistence: shared backend for engines that need durable state.
	// May be nil only in tests; New() always sets it.
	engineBackend enginebackend.Backend

	// Composite subsystems (embedded for direct field access)
	Analytics
	Governance
	Streaming
	Platform
}

// New creates a new KafkaMCP server. Functional options override default
// subsystem instances (e.g., WithMetrics, WithAuditLogger).
func New(cfg *config.Config, kafkaManager kafka.KafkaManager, logger *slog.Logger, version string, opts ...Option) (*Server, error) {
	// Per-cluster schema registry clients
	schemaClients := make(map[string]*schema.Client)
	for _, cl := range cfg.Clusters {
		if cl.SchemaRegistry.URL != "" {
			sc, err := schema.NewClient(
				cl.SchemaRegistry.URL,
				cl.SchemaRegistry.Auth.Username,
				cl.SchemaRegistry.Auth.Password,
				cfg.SchemaCache.MaxEntries,
				cfg.SchemaCache.TTLSeconds,
			)
			if err != nil {
				return nil, fmt.Errorf("schema registry for cluster %s: %w", cl.Name, err)
			}
			schemaClients[cl.Name] = sc
		}
	}

	gov, err := newGovernance(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("creating governance subsystem: %w", err)
	}

	engineBackend, err := engine.Open(engine.Config{
		Backend: cfg.Server.Persistence.Backend,
		Path:    cfg.Server.Persistence.Path,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("opening engine persistence backend: %w", err)
	}

	s := &Server{
		config:          cfg,
		kafkaManager:    kafkaManager,
		schemaClients:   schemaClients,
		connectClients:  make(map[string]*kafka.ConnectClient),
		metrics:         metrics.New(),
		logger:          logger,
		expensiveOps:    make(chan struct{}, 5),
		toolPackFilter:  NewToolPackFilter(cfg.Server.ToolPacks),
		registeredTools: make(map[string]string),
		engineBackend:   engineBackend,
		Analytics:       newAnalytics(logger),
		Governance:      gov,
		Streaming:       newStreaming(logger),
		Platform:        newPlatform(logger, engineBackend),
	}

	// Apply functional options, overriding defaults
	for _, opt := range opts {
		opt(s)
	}

	mcpServer := server.NewMCPServer(
		"KafkaMCP",
		version,
		server.WithToolCapabilities(true),
		server.WithResourceCapabilities(true, true),
	)

	s.mcpServer = mcpServer
	s.registerTools()
	s.registerResources()

	if s.toolPackFilter != nil {
		if s.toolPackFilter.AllAllowed() {
			logger.Info("tool packs: all tools registered",
				"registered", len(s.registeredTools))
		} else {
			logger.Info("tool packs active",
				"packs", s.toolPackFilter.Packs(),
				"registered", len(s.registeredTools),
				"skipped", s.skippedToolCount)
		}
	}

	return s, nil
}

// MCPServer returns the underlying MCP server.
func (s *Server) MCPServer() *server.MCPServer { return s.mcpServer }

// Metrics returns the Prometheus metrics.
func (s *Server) Metrics() *metrics.Metrics { return s.metrics }

// HealthCheck verifies Kafka connectivity by pinging the default cluster.
func (s *Server) HealthCheck(ctx context.Context) error {
	return s.kafkaManager.Ping(ctx)
}

// SetOTelProvider sets the OpenTelemetry provider for trace instrumentation.
func (s *Server) SetOTelProvider(p *telemetry.Provider) {
	s.otelProvider = p
}

// Close shuts down the server and all subsystems. All shutdown errors are
// joined together so the caller (or supervisor) sees every failure rather
// than only the last one.
func (s *Server) Close() error {
	var errs []error
	s.kafkaManager.Close()
	if s.watchManager != nil {
		s.watchManager.Close()
	}
	if s.anomalyDetector != nil {
		s.anomalyDetector.Stop()
	}
	if s.otelProvider != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := s.otelProvider.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("failed to shutdown OTel provider", "error", err)
			errs = append(errs, fmt.Errorf("otel provider: %w", err))
		}
		cancel()
	}
	if s.pluginRegistry != nil {
		s.pluginRegistry.Close()
	}
	if s.chaosEngine != nil {
		s.chaosEngine.Close()
	}
	if err := s.auditLogger.Close(); err != nil {
		errs = append(errs, fmt.Errorf("audit logger: %w", err))
	}
	if s.engineBackend != nil {
		if err := s.engineBackend.Close(); err != nil {
			errs = append(errs, fmt.Errorf("engine backend: %w", err))
		}
	}
	return errors.Join(errs...)
}

// EngineBackend returns the shared persistence backend used by KafkaMCP
// engines. It is non-nil for servers constructed via New().
func (s *Server) EngineBackend() enginebackend.Backend { return s.engineBackend }

// getSchemaClient returns the schema client for a cluster, falling back to default.
func (s *Server) getSchemaClient(cluster string) *schema.Client {
	name := cluster
	if name == "" {
		name = s.kafkaManager.DefaultClusterName()
	}
	if c, ok := s.schemaClients[name]; ok {
		return c
	}
	// Fall back to default cluster's client
	if c, ok := s.schemaClients[s.kafkaManager.DefaultClusterName()]; ok {
		return c
	}
	return nil
}

func (s *Server) hasSchemaRegistry() bool {
	return len(s.schemaClients) > 0
}

// getConnectClient returns a cached ConnectClient for the given cluster config.
// Clients are created once and reused across requests.
func (s *Server) getConnectClient(clCfg *config.ClusterConfig) (*kafka.ConnectClient, error) {
	if clCfg.ConnectURL == "" {
		return nil, nil
	}
	s.connectMu.RLock()
	if c, ok := s.connectClients[clCfg.Name]; ok {
		s.connectMu.RUnlock()
		return c, nil
	}
	s.connectMu.RUnlock()

	s.connectMu.Lock()
	defer s.connectMu.Unlock()
	// Double-check after acquiring write lock
	if c, ok := s.connectClients[clCfg.Name]; ok {
		return c, nil
	}
	c, err := kafka.NewConnectClient(clCfg.ConnectURL, connectTLSOpts(clCfg)...)
	if err != nil {
		return nil, err
	}
	if clCfg.ConnectTLS.InsecureSkipVerify {
		s.logger.Warn("Kafka Connect client using InsecureSkipVerify - not recommended for production", "cluster", clCfg.Name)
	}
	if c != nil {
		s.connectClients[clCfg.Name] = c
	}
	return c, nil
}

// matchTopicFilter checks whether a topic name matches a glob pattern.
func matchTopicFilter(pattern, name string) bool {
	return match.Glob(pattern, name)
}

// acquireExpensiveOp blocks until a slot is available for an expensive operation,
// or returns an error if the context is cancelled while waiting.
func (s *Server) acquireExpensiveOp(ctx context.Context) error {
	select {
	case s.expensiveOps <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for available capacity; too many concurrent expensive operations")
	}
}

// releaseExpensiveOp releases a slot from the expensive operation semaphore.
func (s *Server) releaseExpensiveOp() {
	<-s.expensiveOps
}

// resolveClusterName returns the given cluster name if it maps to a valid
// configured cluster, otherwise falls back to the default cluster.
func (s *Server) resolveClusterName(cluster string) string {
	if cluster != "" {
		if _, err := s.kafkaManager.GetClusterConfig(cluster); err == nil {
			return cluster
		}
		s.logger.Warn("invalid cluster name, falling back to default", "requested", cluster, "default", s.kafkaManager.DefaultClusterName())
		return s.kafkaManager.DefaultClusterName()
	}
	return s.kafkaManager.DefaultClusterName()
}

// clampInt64 constrains a value to [min, max].
func clampInt64(v, min, max int64) int64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

// Tool and resource handler implementations are in:
// - tools_p0.go           (P0 handlers: list topics, describe, consume, produce, schemas, consumer groups)
// - tools_p1.go           (P1 handlers: create/alter/delete topic, search, reset offsets, DLQ, subscribe)
// - tools_p2.go           (P2 handlers: aggregate, schema evolution, connectors, join, NL query)
// - tools_p3.go           (P3 handlers: watch, DLQ remediation, schema diff, streaming, agent registry)
// - tools_advanced.go     (Advanced handlers: connector CRUD, time travel, recommend, trace, ACLs, config drift, playground)
// - tools_features.go     (Feature handlers: anomalies, NL query, cost analysis, workflows, GitOps, knowledge graph, contracts)
// - tools_streaming.go    (Streaming consumer session management)
// - tools_new_features.go (New features: lag diagnosis, migration, lineage, group health, personas, forecasting)
// - resources.go          (Resource handlers: cluster, topics, schemas, groups, audit, graph)
