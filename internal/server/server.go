package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/josedab/kafkamcp/internal/approval"
	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/engine"
	enginebackend "github.com/josedab/kafkamcp/internal/engine/backend"
	"github.com/josedab/kafkamcp/internal/idempotency"
	"github.com/josedab/kafkamcp/internal/kafka"
	"github.com/josedab/kafkamcp/internal/match"
	"github.com/josedab/kafkamcp/internal/metrics"
	"github.com/josedab/kafkamcp/internal/schema"
	"github.com/josedab/kafkamcp/internal/telemetry"

	"github.com/mark3labs/mcp-go/server"
)

// Server is the KafkaMCP MCP server.
type Server struct {
	// Core fields
	config            *config.Config
	kafkaManager      kafka.KafkaManager
	operationProvider kafka.OperationProvider
	schemaClients     map[string]*schema.Client // per-cluster schema clients
	connectMu         sync.RWMutex
	connectClients    map[string]*kafka.ConnectClient // cached per-cluster Connect clients
	metrics           *metrics.Metrics
	logger            *slog.Logger
	mcpServer         *server.MCPServer
	expensiveOps      chan struct{} // semaphore for bounded message searches

	// Tool surface management
	toolPackFilter   *ToolPackFilter
	registeredTools  map[string]string   // name -> tier, populated as tools are registered
	skippedToolCount int                 // count of tools filtered out by the active pack
	seenToolNames    map[string]struct{} // every name ever passed to addTool (pre-filter)
	duplicateTools   []string            // tool names registered more than once (a bug)

	// Persistence: shared backend for engines that need durable state.
	// May be nil only in tests; New() always sets it.
	engineBackend enginebackend.Backend

	// Approval enforcement: short-lived grants gating write/destructive tools.
	// approvalManager may be nil in tests (no enforcement); New() always sets it.
	approvalManager    *approval.Manager
	approvalEnforced   bool
	idempotencyManager *idempotency.Manager

	// activation holds opt-in, privacy-preserving local activation metrics.
	activation *metrics.ActivationMetrics

	// Composite subsystems (embedded for direct field access)
	Analytics
	Governance
	Platform
}

// New creates a new KafkaMCP server. Functional options override default
// subsystem instances (e.g., WithMetrics, WithAuditLogger).
func New(cfg *config.Config, kafkaManager kafka.KafkaManager, logger *slog.Logger, version string, opts ...Option) (*Server, error) {
	guardedPacksEnabled := config.HasToolPack(cfg.Server.ToolPacks, config.PackOpsWrite) ||
		config.HasToolPack(cfg.Server.ToolPacks, config.PackGovernance)
	if guardedPacksEnabled {
		if !cfg.NetworkTransport() {
			return nil, fmt.Errorf("ops-write and governance require sse or streamable-http with a verified identity; stdio is core-read only")
		}
		if err := cfg.TransportSecurityError(); err != nil {
			return nil, fmt.Errorf("guarded tool-pack security posture: %w", err)
		}
	}

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

	engineBackend, err := engine.Open(engine.Config{
		Backend: cfg.Server.Persistence.Backend,
		Path:    cfg.Server.Persistence.Path,
	}, logger)
	if err != nil {
		return nil, fmt.Errorf("opening engine persistence backend: %w", err)
	}

	approvalManager, err := approval.NewManager(context.Background(), engineBackend, approval.Config{
		DefaultTTL:             time.Duration(cfg.Policies.Approval.TTLSeconds) * time.Second,
		MaxTTL:                 time.Duration(cfg.Policies.Approval.MaxTTLSeconds) * time.Second,
		SingleUse:              cfg.Policies.Approval.SingleUse == nil || *cfg.Policies.Approval.SingleUse,
		AllowSelfApproval:      cfg.Policies.Approval.AllowSelfApproval,
		DecisionSigningKey:     []byte(os.Getenv(cfg.Policies.Approval.EffectiveSigningKeyEnv())),
		RequireSignedDecisions: guardedPacksEnabled,
		VerificationKeys:       approvalVerificationKeys(cfg),
		TerminalRetention:      time.Duration(cfg.Policies.Approval.TerminalRetentionSeconds) * time.Second,
	})
	if err != nil {
		_ = engineBackend.Close()
		return nil, fmt.Errorf("creating approval manager: %w", err)
	}

	gov, err := newGovernance(cfg, logger, engineBackend)
	if err != nil {
		_ = engineBackend.Close()
		return nil, fmt.Errorf("creating governance subsystem: %w", err)
	}

	s := &Server{
		config:            cfg,
		kafkaManager:      kafkaManager,
		operationProvider: kafka.NewNativeOperationProvider(kafkaManager),
		schemaClients:     schemaClients,
		connectClients:    make(map[string]*kafka.ConnectClient),
		metrics:           metrics.New(),
		logger:            logger,
		expensiveOps:      make(chan struct{}, 5),
		toolPackFilter:    NewToolPackFilter(cfg.Server.ToolPacks),
		registeredTools:   make(map[string]string),
		engineBackend:     engineBackend,
		approvalManager:   approvalManager,
		approvalEnforced:  guardedPacksEnabled || cfg.Policies.Approval.EnforcementEnabled(cfg.Policies.DefaultDeny),
		Analytics:         newAnalytics(logger),
		Governance:        gov,
		Platform:          newPlatform(logger, engineBackend),
	}

	// Approval enforcement for write/destructive tools. The manager persists via
	// the shared engine backend (sqlite or in-memory). Enforcement follows the
	// default-deny posture unless explicitly configured.
	if s.approvalManager != nil {
		s.approvalManager.SetAudit(s.approvalAuditHook())
	}
	s.idempotencyManager = idempotency.NewManager(engineBackend, 24*time.Hour)

	// Apply functional options, overriding defaults
	for _, opt := range opts {
		opt(s)
	}

	// Opt-in activation metrics (local only; no phone-home). Constructed after
	// options so a custom metrics registry is honored.
	if s.metrics != nil {
		s.activation = metrics.NewActivationMetrics(s.metrics.Registry, cfg.Telemetry.ActivationMetrics)
		if s.toolPackFilter != nil {
			for _, p := range s.toolPackFilter.Packs() {
				s.activation.RecordToolPack(p)
			}
		}
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

	// Fail closed on duplicate tool registration (a code bug): each MCP tool
	// name must be registered exactly once.
	if err := s.DuplicateToolError(); err != nil {
		return nil, err
	}

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

func approvalVerificationKeys(cfg *config.Config) [][]byte {
	keys := make([][]byte, 0, len(cfg.Policies.Approval.PreviousSigningKeyEnvs))
	for _, envName := range cfg.Policies.Approval.PreviousSigningKeyEnvs {
		keys = append(keys, []byte(os.Getenv(envName)))
	}
	return keys
}

// MCPServer returns the underlying MCP server.
func (s *Server) MCPServer() *server.MCPServer { return s.mcpServer }

// Metrics returns the Prometheus metrics.
func (s *Server) Metrics() *metrics.Metrics { return s.metrics }

// HealthCheck verifies Kafka connectivity by pinging the default cluster.
func (s *Server) HealthCheck(ctx context.Context) error {
	return s.kafkaManager.Ping(ctx)
}

// ReadinessCheck reports whether the server is ready to accept traffic.
// Readiness is stricter than HealthCheck: it requires (a) a successful Kafka
// metadata fetch on every configured cluster, (b) the persistence backend to
// respond to a round-trip if one is configured, and (c) the identity provider
// to be initialized. Returns nil when ready, or a wrapped error describing
// every subsystem that is still warming up. A failed readiness check returns
// HTTP 503 so Kubernetes / load balancers will withhold traffic.
func (s *Server) ReadinessCheck(ctx context.Context) error {
	var errs []error

	if err := s.kafkaManager.Ping(ctx); err != nil {
		errs = append(errs, fmt.Errorf("kafka: %w", err))
	}

	if s.engineBackend != nil {
		if _, err := s.engineBackend.Get(ctx, "_ready", "_probe"); err != nil &&
			!errors.Is(err, enginebackend.ErrNotFound) {
			errs = append(errs, fmt.Errorf("persistence: %w", err))
		}
	}

	return errors.Join(errs...)
}

// SetOTelProvider sets the OpenTelemetry provider for trace instrumentation.
func (s *Server) SetOTelProvider(p *telemetry.Provider) {
	s.otelProvider = p
}

// Close shuts down server-owned subsystems before closing Kafka last. The app
// has already drained HTTP and metrics traffic before calling this method.
// All shutdown errors are joined so the supervisor sees every failure.
func (s *Server) Close() error {
	var errs []error
	// 1. Telemetry provider.
	if s.otelProvider != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := s.otelProvider.Shutdown(shutdownCtx); err != nil {
			s.logger.Error("failed to shutdown OTel provider", "error", err)
			errs = append(errs, fmt.Errorf("otel provider: %w", err))
		}
		cancel()
	}
	// 2. Audit logger.
	if s.auditLogger != nil {
		if err := s.auditLogger.Close(); err != nil {
			errs = append(errs, fmt.Errorf("audit logger: %w", err))
		}
	}
	// 3. Identity provider.
	if s.identityProvider != nil {
		if closer, ok := s.identityProvider.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, fmt.Errorf("identity provider %s: %w", s.identityProvider.Name(), err))
			}
		}
	}
	// 4. Shared persistence backend after all persistence-aware subsystems.
	if s.engineBackend != nil {
		if err := s.engineBackend.Close(); err != nil {
			errs = append(errs, fmt.Errorf("engine backend: %w", err))
		}
	}
	// 5. Kafka manager last so any subsystem shutdown can still finish broker I/O.
	if s.kafkaManager != nil {
		s.kafkaManager.Close()
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
// or returns an error if the context is canceled while waiting.
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
	if s == nil || s.kafkaManager == nil {
		return cluster
	}
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

// Supported handlers live in tools_p0.go, tools_p1.go, tools_connect.go,
// tools_lagdiag.go, tools_aclmgmt.go, tools_capability.go, the approval/policy
// registration files, and resources.go.
