package app

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mcpgoserver "github.com/mark3labs/mcp-go/server"

	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/kafka"
	"github.com/josedab/kafkamcp/internal/metrics"
	mcpserver "github.com/josedab/kafkamcp/internal/server"
)

// AppServer is satisfied by *mcpserver.Server. The interface is intentionally
// narrow: it contains only what the runner needs, so tests can provide fakes
// without reimplementing the full server.
type AppServer interface {
	// MCPServer returns the underlying mcp-go server used by transport layers.
	MCPServer() *mcpgoserver.MCPServer
	// Metrics returns the Prometheus metrics registry.
	Metrics() *metrics.Metrics
	// HealthCheck pings the default Kafka cluster.
	HealthCheck(ctx context.Context) error
	// ReadinessCheck reports whether the server is ready to serve traffic.
	ReadinessCheck(ctx context.Context) error
	// RegisteredToolCount returns the number of tools exposed to MCP clients.
	RegisteredToolCount() int
	// SkippedToolCount returns the number of tools filtered by the active pack.
	SkippedToolCount() int
	// Close shuts down the server and all subsystems.
	Close() error
	// MTLSIdentityMiddleware wraps h and attaches the verified mTLS peer
	// identity to the request context. It is a no-op when mTLS is not
	// configured.
	MTLSIdentityMiddleware(h http.Handler) http.Handler
	// BuildMTLSTLSConfig constructs a *tls.Config that requires and verifies
	// client certificates. Returns nil when mTLS is not the active identity
	// provider.
	BuildMTLSTLSConfig(caBundlePath string) (*tls.Config, error)
}

// StdioRunner abstracts the mcp-go stdio server so it can be replaced in
// tests without opening real file descriptors.
type StdioRunner interface {
	Listen(ctx context.Context, r io.Reader, w io.Writer) error
}

// Deps holds injectable factories and functions used by Run. Every field has a
// sensible production default; tests substitute fakes for the fields that
// would otherwise open network connections or Kafka clients.
type Deps struct {
	// NewKafkaManager creates the Kafka manager. Defaults to kafka.NewManager.
	NewKafkaManager func(cfg *config.Config, logger *slog.Logger) (kafka.KafkaManager, error)

	// NewAppServer creates the MCP server. Defaults to mcpserver.New.
	NewAppServer func(cfg *config.Config, km kafka.KafkaManager, logger *slog.Logger, version string) (AppServer, error)

	// NewSignalCtx creates a context that is canceled when one of the
	// supplied OS signals is received. Defaults to signal.NotifyContext.
	NewSignalCtx func(parent context.Context, signals ...os.Signal) (context.Context, context.CancelFunc)

	// ListenAndServe starts the main HTTP server (plain-text). Defaults to
	// (*http.Server).ListenAndServe.
	ListenAndServe func(srv *http.Server) error

	// ListenAndServeTLS starts the main HTTPS server. Defaults to
	// (*http.Server).ListenAndServeTLS.
	ListenAndServeTLS func(srv *http.Server, certFile, keyFile string) error

	// MetricsListen starts the metrics HTTP server. Defaults to
	// (*http.Server).ListenAndServe.
	MetricsListen func(srv *http.Server) error

	// NewStdioRunner creates the stdio MCP transport. Defaults to
	// mcpgoserver.NewStdioServer.
	NewStdioRunner func(srv *mcpgoserver.MCPServer) StdioRunner

	// Stdin is the reader for the stdio transport. Defaults to os.Stdin.
	Stdin io.Reader

	// Stdout is the writer for the stdio transport. Defaults to os.Stdout.
	Stdout io.Writer

	// ShutdownTimeout is the maximum time to drain connections during graceful
	// shutdown. Defaults to 10 seconds.
	ShutdownTimeout time.Duration
}

// applyDefaults fills in zero fields with production defaults.
func (d *Deps) applyDefaults() {
	if d.NewKafkaManager == nil {
		d.NewKafkaManager = func(cfg *config.Config, logger *slog.Logger) (kafka.KafkaManager, error) {
			return kafka.NewManager(cfg, logger)
		}
	}
	if d.NewAppServer == nil {
		d.NewAppServer = func(cfg *config.Config, km kafka.KafkaManager, logger *slog.Logger, version string) (AppServer, error) {
			return mcpserver.New(cfg, km, logger, version)
		}
	}
	if d.NewSignalCtx == nil {
		d.NewSignalCtx = signal.NotifyContext
	}
	if d.ListenAndServe == nil {
		d.ListenAndServe = func(srv *http.Server) error { return srv.ListenAndServe() }
	}
	if d.ListenAndServeTLS == nil {
		d.ListenAndServeTLS = func(srv *http.Server, cert, key string) error {
			return srv.ListenAndServeTLS(cert, key)
		}
	}
	if d.MetricsListen == nil {
		d.MetricsListen = func(srv *http.Server) error { return srv.ListenAndServe() }
	}
	if d.NewStdioRunner == nil {
		d.NewStdioRunner = func(srv *mcpgoserver.MCPServer) StdioRunner {
			return mcpgoserver.NewStdioServer(srv)
		}
	}
	if d.Stdin == nil {
		d.Stdin = os.Stdin
	}
	if d.Stdout == nil {
		d.Stdout = os.Stdout
	}
	if d.ShutdownTimeout == 0 {
		d.ShutdownTimeout = 10 * time.Second
	}
}

// DefaultDeps returns a Deps populated with all production defaults.
func DefaultDeps() Deps {
	var d Deps
	d.applyDefaults()
	return d
}

// Run is the production entry point. It:
//
//  1. Validates the TLS cert/key pairing.
//  2. Enforces the fail-closed security posture (cfg.TransportSecurityError)
//     before creating any listener or Kafka client.
//  3. Creates the Kafka manager.
//  4. Creates the MCP server.
//  5. Emits startup warnings (tool packs, policies, experimental features,
//     persistence backend).
//  6. Installs OS signal handling on the context.
//  7. Starts the metrics server for non-stdio transports.
//  8. Runs the selected transport until ctx is canceled or an error occurs.
//  9. Shuts down all subsystems in reverse order with a bounded 10 s drain.
func Run(ctx context.Context, cfg *config.Config, version string, logger *slog.Logger, deps Deps) (runErr error) {
	deps.applyDefaults()

	// ── Step 1: TLS pairing (cert + key must appear together) ────────────────
	if err := ValidateTLSPairing(cfg); err != nil {
		return err
	}
	if err := ValidateMTLSRequiresTLS(cfg); err != nil {
		return err
	}

	// ── Step 2: Fail-closed security check ───────────────────────────────────
	// Must happen before any listener is opened or Kafka client is created.
	if err := cfg.TransportSecurityError(); err != nil {
		return err
	}
	if cfg.NetworkTransport() && cfg.UnsafeNetworkOverrideEnabled() {
		logger.Warn("UNSAFE OVERRIDE: network transport is running WITHOUT enforced cryptographic identity/default-deny " +
			"(server.dangerously_allow_unauthenticated_network is enabled) — LOCAL DEVELOPMENT ONLY, never use in production")
	}

	// ── Step 3: Kafka manager ─────────────────────────────────────────────────
	km, err := deps.NewKafkaManager(cfg, logger)
	if err != nil {
		if isConnectionRefused(err) {
			bootstrap := ""
			if len(cfg.Clusters) > 0 {
				bootstrap = cfg.Clusters[0].BootstrapServers
			}
			return fmt.Errorf("cannot connect to Kafka at %s: %w\n\n"+
				"Hint: Run 'make docker-dev' to start a local Kafka cluster, "+
				"or verify that bootstrap_servers in your config points to a running broker.",
				bootstrap, err)
		}
		return fmt.Errorf("creating kafka manager: %w", err)
	}

	// ── Step 4: MCP server ────────────────────────────────────────────────────
	srv, err := deps.NewAppServer(cfg, km, logger, version)
	if err != nil {
		if km != nil {
			km.Close()
		}
		return fmt.Errorf("creating server: %w", err)
	}
	defer func() {
		if cerr := srv.Close(); cerr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("closing server: %w", cerr))
		}
	}()

	// ── Step 5: Startup warnings ──────────────────────────────────────────────
	logger.Info("tool packs configured",
		"packs", cfg.Server.ToolPacks,
		"registered_tools", srv.RegisteredToolCount(),
		"skipped_tools", srv.SkippedToolCount())

	emitToolPackWarnings(cfg, logger)
	emitPolicyWarnings(cfg, logger)
	EmitPersistenceWarning(cfg, logger)

	// ── Step 6: Signal context ────────────────────────────────────────────────
	signalCtx, cancelSignal := deps.NewSignalCtx(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancelSignal()

	// ── Step 7: Metrics server ────────────────────────────────────────────────
	// ── Step 8: Transport ─────────────────────────────────────────────────────
	switch cfg.Server.Transport {
	case "stdio":
		logger.Info("serving via stdio transport")
		runner := deps.NewStdioRunner(srv.MCPServer())
		return runner.Listen(signalCtx, deps.Stdin, deps.Stdout)

	case "sse", "streamable-http":
		return runNetworkTransports(signalCtx, cfg, srv, logger, deps)

	default:
		return fmt.Errorf("unsupported transport: %s", cfg.Server.Transport)
	}
}

func runNetworkTransports(ctx context.Context, cfg *config.Config, srv AppServer, logger *slog.Logger, deps Deps) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	metricsSrv := NewMetricsServer(cfg, srv, logger)
	metricsErrCh := make(chan error, 1)
	go func() {
		logger.Info("metrics server listening", "address", metricsSrv.Addr)
		err := deps.MetricsListen(metricsSrv)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		metricsErrCh <- err
	}()

	transportErrCh := make(chan error, 1)
	go func() {
		transportErrCh <- runHTTPTransport(runCtx, cfg, srv, logger, deps)
	}()

	var transportErr error
	var metricsErr error
	metricsDone := false
	select {
	case transportErr = <-transportErrCh:
		cancel()
	case metricsErr = <-metricsErrCh:
		metricsDone = true
		if metricsErr == nil && ctx.Err() == nil {
			metricsErr = errors.New("metrics server stopped unexpectedly")
		}
		cancel()
		select {
		case transportErr = <-transportErrCh:
		case <-time.After(deps.ShutdownTimeout):
			transportErr = errors.New("timed out waiting for transport shutdown")
		}
	}

	shutCtx, shutCancel := context.WithTimeout(context.Background(), deps.ShutdownTimeout)
	metricsShutdownErr := metricsSrv.Shutdown(shutCtx)
	shutCancel()
	if errors.Is(metricsShutdownErr, http.ErrServerClosed) {
		metricsShutdownErr = nil
	}

	if !metricsDone {
		select {
		case metricsErr = <-metricsErrCh:
		case <-time.After(deps.ShutdownTimeout):
			metricsErr = errors.New("timed out waiting for metrics server shutdown")
		}
	}
	return errors.Join(transportErr, metricsErr, metricsShutdownErr)
}

// runHTTPTransport starts the SSE or streamable-HTTP MCP server and blocks
// until signalCtx is canceled or a fatal error occurs.
func runHTTPTransport(ctx context.Context, cfg *config.Config, srv AppServer, logger *slog.Logger, deps Deps) error {
	addr := cfg.Server.Address()
	logger.Info("serving via HTTP transport", "type", cfg.Server.Transport, "address", addr)

	var baseHandler http.Handler
	if cfg.Server.Transport == "sse" {
		baseHandler = mcpgoserver.NewSSEServer(srv.MCPServer())
	} else {
		baseHandler = mcpgoserver.NewStreamableHTTPServer(srv.MCPServer())
	}

	maxBodyBytes := int64(cfg.Server.MaxMessageSize)
	if maxBodyBytes <= 0 {
		maxBodyBytes = 1 << 20
	}
	handler := BuildHTTPHandler(baseHandler, srv.MTLSIdentityMiddleware, maxBodyBytes)

	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	var serve func() error
	if cfg.Server.TLSCert != "" && cfg.Server.TLSKey != "" {
		// Build TLS config; layer on mTLS if the identity provider requires it.
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13}
		if mtlsCfg, err := srv.BuildMTLSTLSConfig(cfg.Policies.Identity.MTLS.CABundle); err != nil {
			return fmt.Errorf("configuring mTLS: %w", err)
		} else if mtlsCfg != nil {
			tlsCfg = mtlsCfg
		}
		httpSrv.TLSConfig = tlsCfg

		logger.Info("TLS enabled", "cert", cfg.Server.TLSCert, "key", cfg.Server.TLSKey)
		serve = func() error {
			return deps.ListenAndServeTLS(httpSrv, cfg.Server.TLSCert, cfg.Server.TLSKey)
		}
	} else {
		serve = func() error { return deps.ListenAndServe(httpSrv) }
	}

	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- serve()
	}()

	select {
	case err := <-serveErrCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP server error: %w", err)
		}
		return nil
	case <-ctx.Done():
		logger.Info("shutting down HTTP server")
		shutCtx, cancel := context.WithTimeout(context.Background(), deps.ShutdownTimeout)
		shutdownErr := httpSrv.Shutdown(shutCtx)
		cancel()
		if shutdownErr != nil {
			_ = httpSrv.Close()
		}
		var serveErr error
		select {
		case serveErr = <-serveErrCh:
		case <-time.After(deps.ShutdownTimeout):
			serveErr = errors.New("timed out waiting for HTTP server shutdown")
		}
		if errors.Is(serveErr, http.ErrServerClosed) {
			serveErr = nil
		}
		return errors.Join(shutdownErr, serveErr)
	}
}

// emitToolPackWarnings logs deprecation notices for obsolete tool-pack aliases.
func emitToolPackWarnings(cfg *config.Config, logger *slog.Logger) {
	for _, p := range cfg.Server.ToolPacks {
		if msg, ok := config.DeprecatedToolPacks[strings.ToLower(strings.TrimSpace(p))]; ok {
			logger.Warn("deprecated tool pack in use", "pack", p, "guidance", msg)
		}
	}
}

// emitPolicyWarnings logs insecure-defaults warnings when default_deny is off.
func emitPolicyWarnings(cfg *config.Config, logger *slog.Logger) {
	if cfg.Policies.DefaultDeny {
		return
	}
	logger.Warn("running with default_deny=false — all agents have unrestricted access; enable policies.default_deny for production use")

	for _, agent := range cfg.Policies.Agents {
		if agent.RateLimit.RequestsPerMinute > 0 {
			return
		}
	}
	logger.Warn("INSECURE DEFAULTS: default_deny is false and no rate limits are configured — " +
		"this installation has zero access control; configure policies for production use")
}

// isConnectionRefused reports whether err looks like a Kafka TCP connection failure.
func isConnectionRefused(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "dial tcp")
}
