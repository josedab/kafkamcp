package app_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	mcpgoserver "github.com/mark3labs/mcp-go/server"

	"github.com/josedab/kafkamcp/internal/app"
	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/kafka"
)

// minimalCfg returns a config that passes all security checks for the given
// transport without requiring a real Kafka broker.
func minimalCfg(transport string) *config.Config {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Transport: transport,
			Port:      0, // OS-assigned port, never actually bound in tests
		},
		Clusters: []config.ClusterConfig{
			{Name: "test", BootstrapServers: "localhost:9092"},
		},
	}
	if transport != "stdio" {
		// Network transports need default_deny + cryptographic identity, or the
		// unsafe override, to pass TransportSecurityError.
		cfg.Server.DangerouslyAllowUnauthenticatedNetwork = true
	}
	return cfg
}

// stdioOnlyDeps returns a Deps that handles a stdio run without touching
// Kafka or opening any port.
func stdioOnlyDeps(stdioRunner *fakeStdioRunner) app.Deps {
	return app.Deps{
		NewKafkaManager: fakeKafkaFactory,
		NewAppServer:    fakeAppServerFactory,
		NewSignalCtx:    immediateSignalCtx,
		NewStdioRunner: func(_ *mcpgoserver.MCPServer) app.StdioRunner {
			return stdioRunner
		},
		Stdin:  io.NopCloser(nil),
		Stdout: io.Discard,
	}
}

// networkDeps returns a Deps for SSE/streamable-HTTP runs that returns
// http.ErrServerClosed immediately so no real port is opened.
func networkDeps() app.Deps {
	return app.Deps{
		NewKafkaManager: fakeKafkaFactory,
		NewAppServer:    fakeAppServerFactory,
		NewSignalCtx:    immediateSignalCtx,
		ListenAndServe: func(_ *http.Server) error {
			return http.ErrServerClosed
		},
		ListenAndServeTLS: func(_ *http.Server, _, _ string) error {
			return http.ErrServerClosed
		},
		MetricsListen: func(_ *http.Server) error {
			return http.ErrServerClosed
		},
	}
}

func fakeKafkaFactory(cfg *config.Config, logger *slog.Logger) (kafka.KafkaManager, error) {
	return nil, nil // nil KafkaManager is fine; fakeAppServer never calls it
}

func fakeAppServerFactory(cfg *config.Config, km kafka.KafkaManager, logger *slog.Logger, version string) (app.AppServer, error) {
	return &fakeAppServer{}, nil
}

// immediateSignalCtx creates a context that is pre-canceled so that Run
// returns promptly without waiting for a real OS signal.
func immediateSignalCtx(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	cancel() // cancel immediately so the transport exits right away
	return ctx, cancel
}

// ── Transport selection ───────────────────────────────────────────────────────

func TestRun_StdioTransportUsesStdioRunner(t *testing.T) {
	runner := &fakeStdioRunner{}
	deps := stdioOnlyDeps(runner)
	cfg := minimalCfg("stdio")

	err := app.Run(context.Background(), cfg, "test", noopLogger(), deps)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !runner.called {
		t.Error("expected stdio runner to be called")
	}
}

func TestRun_SSETransportCallsListenAndServe(t *testing.T) {
	listenCalled := false
	deps := networkDeps()
	deps.ListenAndServe = func(_ *http.Server) error {
		listenCalled = true
		return http.ErrServerClosed
	}
	cfg := minimalCfg("sse")

	err := app.Run(context.Background(), cfg, "test", noopLogger(), deps)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !listenCalled {
		t.Error("expected ListenAndServe to be called for SSE transport")
	}
}

func TestRun_StreamableHTTPTransportCallsListenAndServe(t *testing.T) {
	listenCalled := false
	deps := networkDeps()
	deps.ListenAndServe = func(_ *http.Server) error {
		listenCalled = true
		return http.ErrServerClosed
	}
	cfg := minimalCfg("streamable-http")

	err := app.Run(context.Background(), cfg, "test", noopLogger(), deps)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !listenCalled {
		t.Error("expected ListenAndServe to be called for streamable-http transport")
	}
}

func TestRun_UnsupportedTransportReturnsError(t *testing.T) {
	deps := networkDeps()
	cfg := minimalCfg("grpc") // not a real transport
	cfg.Server.DangerouslyAllowUnauthenticatedNetwork = true

	err := app.Run(context.Background(), cfg, "test", noopLogger(), deps)
	if err == nil {
		t.Fatal("expected error for unsupported transport, got nil")
	}
}

// ── Fail-closed security check ────────────────────────────────────────────────

func TestRun_SecurityCheckBeforeKafkaManager(t *testing.T) {
	kafkaCreated := false

	deps := app.Deps{
		NewKafkaManager: func(cfg *config.Config, logger *slog.Logger) (kafka.KafkaManager, error) {
			kafkaCreated = true
			return nil, nil
		},
		NewAppServer: fakeAppServerFactory,
		NewSignalCtx: immediateSignalCtx,
	}

	// Network transport without default_deny or cryptographic identity and
	// without the unsafe override → TransportSecurityError must fire.
	cfg := &config.Config{
		Server: config.ServerConfig{
			Transport: "sse",
			Port:      3100,
		},
		Clusters: []config.ClusterConfig{{Name: "test", BootstrapServers: "localhost:9092"}},
	}

	err := app.Run(context.Background(), cfg, "test", noopLogger(), deps)
	if err == nil {
		t.Fatal("expected security error for network transport without identity, got nil")
	}
	if kafkaCreated {
		t.Error("Kafka manager must not be created when security check fails (fail-closed)")
	}
}

// ── TLS pairing validation ────────────────────────────────────────────────────

func TestRun_TLSPairingErrorReturnedBeforeKafka(t *testing.T) {
	kafkaCreated := false
	deps := app.Deps{
		NewKafkaManager: func(cfg *config.Config, logger *slog.Logger) (kafka.KafkaManager, error) {
			kafkaCreated = true
			return nil, nil
		},
		NewAppServer: fakeAppServerFactory,
		NewSignalCtx: immediateSignalCtx,
	}

	cfg := minimalCfg("stdio")
	cfg.Server.TLSCert = "only-cert-no-key.pem" // missing key

	err := app.Run(context.Background(), cfg, "test", noopLogger(), deps)
	if err == nil {
		t.Fatal("expected TLS pairing error, got nil")
	}
	if kafkaCreated {
		t.Error("Kafka manager must not be created when TLS pairing validation fails")
	}
}

// ── Graceful shutdown: server.Close is always called ─────────────────────────

func TestRun_ServerCloseCalledOnExit(t *testing.T) {
	fakeSrv := &fakeAppServer{}
	deps := app.Deps{
		NewKafkaManager: fakeKafkaFactory,
		NewAppServer: func(cfg *config.Config, km kafka.KafkaManager, logger *slog.Logger, version string) (app.AppServer, error) {
			return fakeSrv, nil
		},
		NewSignalCtx: immediateSignalCtx,
		NewStdioRunner: func(_ *mcpgoserver.MCPServer) app.StdioRunner {
			return &fakeStdioRunner{}
		},
		Stdin:  io.NopCloser(nil),
		Stdout: io.Discard,
	}
	cfg := minimalCfg("stdio")

	_ = app.Run(context.Background(), cfg, "test", noopLogger(), deps)

	if !fakeSrv.closed {
		t.Error("expected srv.Close() to be called on Run exit")
	}
}

// ── Shutdown timeout is injectable ───────────────────────────────────────────

func TestRun_ShutdownTimeoutDefault10s(t *testing.T) {
	deps := app.DefaultDeps()
	if deps.ShutdownTimeout != 10*time.Second {
		t.Errorf("expected default ShutdownTimeout=10s, got %v", deps.ShutdownTimeout)
	}
}

// ── Error propagation ─────────────────────────────────────────────────────────

func TestRun_KafkaManagerErrorPropagated(t *testing.T) {
	boom := errors.New("kafka boom")
	deps := app.Deps{
		NewKafkaManager: func(cfg *config.Config, logger *slog.Logger) (kafka.KafkaManager, error) {
			return nil, boom
		},
		NewAppServer: fakeAppServerFactory,
		NewSignalCtx: immediateSignalCtx,
	}
	cfg := minimalCfg("stdio")

	err := app.Run(context.Background(), cfg, "test", noopLogger(), deps)
	if !errors.Is(err, boom) {
		t.Errorf("expected kafka error to be propagated, got: %v", err)
	}
}

func TestRun_AppServerErrorPropagated(t *testing.T) {
	boom := errors.New("server boom")
	deps := app.Deps{
		NewKafkaManager: fakeKafkaFactory,
		NewAppServer: func(cfg *config.Config, km kafka.KafkaManager, logger *slog.Logger, version string) (app.AppServer, error) {
			return nil, boom
		},
		NewSignalCtx: immediateSignalCtx,
	}
	cfg := minimalCfg("stdio")

	err := app.Run(context.Background(), cfg, "test", noopLogger(), deps)
	if !errors.Is(err, boom) {
		t.Errorf("expected app server error to be propagated, got: %v", err)
	}
}
