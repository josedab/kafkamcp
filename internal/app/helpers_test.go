package app_test

// This file contains shared test helpers used across the app_test package.
// It is NOT a test file (no _test.go suffix convention violation) — Go
// excludes files named *_test.go from regular builds automatically.

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	mcpgoserver "github.com/mark3labs/mcp-go/server"

	"github.com/josedab/kafkamcp/internal/metrics"
)

// fakeAppServer is a test double for app.AppServer that records Close calls
// and returns configurable errors. All other methods are benign no-ops.
type fakeAppServer struct {
	closed       bool
	closeErr     error
	healthErr    error
	readinessErr error
}

func (f *fakeAppServer) MCPServer() *mcpgoserver.MCPServer {
	return mcpgoserver.NewMCPServer("test", "0.0.0")
}

func (f *fakeAppServer) Metrics() *metrics.Metrics {
	return metrics.New()
}

func (f *fakeAppServer) HealthCheck(_ context.Context) error    { return f.healthErr }
func (f *fakeAppServer) ReadinessCheck(_ context.Context) error { return f.readinessErr }
func (f *fakeAppServer) RegisteredToolCount() int               { return 0 }
func (f *fakeAppServer) SkippedToolCount() int                  { return 0 }

func (f *fakeAppServer) Close() error {
	f.closed = true
	return f.closeErr
}

func (f *fakeAppServer) MTLSIdentityMiddleware(next http.Handler) http.Handler { return next }

func (f *fakeAppServer) BuildMTLSTLSConfig(_ string) (*tls.Config, error) { return nil, nil }

// fakeStdioRunner is a test double for app.StdioRunner.
type fakeStdioRunner struct {
	called bool
	err    error
}

func (f *fakeStdioRunner) Listen(_ context.Context, _ io.Reader, _ io.Writer) error {
	f.called = true
	return f.err
}

// noopLogger returns a discard logger so tests don't pollute stdout.
func noopLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// newShortCtx returns a context canceled after d, and registers cleanup with t.
func newShortCtx(t *testing.T, d time.Duration) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx, cancel
}
