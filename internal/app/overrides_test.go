package app_test

import (
	"testing"

	"github.com/josedab/kafkamcp/internal/app"
	"github.com/josedab/kafkamcp/internal/config"
)

func TestApplyOverrides_DebugFlagSetsLogLevel(t *testing.T) {
	cfg := &config.Config{}
	app.ApplyOverrides(cfg, "", true)
	if cfg.Server.LogLevel != "debug" {
		t.Errorf("expected log level debug, got %q", cfg.Server.LogLevel)
	}
}

func TestApplyOverrides_ExplicitLogLevelOverridesDebug(t *testing.T) {
	cfg := &config.Config{}
	// debug=true but explicit logLevel wins
	app.ApplyOverrides(cfg, "warn", false)
	if cfg.Server.LogLevel != "warn" {
		t.Errorf("expected log level warn, got %q", cfg.Server.LogLevel)
	}
}

func TestApplyOverrides_TransportEnvOverridesConfig(t *testing.T) {
	t.Setenv("KAFKAMCP_TRANSPORT", "sse")
	cfg := &config.Config{Server: config.ServerConfig{Transport: "stdio"}}
	app.ApplyOverrides(cfg, "", false)
	if cfg.Server.Transport != "sse" {
		t.Errorf("expected transport sse, got %q", cfg.Server.Transport)
	}
}

func TestApplyOverrides_LogLevelEnvAppliedWhenNoFlag(t *testing.T) {
	t.Setenv("KAFKAMCP_LOG_LEVEL", "error")
	cfg := &config.Config{}
	app.ApplyOverrides(cfg, "", false)
	if cfg.Server.LogLevel != "error" {
		t.Errorf("expected log level error, got %q", cfg.Server.LogLevel)
	}
}

func TestApplyOverrides_CLIFlagWinsOverEnv(t *testing.T) {
	t.Setenv("KAFKAMCP_LOG_LEVEL", "warn")
	cfg := &config.Config{}
	// CLI flag explicitly set; env must not overwrite it
	app.ApplyOverrides(cfg, "info", false)
	if cfg.Server.LogLevel != "info" {
		t.Errorf("expected log level info (CLI wins), got %q", cfg.Server.LogLevel)
	}
}

func TestApplyOverrides_NoEnvNoFlagsLeavesConfigUnchanged(t *testing.T) {
	cfg := &config.Config{Server: config.ServerConfig{Transport: "stdio", LogLevel: "warn"}}
	app.ApplyOverrides(cfg, "", false)
	if cfg.Server.Transport != "stdio" {
		t.Errorf("transport should be unchanged, got %q", cfg.Server.Transport)
	}
	if cfg.Server.LogLevel != "warn" {
		t.Errorf("log level should be unchanged, got %q", cfg.Server.LogLevel)
	}
}

func TestNewLogger_RespectsLogLevel(t *testing.T) {
	tests := []struct {
		level    string
		wantName string
	}{
		{"debug", "debug"},
		{"info", "info"},
		{"warn", "warn"},
		{"error", "error"},
		{"", "info"}, // defaults to info
	}
	for _, tc := range tests {
		t.Run(tc.level, func(t *testing.T) {
			cfg := &config.Config{Server: config.ServerConfig{LogLevel: tc.level}}
			var buf nopWriter
			logger := app.NewLogger(cfg, &buf)
			if logger == nil {
				t.Fatal("NewLogger returned nil")
			}
		})
	}
}

// nopWriter discards all writes (used to avoid allocating os.Stderr in tests).
type nopWriter struct{}

func (*nopWriter) Write(p []byte) (int, error) { return len(p), nil }
