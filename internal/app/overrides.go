// Package app contains the production entry-point wiring for KafkaMCP:
// config overrides, logging setup, transport selection, middleware assembly,
// metrics server lifecycle, and graceful shutdown. Keeping this logic in a
// dedicated package makes it testable without network or Kafka dependencies.
package app

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/josedab/kafkamcp/internal/config"
)

// ApplyOverrides mutates cfg in-place by applying CLI-flag and environment-
// variable overrides. The precedence order (highest → lowest) is:
//
//  1. --log-level / --debug CLI flags (logLevel, debug parameters)
//  2. KAFKAMCP_TRANSPORT and KAFKAMCP_LOG_LEVEL environment variables
//  3. Values already present in cfg (from the config file)
//
// Call ApplyOverrides once, immediately after config.Load.
func ApplyOverrides(cfg *config.Config, logLevel string, debug bool) {
	// --debug is a shorthand for --log-level debug.
	if debug && logLevel == "" {
		logLevel = "debug"
	}
	// Explicit CLI flag beats the config file.
	if logLevel != "" {
		cfg.Server.LogLevel = logLevel
	}

	// KAFKAMCP_TRANSPORT overrides the config-file transport.
	if t := os.Getenv("KAFKAMCP_TRANSPORT"); t != "" {
		cfg.Server.Transport = t
	}

	// KAFKAMCP_LOG_LEVEL overrides the config-file log level, but an explicit
	// CLI flag (logLevel != "") already won above so skip if already set.
	if l := os.Getenv("KAFKAMCP_LOG_LEVEL"); l != "" && logLevel == "" {
		cfg.Server.LogLevel = l
	}
}

// NewLogger constructs a structured JSON logger at the level specified in cfg.
// w is the destination writer; pass os.Stderr for production use.
func NewLogger(cfg *config.Config, w io.Writer) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(cfg.Server.LogLevel)) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
}
