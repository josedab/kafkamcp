package app_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/josedab/kafkamcp/internal/app"
	"github.com/josedab/kafkamcp/internal/config"
)

func TestEmitPersistenceWarning_InMemoryBackend(t *testing.T) {
	for _, backend := range []string{"", "inmem", "memory"} {
		t.Run("backend="+backend, func(t *testing.T) {
			cfg := &config.Config{
				Server: config.ServerConfig{
					Persistence: config.PersistenceConfig{Backend: backend},
				},
			}
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			app.EmitPersistenceWarning(cfg, logger)

			msg := buf.String()
			if msg == "" {
				t.Fatal("expected a log message for in-memory backend")
			}
			if !containsSubstring(msg, "VOLATILE STATE") {
				t.Errorf("expected VOLATILE STATE warning for backend %q, got: %s", backend, msg)
			}
		})
	}
}

func TestEmitPersistenceWarning_SQLiteWithPath(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Persistence: config.PersistenceConfig{Backend: "sqlite", Path: "/var/kafkamcp/state.db"},
		},
	}
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	app.EmitPersistenceWarning(cfg, logger)

	msg := buf.String()
	if !containsSubstring(msg, "sqlite backend enabled") {
		t.Errorf("expected sqlite info log, got: %s", msg)
	}
	// Must NOT contain a VOLATILE STATE warning.
	if containsSubstring(msg, "VOLATILE STATE") {
		t.Errorf("unexpected VOLATILE STATE warning for sqlite with path, got: %s", msg)
	}
}

func TestEmitPersistenceWarning_SQLiteWithoutPath(t *testing.T) {
	cfg := &config.Config{
		Server: config.ServerConfig{
			Persistence: config.PersistenceConfig{Backend: "sqlite"},
		},
	}
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	app.EmitPersistenceWarning(cfg, logger)

	msg := buf.String()
	if !containsSubstring(msg, "falling back to in-memory") {
		t.Errorf("expected fallback warning for sqlite without path, got: %s", msg)
	}
}

func containsSubstring(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && contains(s, sub))
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
