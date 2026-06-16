package engine

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/josedab/kafkamcp/internal/engine/backend"
)

// Config controls which persistence backend is opened by [Open].
type Config struct {
	// Backend selects the driver: "inmem" (default) or "sqlite".
	Backend string

	// Path is the on-disk path for file-backed drivers (sqlite).
	Path string
}

// DefaultConfig returns the legacy in-memory configuration.
func DefaultConfig() Config { return Config{Backend: "inmem"} }

// Open instantiates the backend selected by cfg. The returned backend is
// owned by the caller and must be closed at shutdown. When cfg is the
// zero value, an in-memory backend is returned.
func Open(cfg Config, logger *slog.Logger) (backend.Backend, error) {
	driver := strings.ToLower(strings.TrimSpace(cfg.Backend))
	if driver == "" {
		driver = "inmem"
	}
	switch driver {
	case "inmem", "memory", "in-memory":
		if logger != nil {
			logger.Info("engine persistence: using in-memory backend (state will be lost on restart)")
		}
		return backend.NewInMemory(), nil
	case "sqlite", "sqlite3":
		if cfg.Path == "" {
			return nil, fmt.Errorf("kafkamcp/engine: server.persistence.path is required when backend=sqlite")
		}
		be, err := backend.OpenSQLite(backend.SQLiteOptions{Path: cfg.Path})
		if err != nil {
			return nil, err
		}
		if logger != nil {
			logger.Info("engine persistence: using sqlite backend", "path", cfg.Path)
		}
		return be, nil
	default:
		return nil, fmt.Errorf("kafkamcp/engine: unknown backend %q (expected inmem|sqlite)", cfg.Backend)
	}
}
