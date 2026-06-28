package app

import (
	"log/slog"
	"strings"

	"github.com/josedab/kafkamcp/internal/config"
)

// EmitPersistenceWarning logs a warning or info message that reflects the
// actual configured persistence backend. It is truthful: it warns about
// data loss only when the in-memory backend is active, and confirms sqlite
// persistence when that backend is correctly configured.
func EmitPersistenceWarning(cfg *config.Config, logger *slog.Logger) {
	backend := strings.ToLower(strings.TrimSpace(cfg.Server.Persistence.Backend))
	switch backend {
	case "sqlite":
		if strings.TrimSpace(cfg.Server.Persistence.Path) == "" {
			logger.Warn("PERSISTENCE: backend=sqlite but no persistence.path is set — falling back to in-memory; state will be lost on restart")
		} else {
			logger.Info("PERSISTENCE: sqlite backend enabled — approval grants, idempotency receipts, audit ring-buffer entries, and rate-limit buckets survive restart. See docs/PERSISTENCE.md")
		}
	default:
		logger.Warn("VOLATILE STATE: persistence.backend is in-memory (default) — approval grants, idempotency receipts, audit ring-buffer entries, and rate-limit buckets are lost on restart. Set server.persistence.backend: sqlite for durable guarded operations — see docs/PERSISTENCE.md")
	}
}
