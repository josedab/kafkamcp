package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/engine"
	"github.com/kafkamcp/kafkamcp/internal/engine/backend"
)

// Entry represents a single audit log entry.
type Entry struct {
	Timestamp      string         `json:"timestamp"`
	AgentID        string         `json:"agent_id"`
	IdentityMethod string         `json:"identity_method,omitempty"` // How AgentID was verified ("none", "static", "oidc", "mtls")
	Issuer         string         `json:"issuer,omitempty"`          // OIDC issuer URL (when IdentityMethod == "oidc")
	Subject        string         `json:"subject,omitempty"`         // JWT sub claim or static agent ID; differs from AgentID when agent_id_claim overrides sub
	Tool           string         `json:"tool"`
	Parameters     map[string]any `json:"parameters,omitempty"`
	Cluster        string         `json:"cluster,omitempty"`
	Result         Result         `json:"result"`
	LatencyMs      int64          `json:"latency_ms"`
	PolicyMatched  string         `json:"policy_matched,omitempty"`
}

// Result describes the outcome of a tool call.
type Result struct {
	Status       string `json:"status"`
	MessageCount int    `json:"message_count,omitempty"`
	Error        string `json:"error,omitempty"`
}

// Logger handles structured audit logging with a ring buffer for recent entries.
type Logger struct {
	mu         sync.RWMutex
	entries    []Entry
	maxEntries int
	pos        int
	full       bool
	fileWriter io.Writer
	writeCh    chan []byte // buffered channel for async file writes
	done       chan struct{}
	slogger    *slog.Logger
	enabled    bool

	// persistent ring buffer state (optional)
	store  *engine.Store[Entry]
	seq    int64 // monotonic counter for store keys (formatted as zero-padded big-endian decimal)
	oldest int64 // smallest persisted key; advanced when trimming for ring eviction
}

// NewLogger creates a new in-memory audit logger.
//
// For deployments that need the recent-entries ring buffer to survive
// restarts, use [NewLoggerWithBackend].
func NewLogger(ctx context.Context, enabled bool, maxEntries int, logFile string, logger *slog.Logger) (*Logger, error) {
	return NewLoggerWithBackend(ctx, enabled, maxEntries, logFile, logger, nil)
}

// NewLoggerWithBackend creates an audit logger that optionally persists
// its ring buffer to the supplied backend.
//
// When be is non-nil, previously-persisted entries are loaded eagerly
// (capped at maxEntries — older entries are dropped). Subsequent Log
// calls append to the backend and trim oldest entries on overflow,
// preserving the bounded retention semantics of the in-memory ring.
//
// When be is nil this is equivalent to [NewLogger].
func NewLoggerWithBackend(ctx context.Context, enabled bool, maxEntries int, logFile string, logger *slog.Logger, be backend.Backend) (*Logger, error) {
	l := &Logger{
		enabled:    enabled,
		maxEntries: maxEntries,
		entries:    make([]Entry, 0, min(maxEntries, 100)),
		slogger:    logger,
	}

	if !enabled {
		return l, nil
	}

	if logFile != "" {
		cleanPath := filepath.Clean(logFile)
		if strings.Contains(cleanPath, "..") {
			return nil, fmt.Errorf("audit log file path must not contain '..': %s", logFile)
		}
		f, err := os.OpenFile(cleanPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, fmt.Errorf("opening audit log file: %w", err)
		}
		l.fileWriter = f
		l.writeCh = make(chan []byte, 256)
		l.done = make(chan struct{})
		go l.fileWriteLoop(ctx)
	}

	if be != nil {
		l.store = engine.NewStore[Entry](be, "audit.entries")
		if err := l.loadFromBackend(ctx); err != nil {
			return nil, fmt.Errorf("loading persisted audit entries: %w", err)
		}
	}

	return l, nil
}

// loadFromBackend restores the ring buffer from the backend. Entries
// are ordered by their key (a zero-padded monotonic counter), and only
// the most recent maxEntries are kept in memory; older keys are
// trimmed off-disk for tidy retention.
func (l *Logger) loadFromBackend(ctx context.Context) error {
	all, err := l.store.List(ctx, "")
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			return nil
		}
		return err
	}
	if len(all) == 0 {
		return nil
	}

	// Collect keys and sort lexicographically; zero-padding keeps that
	// order numerically correct.
	keys := make([]string, 0, len(all))
	for k := range all {
		keys = append(keys, k)
	}
	sortStrings(keys)

	// Track the max sequence we've seen so future writes don't collide.
	if n, err := strconv.ParseInt(keys[len(keys)-1], 10, 64); err == nil {
		l.seq = n
	}
	l.oldest, _ = strconv.ParseInt(keys[0], 10, 64)

	// Trim anything beyond the configured ring size (newest tail wins).
	if len(keys) > l.maxEntries {
		trim := len(keys) - l.maxEntries
		for _, k := range keys[:trim] {
			if err := l.store.Delete(ctx, k); err != nil && l.slogger != nil {
				l.slogger.Warn("audit: failed to trim oldest persisted entry on load", "key", k, "error", err)
			}
		}
		keys = keys[trim:]
		l.oldest, _ = strconv.ParseInt(keys[0], 10, 64)
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	for _, k := range keys {
		l.entries = append(l.entries, all[k])
	}
	l.pos = len(l.entries)
	if l.pos >= l.maxEntries {
		l.full = true
	}
	return nil
}

// keyForSeq returns the 20-char zero-padded decimal key used to store
// an entry in the backend. 20 chars covers the full int64 range without
// risk of lexicographic mis-ordering.
func keyForSeq(n int64) string {
	return fmt.Sprintf("%020d", n)
}

// sortStrings is a tiny inline replacement for sort.Strings to avoid
// pulling in the sort package for a single call.
func sortStrings(ss []string) {
	// Insertion sort is fine for the moderate sizes (<= maxEntries).
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && ss[j-1] > ss[j]; j-- {
			ss[j-1], ss[j] = ss[j], ss[j-1]
		}
	}
}

// fileWriteLoop drains the write channel and writes to the file writer.
func (l *Logger) fileWriteLoop(ctx context.Context) {
	defer close(l.done)
	for {
		select {
		case data, ok := <-l.writeCh:
			if !ok {
				return
			}
			if _, err := l.fileWriter.Write(data); err != nil {
				l.slogger.Error("audit write failed", "error", err)
			}
		case <-ctx.Done():
			// Drain remaining items before exiting
			for {
				select {
				case data, ok := <-l.writeCh:
					if !ok {
						return
					}
					if _, err := l.fileWriter.Write(data); err != nil {
						l.slogger.Error("audit write failed", "error", err)
					}
				default:
					return
				}
			}
		}
	}
}

// Log records an audit entry.
func (l *Logger) Log(entry Entry) {
	if !l.enabled {
		return
	}

	if entry.Timestamp == "" {
		entry.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}

	// Write to ring buffer under lock (fast in-memory operation only)
	l.mu.Lock()
	if len(l.entries) < l.maxEntries {
		l.entries = append(l.entries, entry)
		l.pos = len(l.entries)
	} else {
		l.entries[l.pos%l.maxEntries] = entry
		l.pos++
		if l.pos >= l.maxEntries {
			l.full = true
		}
	}
	l.persistLocked(entry)
	l.mu.Unlock()

	// Async file write via buffered channel (non-blocking)
	if l.writeCh != nil {
		data, err := json.Marshal(entry)
		if err == nil {
			data = append(data, '\n')
			select {
			case l.writeCh <- data:
			default:
				l.slogger.Warn("audit write channel full, dropping entry")
			}
		} else {
			l.slogger.Error("failed to marshal audit entry", "error", err)
		}
	}

	// Also emit via slog at debug level
	l.slogger.Debug("audit",
		"agent_id", entry.AgentID,
		"tool", entry.Tool,
		"status", entry.Result.Status,
		"latency_ms", entry.LatencyMs,
	)
}

// persistLocked writes the entry to the backend and trims the oldest
// persisted entry when the ring overflows. Called under l.mu.
func (l *Logger) persistLocked(entry Entry) {
	if l.store == nil {
		return
	}
	l.seq++
	key := keyForSeq(l.seq)
	if err := l.store.Set(context.Background(), key, entry); err != nil {
		if l.slogger != nil {
			l.slogger.Warn("audit: persist entry failed", "key", key, "error", err)
		}
		return
	}
	// Trim oldest when ring is over capacity.
	if l.full && l.oldest > 0 {
		l.oldest++
		dropKey := keyForSeq(l.oldest - 1)
		if err := l.store.Delete(context.Background(), dropKey); err != nil && l.slogger != nil {
			l.slogger.Warn("audit: trim oldest persisted entry failed", "key", dropKey, "error", err)
		}
	} else if l.oldest == 0 {
		l.oldest = 1
	}
}

// RecentEntries returns the most recent N audit entries.
func (l *Logger) RecentEntries(n int) []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	count := len(l.entries)

	if n > count {
		n = count
	}
	if n <= 0 {
		return nil
	}

	result := make([]Entry, 0, n)

	// Read from ring buffer in reverse chronological order
	for i := 0; i < n; i++ {
		idx := l.pos - 1 - i
		if idx < 0 {
			idx += count
		}
		idx %= count
		result = append(result, l.entries[idx])
	}

	return result
}

// Recent is an alias for RecentEntries.
func (l *Logger) Recent(n int) []Entry {
	return l.RecentEntries(n)
}

// Close closes the audit logger, draining any pending writes.
func (l *Logger) Close() error {
	if l.writeCh != nil {
		close(l.writeCh)
		<-l.done // wait for pending writes to complete
	}
	if closer, ok := l.fileWriter.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
