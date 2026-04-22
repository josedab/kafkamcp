package audit

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Entry represents a single audit log entry.
type Entry struct {
	Timestamp    string         `json:"timestamp"`
	AgentID      string         `json:"agent_id"`
	Tool         string         `json:"tool"`
	Parameters   map[string]any `json:"parameters,omitempty"`
	Cluster      string         `json:"cluster,omitempty"`
	Result       Result         `json:"result"`
	LatencyMs    int64          `json:"latency_ms"`
	PolicyMatched string        `json:"policy_matched,omitempty"`
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
}

// NewLogger creates a new audit logger.
func NewLogger(enabled bool, maxEntries int, logFile string, logger *slog.Logger) (*Logger, error) {
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
		go l.fileWriteLoop()
	}

	return l, nil
}

// fileWriteLoop drains the write channel and writes to the file writer.
func (l *Logger) fileWriteLoop() {
	defer close(l.done)
	for data := range l.writeCh {
		if _, err := l.fileWriter.Write(data); err != nil {
			l.slogger.Error("audit write failed", "error", err)
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
		idx = idx % count
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
