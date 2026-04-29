package sessionreplay

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// ToolCallRecord captures a single tool invocation.
type ToolCallRecord struct {
	Sequence   int            `json:"sequence"`
	Tool       string         `json:"tool"`
	Args       map[string]any `json:"args,omitempty"`
	Response   any            `json:"response,omitempty"`
	IsError    bool           `json:"is_error"`
	ErrorMsg   string         `json:"error_message,omitempty"`
	StartedAt  time.Time      `json:"started_at"`
	DurationMs float64        `json:"duration_ms"`
}

// Session represents a recorded agent session.
type Session struct {
	ID        string           `json:"id"`
	AgentID   string           `json:"agent_id"`
	StartedAt time.Time        `json:"started_at"`
	EndedAt   *time.Time       `json:"ended_at,omitempty"`
	Status    string           `json:"status"` // recording, completed, replaying
	Calls     []ToolCallRecord `json:"calls"`
	Metadata  map[string]any   `json:"metadata,omitempty"`
}

// SessionSummary is a compact representation for listing.
type SessionSummary struct {
	ID         string    `json:"id"`
	AgentID    string    `json:"agent_id"`
	StartedAt  time.Time `json:"started_at"`
	Status     string    `json:"status"`
	TotalCalls int       `json:"total_calls"`
	ErrorCount int       `json:"error_count"`
	DurationMs float64   `json:"total_duration_ms"`
}

// ReplayResult summarizes a session replay.
type ReplayResult struct {
	SessionID     string           `json:"session_id"`
	TotalCalls    int              `json:"total_calls"`
	ErrorCalls    int              `json:"error_calls"`
	TotalDuration float64          `json:"total_duration_ms"`
	AvgLatency    float64          `json:"avg_latency_ms"`
	Timeline      []ToolCallRecord `json:"timeline"`
	Bottlenecks   []string         `json:"bottlenecks,omitempty"`
}

// Engine manages session recording and replay.
type Engine struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	logger   *slog.Logger
	nextID   int64
}

// NewEngine creates a new session replay engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		sessions: make(map[string]*Session),
		logger:   logger,
	}
}

// StartSession begins recording a new session.
func (e *Engine) StartSession(agentID string, metadata map[string]any) *Session {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("session_%d", e.nextID)

	s := &Session{
		ID:        id,
		AgentID:   agentID,
		StartedAt: time.Now(),
		Status:    "recording",
		Metadata:  metadata,
	}
	e.sessions[id] = s
	e.logger.Info("session started", "id", id, "agent", agentID)
	return s
}

// RecordCall adds a tool call record to an active session.
func (e *Engine) RecordCall(sessionID, tool string, args map[string]any, response any, isError bool, errorMsg string, durationMs float64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return fmt.Errorf("session %q not found", sessionID)
	}
	if s.Status != "recording" {
		return fmt.Errorf("session %q is not recording (status: %s)", sessionID, s.Status)
	}

	s.Calls = append(s.Calls, ToolCallRecord{
		Sequence:   len(s.Calls) + 1,
		Tool:       tool,
		Args:       args,
		Response:   response,
		IsError:    isError,
		ErrorMsg:   errorMsg,
		StartedAt:  time.Now(),
		DurationMs: durationMs,
	})
	return nil
}

// EndSession completes a recording session.
func (e *Engine) EndSession(sessionID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return fmt.Errorf("session %q not found", sessionID)
	}
	now := time.Now()
	s.EndedAt = &now
	s.Status = "completed"
	return nil
}

// GetSession retrieves a session by ID.
func (e *Engine) GetSession(sessionID string) (*Session, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	return s, nil
}

// ListSessions returns session summaries with optional filtering.
func (e *Engine) ListSessions(agentID string, limit int) []SessionSummary {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var summaries []SessionSummary
	for _, s := range e.sessions {
		if agentID != "" && s.AgentID != agentID {
			continue
		}
		var errorCount int
		var totalDuration float64
		for _, c := range s.Calls {
			if c.IsError {
				errorCount++
			}
			totalDuration += c.DurationMs
		}
		summaries = append(summaries, SessionSummary{
			ID:         s.ID,
			AgentID:    s.AgentID,
			StartedAt:  s.StartedAt,
			Status:     s.Status,
			TotalCalls: len(s.Calls),
			ErrorCount: errorCount,
			DurationMs: totalDuration,
		})
	}
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].StartedAt.After(summaries[j].StartedAt) })
	if len(summaries) > limit {
		summaries = summaries[:limit]
	}
	return summaries
}

// Replay analyzes a completed session for debugging.
func (e *Engine) Replay(sessionID string) (*ReplayResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	s, ok := e.sessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	var errorCalls int
	var totalDuration float64
	for _, c := range s.Calls {
		if c.IsError {
			errorCalls++
		}
		totalDuration += c.DurationMs
	}

	avgLatency := 0.0
	if len(s.Calls) > 0 {
		avgLatency = totalDuration / float64(len(s.Calls))
	}

	var bottlenecks []string
	for _, c := range s.Calls {
		if c.DurationMs > 5000 {
			bottlenecks = append(bottlenecks, fmt.Sprintf("slow call: %s took %.0fms (seq #%d)", c.Tool, c.DurationMs, c.Sequence))
		}
	}

	return &ReplayResult{
		SessionID:     sessionID,
		TotalCalls:    len(s.Calls),
		ErrorCalls:    errorCalls,
		TotalDuration: totalDuration,
		AvgLatency:    avgLatency,
		Timeline:      s.Calls,
		Bottlenecks:   bottlenecks,
	}, nil
}

// DeleteSession removes a session.
func (e *Engine) DeleteSession(sessionID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.sessions[sessionID]; !ok {
		return fmt.Errorf("session %q not found", sessionID)
	}
	delete(e.sessions, sessionID)
	return nil
}
