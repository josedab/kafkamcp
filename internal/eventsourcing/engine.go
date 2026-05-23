// Package eventsourcing provides event-sourced agent action recording with CQRS
// projections, materialized views for session replay and analytics, and JSONL
// export for LLM fine-tuning.
package eventsourcing

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ActionEvent represents a single agent action to be event-sourced.
type ActionEvent struct {
	ID         string         `json:"id"`
	AgentID    string         `json:"agent_id"`
	SessionID  string         `json:"session_id,omitempty"`
	Tool       string         `json:"tool"`
	Parameters map[string]any `json:"parameters,omitempty"`
	Result     ActionResult   `json:"result"`
	DurationMs int64          `json:"duration_ms"`
	Context    AgentContext   `json:"context,omitempty"`
	Timestamp  time.Time      `json:"timestamp"`
	Sequence   int64          `json:"sequence"`
}

// ActionResult captures the outcome of an action.
type ActionResult struct {
	Status       string `json:"status"` // success, error
	MessageCount int    `json:"message_count,omitempty"`
	Error        string `json:"error,omitempty"`
	ByteSize     int64  `json:"byte_size,omitempty"`
}

// AgentContext provides additional context about the agent.
type AgentContext struct {
	AgentType string            `json:"agent_type,omitempty"`
	Cluster   string            `json:"cluster,omitempty"`
	Topic     string            `json:"topic,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// SessionView is a materialized view of an agent session.
type SessionView struct {
	SessionID    string         `json:"session_id"`
	AgentID      string         `json:"agent_id"`
	Actions      []ActionEvent  `json:"actions"`
	ToolCounts   map[string]int `json:"tool_counts"`
	TotalActions int            `json:"total_actions"`
	ErrorCount   int            `json:"error_count"`
	StartTime    time.Time      `json:"start_time"`
	EndTime      time.Time      `json:"end_time"`
	Duration     time.Duration  `json:"duration"`
}

// ToolUsageView is an analytics view of tool usage.
type ToolUsageView struct {
	Tool          string  `json:"tool"`
	TotalCalls    int64   `json:"total_calls"`
	ErrorRate     float64 `json:"error_rate"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
	P95DurationMs float64 `json:"p95_duration_ms"`
	UniqueAgents  int     `json:"unique_agents"`
}

// ExportFilter defines criteria for filtering events for export.
type ExportFilter struct {
	AgentID   string    `json:"agent_id,omitempty"`
	Tool      string    `json:"tool,omitempty"`
	StartTime time.Time `json:"start_time,omitempty"`
	EndTime   time.Time `json:"end_time,omitempty"`
	Status    string    `json:"status,omitempty"`
	Limit     int       `json:"limit,omitempty"`
}

// CompactionPolicy defines how old events are compacted.
type CompactionPolicy struct {
	MaxEvents    int           `json:"max_events" yaml:"max_events"`
	MaxAge       time.Duration `json:"max_age" yaml:"max_age"`
	SamplingRate float64       `json:"sampling_rate" yaml:"sampling_rate"` // 0.0-1.0
}

// DefaultCompactionPolicy returns sensible defaults.
func DefaultCompactionPolicy() CompactionPolicy {
	return CompactionPolicy{
		MaxEvents:    100000,
		MaxAge:       7 * 24 * time.Hour,
		SamplingRate: 1.0,
	}
}

// Engine is the event sourcing engine for agent actions.
type Engine struct {
	mu        sync.RWMutex
	events    []ActionEvent
	sessions  map[string]*SessionView
	toolUsage map[string]*toolUsageAccum
	sequence  int64
	policy    CompactionPolicy
	logger    *slog.Logger
	topicName string
}

type toolUsageAccum struct {
	totalCalls int64
	errorCalls int64
	totalDurMs int64
	durations  []int64
	agents     map[string]bool
}

// NewEngine creates a new event sourcing engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		events:    make([]ActionEvent, 0),
		sessions:  make(map[string]*SessionView),
		toolUsage: make(map[string]*toolUsageAccum),
		policy:    DefaultCompactionPolicy(),
		logger:    logger,
		topicName: "_kafkamcp.agent.actions",
	}
}

// Record appends an action event to the event store.
func (e *Engine) Record(event ActionEvent) error {
	if event.AgentID == "" {
		return fmt.Errorf("agent_id is required")
	}
	if event.Tool == "" {
		return fmt.Errorf("tool is required")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.sequence++
	event.Sequence = e.sequence
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	if event.ID == "" {
		event.ID = fmt.Sprintf("evt-%d", e.sequence)
	}

	e.events = append(e.events, event)

	// Update session view
	e.updateSessionView(event)

	// Update tool usage projection
	e.updateToolUsage(event)

	// Compact if needed
	if len(e.events) > e.policy.MaxEvents {
		e.compact()
	}

	return nil
}

// GetSession returns the session view for an agent session.
func (e *Engine) GetSession(sessionID string) (*SessionView, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	sv, ok := e.sessions[sessionID]
	return sv, ok
}

// GetToolUsage returns tool usage analytics.
func (e *Engine) GetToolUsage() []ToolUsageView {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []ToolUsageView
	for tool, accum := range e.toolUsage {
		view := ToolUsageView{
			Tool:         tool,
			TotalCalls:   accum.totalCalls,
			UniqueAgents: len(accum.agents),
		}
		if accum.totalCalls > 0 {
			view.ErrorRate = float64(accum.errorCalls) / float64(accum.totalCalls)
			view.AvgDurationMs = float64(accum.totalDurMs) / float64(accum.totalCalls)
		}
		if len(accum.durations) > 0 {
			view.P95DurationMs = percentile(accum.durations, 95)
		}
		result = append(result, view)
	}
	return result
}

// ExportJSONL exports filtered events as JSONL lines for LLM fine-tuning.
func (e *Engine) ExportJSONL(filter ExportFilter) ([][]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var lines [][]byte
	limit := filter.Limit
	if limit <= 0 {
		limit = 10000
	}

	for _, evt := range e.events {
		if len(lines) >= limit {
			break
		}
		if filter.AgentID != "" && evt.AgentID != filter.AgentID {
			continue
		}
		if filter.Tool != "" && evt.Tool != filter.Tool {
			continue
		}
		if !filter.StartTime.IsZero() && evt.Timestamp.Before(filter.StartTime) {
			continue
		}
		if !filter.EndTime.IsZero() && evt.Timestamp.After(filter.EndTime) {
			continue
		}
		if filter.Status != "" && evt.Result.Status != filter.Status {
			continue
		}

		line, err := json.Marshal(evt)
		if err != nil {
			continue
		}
		lines = append(lines, line)
	}
	return lines, nil
}

// Stats returns summary statistics.
func (e *Engine) Stats() map[string]any {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return map[string]any{
		"total_events":   len(e.events),
		"total_sessions": len(e.sessions),
		"total_tools":    len(e.toolUsage),
		"sequence":       e.sequence,
		"topic":          e.topicName,
	}
}

func (e *Engine) updateSessionView(event ActionEvent) {
	sessionID := event.SessionID
	if sessionID == "" {
		sessionID = event.AgentID
	}

	sv, ok := e.sessions[sessionID]
	if !ok {
		sv = &SessionView{
			SessionID:  sessionID,
			AgentID:    event.AgentID,
			ToolCounts: make(map[string]int),
			StartTime:  event.Timestamp,
		}
		e.sessions[sessionID] = sv
	}

	sv.Actions = append(sv.Actions, event)
	sv.TotalActions++
	sv.ToolCounts[event.Tool]++
	sv.EndTime = event.Timestamp
	sv.Duration = sv.EndTime.Sub(sv.StartTime)

	if event.Result.Status == "error" {
		sv.ErrorCount++
	}
}

func (e *Engine) updateToolUsage(event ActionEvent) {
	accum, ok := e.toolUsage[event.Tool]
	if !ok {
		accum = &toolUsageAccum{agents: make(map[string]bool)}
		e.toolUsage[event.Tool] = accum
	}

	accum.totalCalls++
	accum.totalDurMs += event.DurationMs
	accum.agents[event.AgentID] = true

	if event.Result.Status == "error" {
		accum.errorCalls++
	}

	// Keep last 1000 durations for P95
	accum.durations = append(accum.durations, event.DurationMs)
	if len(accum.durations) > 1000 {
		accum.durations = accum.durations[1:]
	}
}

func (e *Engine) compact() {
	cutoff := time.Now().Add(-e.policy.MaxAge)
	var kept []ActionEvent
	for _, evt := range e.events {
		if evt.Timestamp.After(cutoff) {
			kept = append(kept, evt)
		}
	}
	if len(kept) > e.policy.MaxEvents {
		kept = kept[len(kept)-e.policy.MaxEvents:]
	}
	e.events = kept
}

func percentile(values []int64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := make([]int64, len(values))
	copy(sorted, values)
	// Simple selection for P95
	idx := int(float64(len(sorted)) * pct / 100)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return float64(sorted[idx])
}
