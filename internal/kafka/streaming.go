package kafka

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// StreamConfig defines parameters for a streaming consumer session.
type StreamConfig struct {
	Topic           string `json:"topic"`
	GroupID         string `json:"group_id"`
	BatchSize       int    `json:"batch_size"`
	MaxMsgPerSec    int    `json:"max_msg_per_sec"`
	MaxMessages     int    `json:"max_messages"`
	TimeoutMs       int    `json:"timeout_ms"`
	Deserializer    string `json:"deserializer"`
	AutoCommit      bool   `json:"auto_commit"`
	AgentID         string `json:"agent_id,omitempty"`
}

// StreamSession represents an active streaming consumer session.
type StreamSession struct {
	ID          string        `json:"id"`
	Topic       string        `json:"topic"`
	GroupID     string        `json:"group_id"`
	AgentID     string        `json:"agent_id,omitempty"`
	Status      string        `json:"status"` // running, paused, stopped
	StartedAt   string        `json:"started_at"`
	MsgCount    int64         `json:"messages_delivered"`
	BatchSize   int           `json:"batch_size"`

	mu         sync.Mutex
	cancel     context.CancelFunc
	paused     atomic.Bool
	stopped    atomic.Bool
	messages   []Message
	msgChan    chan []Message
	errChan    chan error
}

// StreamResult contains the result of a stream poll or control operation.
type StreamResult struct {
	SessionID  string    `json:"session_id"`
	Messages   []Message `json:"messages,omitempty"`
	Count      int       `json:"count"`
	Status     string    `json:"status"`
	Topic      string    `json:"topic"`
	GroupID    string    `json:"group_id"`
	TotalSent  int64     `json:"total_delivered"`
}

// streamSessionManager manages active streaming sessions.
type streamSessionManager struct {
	mu       sync.RWMutex
	sessions map[string]*StreamSession
}

// newStreamSessionManager creates a new stream session manager.
func newStreamSessionManager() *streamSessionManager {
	return &streamSessionManager{
		sessions: make(map[string]*StreamSession),
	}
}

const (
	maxSessionsPerAgent = 10
	maxSessionsGlobal   = 100
)

func generateSessionID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return "stream-" + hex.EncodeToString(b), nil
}

// StartStream creates and starts a new streaming consumer session.
func (m *Manager) StartStream(ctx context.Context, cluster string, cfg StreamConfig) (*StreamResult, error) {
	clCfg, err := m.GetClusterConfig(cluster)
	if err != nil {
		return nil, err
	}

	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10
	}
	if cfg.BatchSize > 100 {
		cfg.BatchSize = 100
	}
	if cfg.MaxMsgPerSec <= 0 {
		cfg.MaxMsgPerSec = 1000
	}
	if cfg.MaxMessages <= 0 {
		cfg.MaxMessages = 1000
	}
	if cfg.MaxMessages > 10000 {
		cfg.MaxMessages = 10000
	}
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = 30000
	}
	if cfg.TimeoutMs > 60000 {
		cfg.TimeoutMs = 60000
	}
	if cfg.Deserializer == "" {
		cfg.Deserializer = "auto"
	}

	groupID := cfg.GroupID
	if groupID == "" {
		b := make([]byte, 8)
		if _, err := rand.Read(b); err != nil {
			return nil, fmt.Errorf("generating group ID: %w", err)
		}
		groupID = fmt.Sprintf("kafkamcp-stream-%s", hex.EncodeToString(b))
	}

	if m.streamMgr == nil {
		m.streamMgr = newStreamSessionManager()
	}

	// Check session capacity and register placeholder while holding the lock
	// to prevent TOCTOU races where concurrent requests both pass the capacity check.
	m.streamMgr.mu.Lock()
	activeCount := 0
	agentCount := 0
	for _, s := range m.streamMgr.sessions {
		if s.Status != "stopped" {
			activeCount++
			if s.AgentID == cfg.AgentID {
				agentCount++
			}
		}
	}
	if activeCount >= maxSessionsGlobal {
		m.streamMgr.mu.Unlock()
		return nil, fmt.Errorf("global streaming session limit (%d) reached", maxSessionsGlobal)
	}
	if cfg.AgentID != "" && agentCount >= maxSessionsPerAgent {
		m.streamMgr.mu.Unlock()
		return nil, fmt.Errorf("per-agent streaming session limit (%d) reached", maxSessionsPerAgent)
	}

	// Reserve a slot by generating a session ID and inserting a placeholder
	// before releasing the lock, preventing concurrent over-allocation.
	sessionID, err := generateSessionID()
	if err != nil {
		m.streamMgr.mu.Unlock()
		return nil, fmt.Errorf("generating session ID: %w", err)
	}
	placeholder := &StreamSession{
		ID:        sessionID,
		Topic:     cfg.Topic,
		AgentID:   cfg.AgentID,
		Status:    "starting",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	m.streamMgr.sessions[sessionID] = placeholder
	m.streamMgr.mu.Unlock()

	opts, err := m.clientOpts(clCfg)
	if err != nil {
		m.streamMgr.mu.Lock()
		delete(m.streamMgr.sessions, sessionID)
		m.streamMgr.mu.Unlock()
		return nil, fmt.Errorf("building client options: %w", err)
	}

	opts = append(opts,
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.DisableAutoCommit(),
	)

	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		m.streamMgr.mu.Lock()
		delete(m.streamMgr.sessions, sessionID)
		m.streamMgr.mu.Unlock()
		return nil, fmt.Errorf("creating stream consumer: %w", err)
	}

	streamCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.TimeoutMs)*time.Millisecond)

	session := &StreamSession{
		ID:        sessionID,
		Topic:     cfg.Topic,
		GroupID:   groupID,
		AgentID:   cfg.AgentID,
		Status:    "running",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
		BatchSize: cfg.BatchSize,
		cancel:    cancel,
		msgChan:   make(chan []Message, 10),
		errChan:   make(chan error, 1),
	}

	// Replace the placeholder with the fully initialized session.
	m.streamMgr.mu.Lock()
	m.streamMgr.sessions[session.ID] = session
	m.streamMgr.mu.Unlock()

	// Background consumer goroutine
	go func() {
		defer consumer.Close()
		defer cancel()
		defer func() {
			session.mu.Lock()
			session.Status = "stopped"
			session.mu.Unlock()
			close(session.msgChan)
			close(session.errChan)
		}()

		var totalDelivered int64
		ticker := time.NewTicker(time.Second / time.Duration(cfg.MaxMsgPerSec/cfg.BatchSize+1))
		defer ticker.Stop()

		pauseTimer := time.NewTimer(100 * time.Millisecond)
		pauseTimer.Stop()
		defer pauseTimer.Stop()

		backoffTimer := time.NewTimer(100 * time.Millisecond)
		backoffTimer.Stop()
		defer backoffTimer.Stop()

		for {
			select {
			case <-streamCtx.Done():
				return
			default:
			}

			if session.stopped.Load() {
				return
			}
			if session.paused.Load() {
				pauseTimer.Reset(100 * time.Millisecond)
				select {
				case <-streamCtx.Done():
					return
				case <-pauseTimer.C:
					continue
				}
			}

			select {
			case <-streamCtx.Done():
				return
			case <-ticker.C:
			}

			if totalDelivered >= int64(cfg.MaxMessages) {
				return
			}

			fetches := consumer.PollFetches(streamCtx)
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					if e.Err == context.DeadlineExceeded || e.Err == context.Canceled {
						return
					}
				}
			}

			var batch []Message
			fetches.EachRecord(func(r *kgo.Record) {
				if totalDelivered >= int64(cfg.MaxMessages) {
					return
				}
				headers := make(map[string]string)
				for _, h := range r.Headers {
					headers[h.Key] = string(h.Value)
				}
				batch = append(batch, Message{
					Partition: r.Partition,
					Offset:    r.Offset,
					Timestamp: r.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
					Key:       decodeValue(r.Key, cfg.Deserializer),
					Value:     decodeValue(r.Value, cfg.Deserializer),
					Headers:   headers,
				})
				if len(batch) >= cfg.BatchSize {
					select {
					case session.msgChan <- batch:
						atomic.AddInt64(&session.MsgCount, int64(len(batch)))
						totalDelivered += int64(len(batch))
					case <-streamCtx.Done():
						return
					}
					batch = nil
				}
			})

			// Flush remaining batch
			if len(batch) > 0 {
				select {
				case session.msgChan <- batch:
					atomic.AddInt64(&session.MsgCount, int64(len(batch)))
					totalDelivered += int64(len(batch))
				case <-streamCtx.Done():
					return
				}
			}

			if cfg.AutoCommit {
				if err := consumer.CommitUncommittedOffsets(streamCtx); err != nil {
					m.logger.Warn("stream commit failed", "session", session.ID, "error", err)
				}
			}

			if fetches.NumRecords() == 0 {
				backoffTimer.Reset(100 * time.Millisecond)
				select {
				case <-streamCtx.Done():
					return
				case <-backoffTimer.C:
				}
			}
		}
	}()

	return &StreamResult{
		SessionID: session.ID,
		Status:    "started",
		Topic:     cfg.Topic,
		GroupID:   groupID,
	}, nil
}

// PollStream retrieves available messages from an active stream session.
func (m *Manager) PollStream(sessionID string, timeoutMs int) (*StreamResult, error) {
	if m.streamMgr == nil {
		return nil, fmt.Errorf("no active stream sessions")
	}

	m.streamMgr.mu.RLock()
	session, ok := m.streamMgr.sessions[sessionID]
	m.streamMgr.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("stream session %q not found", sessionID)
	}

	if timeoutMs <= 0 {
		timeoutMs = 5000
	}

	var messages []Message
	timeout := time.After(time.Duration(timeoutMs) * time.Millisecond)

	// Collect messages until timeout or batch full
	for {
		select {
		case batch := <-session.msgChan:
			messages = append(messages, batch...)
			if len(messages) >= session.BatchSize {
				goto done
			}
		case <-timeout:
			goto done
		}
	}

done:
	return &StreamResult{
		SessionID: sessionID,
		Messages:  messages,
		Count:     len(messages),
		Status:    session.Status,
		Topic:     session.Topic,
		GroupID:   session.GroupID,
		TotalSent: atomic.LoadInt64(&session.MsgCount),
	}, nil
}

// ControlStream pauses, resumes, or stops a stream session.
func (m *Manager) ControlStream(sessionID, action string) (*StreamResult, error) {
	if m.streamMgr == nil {
		return nil, fmt.Errorf("no active stream sessions")
	}

	m.streamMgr.mu.RLock()
	session, ok := m.streamMgr.sessions[sessionID]
	m.streamMgr.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("stream session %q not found", sessionID)
	}

	switch action {
	case "pause":
		session.paused.Store(true)
		session.mu.Lock()
		session.Status = "paused"
		session.mu.Unlock()
	case "resume":
		session.paused.Store(false)
		session.mu.Lock()
		session.Status = "running"
		session.mu.Unlock()
	case "stop":
		session.stopped.Store(true)
		session.cancel()
		session.mu.Lock()
		session.Status = "stopped"
		session.mu.Unlock()
		// Remove from manager
		m.streamMgr.mu.Lock()
		delete(m.streamMgr.sessions, sessionID)
		m.streamMgr.mu.Unlock()
	default:
		return nil, fmt.Errorf("unknown action %q; must be: pause, resume, stop", action)
	}

	return &StreamResult{
		SessionID: sessionID,
		Status:    session.Status,
		Topic:     session.Topic,
		GroupID:   session.GroupID,
		TotalSent: atomic.LoadInt64(&session.MsgCount),
	}, nil
}

// ListStreams returns all active stream sessions.
func (m *Manager) ListStreams() []StreamSession {
	if m.streamMgr == nil {
		return nil
	}

	m.streamMgr.mu.RLock()
	defer m.streamMgr.mu.RUnlock()

	var sessions []StreamSession
	for _, s := range m.streamMgr.sessions {
		sessions = append(sessions, StreamSession{
			ID:        s.ID,
			Topic:     s.Topic,
			GroupID:   s.GroupID,
			AgentID:   s.AgentID,
			Status:    s.Status,
			StartedAt: s.StartedAt,
			MsgCount:  atomic.LoadInt64(&s.MsgCount),
			BatchSize: s.BatchSize,
		})
	}
	return sessions
}

// GetStreamSession returns the agent ID for a given stream session.
func (m *Manager) GetStreamSession(sessionID string) (*StreamSession, bool) {
	if m.streamMgr == nil {
		return nil, false
	}

	m.streamMgr.mu.RLock()
	defer m.streamMgr.mu.RUnlock()

	s, ok := m.streamMgr.sessions[sessionID]
	if !ok {
		return nil, false
	}
	return &StreamSession{
		ID:      s.ID,
		AgentID: s.AgentID,
		Topic:   s.Topic,
		GroupID:  s.GroupID,
		Status:  s.Status,
	}, true
}
