package kafka

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// ===================== PollStream =====================

func TestPollStream_ValidSession(t *testing.T) {
	mock := &MockKafkaManager{
		PollStreamFn: func(sessionID string, timeoutMs int) (*StreamResult, error) {
			return &StreamResult{
				SessionID: sessionID,
				Messages: []Message{
					{Partition: 0, Offset: 1, Key: "k1", Value: "v1", Timestamp: "2024-01-01T00:00:00.000Z"},
					{Partition: 0, Offset: 2, Key: "k2", Value: "v2", Timestamp: "2024-01-01T00:00:01.000Z"},
				},
				Count:     2,
				Status:    "running",
				Topic:     "orders",
				GroupID:   "test-group",
				TotalSent: 100,
			}, nil
		},
	}

	result, err := mock.PollStream("stream-abc123", 5000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SessionID != "stream-abc123" {
		t.Errorf("expected session ID 'stream-abc123', got %q", result.SessionID)
	}
	if result.Count != 2 {
		t.Errorf("expected 2 messages, got %d", result.Count)
	}
	if len(result.Messages) != 2 {
		t.Errorf("expected 2 messages in slice, got %d", len(result.Messages))
	}
	if result.Status != "running" {
		t.Errorf("expected status 'running', got %q", result.Status)
	}
	if result.TotalSent != 100 {
		t.Errorf("expected total sent 100, got %d", result.TotalSent)
	}
}

func TestPollStream_InvalidSession(t *testing.T) {
	mock := &MockKafkaManager{
		PollStreamFn: func(sessionID string, timeoutMs int) (*StreamResult, error) {
			return nil, fmt.Errorf("stream session %q not found", sessionID)
		},
	}

	_, err := mock.PollStream("nonexistent-session", 5000)
	if err == nil {
		t.Fatal("expected error for invalid session")
	}
	if !contains(err.Error(), "not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestPollStream_TimeoutNoMessages(t *testing.T) {
	mock := &MockKafkaManager{
		PollStreamFn: func(sessionID string, timeoutMs int) (*StreamResult, error) {
			return &StreamResult{
				SessionID: sessionID,
				Messages:  nil,
				Count:     0,
				Status:    "running",
				TotalSent: 50,
			}, nil
		},
	}

	result, err := mock.PollStream("stream-abc", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Count != 0 {
		t.Errorf("expected 0 messages on timeout, got %d", result.Count)
	}
}

func TestPollStream_CallRecording(t *testing.T) {
	mock := &MockKafkaManager{
		PollStreamFn: func(sessionID string, timeoutMs int) (*StreamResult, error) {
			return &StreamResult{SessionID: sessionID}, nil
		},
	}

	mock.PollStream("stream-123", 3000)
	if mock.CallCount("PollStream") != 1 {
		t.Error("expected PollStream called once")
	}
	call := mock.LastCall("PollStream")
	if call.Args[0] != "stream-123" {
		t.Errorf("expected sessionID 'stream-123', got %v", call.Args[0])
	}
	if call.Args[1] != 3000 {
		t.Errorf("expected timeoutMs 3000, got %v", call.Args[1])
	}
}

// ===================== ControlStream =====================

func TestControlStream_Pause(t *testing.T) {
	m := testManager()
	session := &StreamSession{
		ID:     "test-session",
		Topic:  "orders",
		Status: "running",
	}
	session.cancel = func() {}
	m.streamMgr.sessions["test-session"] = session

	result, err := m.ControlStream("test-session", "pause")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "paused" {
		t.Errorf("expected status 'paused', got %q", result.Status)
	}
	if !session.paused.Load() {
		t.Error("expected paused=true")
	}
}

func TestControlStream_Resume(t *testing.T) {
	m := testManager()
	session := &StreamSession{
		ID:     "test-session",
		Topic:  "orders",
		Status: "paused",
	}
	session.paused.Store(true)
	session.cancel = func() {}
	m.streamMgr.sessions["test-session"] = session

	result, err := m.ControlStream("test-session", "resume")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "running" {
		t.Errorf("expected status 'running', got %q", result.Status)
	}
	if session.paused.Load() {
		t.Error("expected paused=false after resume")
	}
}

func TestControlStream_Stop(t *testing.T) {
	m := testManager()
	var cancelCalled atomic.Bool
	session := &StreamSession{
		ID:     "test-session",
		Topic:  "orders",
		Status: "running",
	}
	session.cancel = func() { cancelCalled.Store(true) }
	m.streamMgr.sessions["test-session"] = session

	result, err := m.ControlStream("test-session", "stop")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != "stopped" {
		t.Errorf("expected status 'stopped', got %q", result.Status)
	}
	if !cancelCalled.Load() {
		t.Error("expected cancel to be called on stop")
	}
	// Session should be removed from manager
	m.streamMgr.mu.RLock()
	_, exists := m.streamMgr.sessions["test-session"]
	m.streamMgr.mu.RUnlock()
	if exists {
		t.Error("expected session to be removed after stop")
	}
}

func TestControlStream_InvalidAction(t *testing.T) {
	m := testManager()
	m.streamMgr.sessions["test-session"] = &StreamSession{
		ID:     "test-session",
		Status: "running",
	}

	_, err := m.ControlStream("test-session", "invalid-action")
	if err == nil {
		t.Fatal("expected error for invalid action")
	}
	if !contains(err.Error(), "unknown action") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestControlStream_InvalidSession(t *testing.T) {
	m := testManager()

	_, err := m.ControlStream("nonexistent", "pause")
	if err == nil {
		t.Fatal("expected error for invalid session")
	}
	if !contains(err.Error(), "not found") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ===================== ListStreams =====================

func TestListStreams_EmptySessions(t *testing.T) {
	m := testManager()
	sessions := m.ListStreams()
	if sessions != nil {
		t.Errorf("expected nil for empty sessions, got %v", sessions)
	}
}

func TestListStreams_MultipleActiveSessions(t *testing.T) {
	m := testManager()
	now := time.Now().UTC().Format(time.RFC3339)

	m.streamMgr.sessions["session-1"] = &StreamSession{
		ID:        "session-1",
		Topic:     "orders",
		GroupID:   "group-1",
		Status:    "running",
		StartedAt: now,
		BatchSize: 10,
	}
	m.streamMgr.sessions["session-2"] = &StreamSession{
		ID:        "session-2",
		Topic:     "events",
		GroupID:   "group-2",
		Status:    "paused",
		StartedAt: now,
		BatchSize: 20,
	}

	sessions := m.ListStreams()
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}

	found := map[string]bool{}
	for i := range sessions {
		s := &sessions[i]
		found[s.ID] = true
		if s.ID == "session-1" {
			if s.Topic != "orders" || s.GroupID != "group-1" || s.Status != "running" {
				t.Errorf("session-1 metadata mismatch: %+v", s)
			}
		}
		if s.ID == "session-2" {
			if s.Topic != "events" || s.GroupID != "group-2" || s.Status != "paused" {
				t.Errorf("session-2 metadata mismatch: %+v", s)
			}
		}
	}
	if !found["session-1"] || !found["session-2"] {
		t.Error("expected both sessions in list")
	}
}

func TestListStreams_MessageCountAtomic(t *testing.T) {
	m := testManager()
	session := &StreamSession{
		ID:        "session-1",
		Topic:     "orders",
		Status:    "running",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
		BatchSize: 10,
	}
	atomic.StoreInt64(&session.MsgCount, 42)
	m.streamMgr.sessions["session-1"] = session

	sessions := m.ListStreams()
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
	if sessions[0].MsgCount != 42 {
		t.Errorf("expected msg count 42, got %d", sessions[0].MsgCount)
	}
}

// ===================== PollStream on Manager directly =====================

func TestPollStream_WithChannelMessages(t *testing.T) {
	m := testManager()
	session := &StreamSession{
		ID:        "test-session",
		Topic:     "orders",
		GroupID:   "group-1",
		Status:    "running",
		BatchSize: 10,
		msgChan:   make(chan []Message, 10),
	}
	m.streamMgr.sessions["test-session"] = session

	// Pre-load messages into channel
	session.msgChan <- []Message{
		{Partition: 0, Offset: 1, Key: "k1", Value: "v1"},
		{Partition: 0, Offset: 2, Key: "k2", Value: "v2"},
	}

	result, err := m.PollStream("test-session", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Count != 2 {
		t.Errorf("expected 2 messages, got %d", result.Count)
	}
	if result.SessionID != "test-session" {
		t.Errorf("expected session ID 'test-session', got %q", result.SessionID)
	}
}

func TestPollStream_TimeoutOnEmptyChannel(t *testing.T) {
	m := testManager()
	session := &StreamSession{
		ID:        "test-session",
		Topic:     "orders",
		GroupID:   "group-1",
		Status:    "running",
		BatchSize: 10,
		msgChan:   make(chan []Message, 10),
	}
	m.streamMgr.sessions["test-session"] = session

	start := time.Now()
	result, err := m.PollStream("test-session", 200)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Count != 0 {
		t.Errorf("expected 0 messages on timeout, got %d", result.Count)
	}
	if elapsed < 150*time.Millisecond {
		t.Error("expected timeout to wait at least ~200ms")
	}
}
