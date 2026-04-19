//go:build integration

package kafka

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// ===================== StartStream config defaults =====================

func TestStartStream_NonexistentCluster(t *testing.T) {
	m := testManager()
	_, err := m.StartStream(context.Background(), "nonexistent", StreamConfig{
		Topic: "test-topic",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestStartStream_MockDefaultBatchSize(t *testing.T) {
	var captured StreamConfig
	mock := &MockKafkaManager{
		StartStreamFn: func(ctx context.Context, cluster string, cfg StreamConfig) (*StreamResult, error) {
			captured = cfg
			return &StreamResult{SessionID: "s1", Status: "started", Topic: cfg.Topic}, nil
		},
	}

	result, err := mock.StartStream(context.Background(), "default", StreamConfig{
		Topic: "events",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.SessionID != "s1" {
		t.Errorf("expected session ID 's1', got %q", result.SessionID)
	}
	if captured.Topic != "events" {
		t.Errorf("expected topic 'events', got %q", captured.Topic)
	}
}

func TestStartStream_MockRateLimitDefault(t *testing.T) {
	var captured StreamConfig
	mock := &MockKafkaManager{
		StartStreamFn: func(ctx context.Context, cluster string, cfg StreamConfig) (*StreamResult, error) {
			captured = cfg
			return &StreamResult{SessionID: "s1", Status: "started"}, nil
		},
	}

	_, err := mock.StartStream(context.Background(), "default", StreamConfig{
		Topic:        "events",
		MaxMsgPerSec: 500,
		BatchSize:    20,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured.MaxMsgPerSec != 500 {
		t.Errorf("expected MaxMsgPerSec=500, got %d", captured.MaxMsgPerSec)
	}
	if captured.BatchSize != 20 {
		t.Errorf("expected BatchSize=20, got %d", captured.BatchSize)
	}
}

func TestStartStream_MockError(t *testing.T) {
	mock := &MockKafkaManager{
		StartStreamFn: func(ctx context.Context, cluster string, cfg StreamConfig) (*StreamResult, error) {
			return nil, context.DeadlineExceeded
		},
	}

	_, err := mock.StartStream(context.Background(), "default", StreamConfig{Topic: "t"})
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got: %v", err)
	}
}

// ===================== ControlStream pause/resume/stop =====================

func TestControlStream_PauseResumeStop(t *testing.T) {
	m := testManager()
	// Create a session directly for testing
	session := &StreamSession{
		ID:     "test-session",
		Topic:  "orders",
		Status: "running",
		cancel: func() {},
	}
	m.streamMgr.mu.Lock()
	m.streamMgr.sessions["test-session"] = session
	m.streamMgr.mu.Unlock()

	// Pause
	result, err := m.ControlStream("test-session", "pause")
	if err != nil {
		t.Fatalf("pause error: %v", err)
	}
	if result.Status != "paused" {
		t.Errorf("expected status 'paused', got %q", result.Status)
	}
	if !session.paused.Load() {
		t.Error("expected paused flag to be true")
	}

	// Resume
	result, err = m.ControlStream("test-session", "resume")
	if err != nil {
		t.Fatalf("resume error: %v", err)
	}
	if result.Status != "running" {
		t.Errorf("expected status 'running', got %q", result.Status)
	}
	if session.paused.Load() {
		t.Error("expected paused flag to be false")
	}

	// Stop
	result, err = m.ControlStream("test-session", "stop")
	if err != nil {
		t.Fatalf("stop error: %v", err)
	}
	if result.Status != "stopped" {
		t.Errorf("expected status 'stopped', got %q", result.Status)
	}

	// Session should be removed
	m.streamMgr.mu.RLock()
	_, exists := m.streamMgr.sessions["test-session"]
	m.streamMgr.mu.RUnlock()
	if exists {
		t.Error("expected session to be removed after stop")
	}
}

// ===================== PollStream timeout =====================

func TestPollStream_DefaultTimeout(t *testing.T) {
	m := testManager()
	session := &StreamSession{
		ID:      "poll-test",
		Topic:   "t",
		Status:  "running",
		BatchSize: 10,
		msgChan: make(chan []Message, 10),
		cancel:  func() {},
	}
	m.streamMgr.mu.Lock()
	m.streamMgr.sessions["poll-test"] = session
	m.streamMgr.mu.Unlock()

	// Poll with very short timeout to verify it doesn't block forever
	start := time.Now()
	result, err := m.PollStream("poll-test", 100) // 100ms timeout
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Count != 0 {
		t.Errorf("expected 0 messages, got %d", result.Count)
	}
	if elapsed > 2*time.Second {
		t.Error("PollStream blocked too long")
	}
}

func TestPollStream_WithMessages(t *testing.T) {
	m := testManager()
	session := &StreamSession{
		ID:        "poll-msg-test",
		Topic:     "orders",
		Status:    "running",
		BatchSize: 5,
		msgChan:   make(chan []Message, 10),
		cancel:    func() {},
	}
	m.streamMgr.mu.Lock()
	m.streamMgr.sessions["poll-msg-test"] = session
	m.streamMgr.mu.Unlock()

	// Send a batch of messages
	session.msgChan <- []Message{
		{Partition: 0, Offset: 1, Key: "k1", Value: "v1"},
		{Partition: 0, Offset: 2, Key: "k2", Value: "v2"},
	}

	result, err := m.PollStream("poll-msg-test", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Count != 2 {
		t.Errorf("expected 2 messages, got %d", result.Count)
	}
}

// ===================== ListStreams with active sessions =====================

func TestListStreams_WithSessions(t *testing.T) {
	m := testManager()
	m.streamMgr.sessions["s1"] = &StreamSession{
		ID: "s1", Topic: "orders", Status: "running", BatchSize: 10,
	}
	m.streamMgr.sessions["s2"] = &StreamSession{
		ID: "s2", Topic: "events", Status: "paused", BatchSize: 5,
	}

	sessions := m.ListStreams()
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}

	found := map[string]bool{}
	for _, s := range sessions {
		found[s.ID] = true
	}
	if !found["s1"] || !found["s2"] {
		t.Error("expected both sessions to be listed")
	}
}

// ===================== Concurrent stream operations =====================

func TestStreamSession_AtomicMsgCount(t *testing.T) {
	session := &StreamSession{}
	atomic.AddInt64(&session.MsgCount, 10)
	atomic.AddInt64(&session.MsgCount, 5)
	if atomic.LoadInt64(&session.MsgCount) != 15 {
		t.Errorf("expected MsgCount=15, got %d", session.MsgCount)
	}
}

func TestControlStream_StopSetsFlags(t *testing.T) {
	m := testManager()
	session := &StreamSession{
		ID:     "flag-test",
		Status: "running",
		cancel: func() {},
	}
	m.streamMgr.sessions["flag-test"] = session

	m.ControlStream("flag-test", "stop")

	if !session.stopped.Load() {
		t.Error("expected stopped flag to be true")
	}
}

func TestPollStream_NegativeTimeout(t *testing.T) {
	m := testManager()
	session := &StreamSession{
		ID:        "neg-timeout",
		Topic:     "t",
		Status:    "running",
		BatchSize: 10,
		msgChan:   make(chan []Message, 10),
		cancel:    func() {},
	}
	m.streamMgr.mu.Lock()
	m.streamMgr.sessions["neg-timeout"] = session
	m.streamMgr.mu.Unlock()

	// Negative timeout should default to 5000ms; poll should still work
	result, err := m.PollStream("neg-timeout", -1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result.SessionID, "neg-timeout") {
		t.Error("expected session ID in result")
	}
}
