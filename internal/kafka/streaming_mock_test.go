package kafka

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// --------------- StreamManager ---------------

func TestNewStreamManager_EmptySessions(t *testing.T) {
	sm := NewStreamManager()
	if sm == nil {
		t.Fatal("expected non-nil StreamManager")
	}
	if len(sm.sessions) != 0 {
		t.Errorf("expected empty sessions map, got %d entries", len(sm.sessions))
	}
}

// --------------- PollStream ---------------

func TestPollStream_NilStreamMgr(t *testing.T) {
	m := &Manager{
		clients:   make(map[string]*kgo.Client),
		admins:    make(map[string]*kadm.Client),
		logger:    slog.Default(),
		streamMgr: nil,
	}
	_, err := m.PollStream("some-session", 1000)
	if err == nil {
		t.Fatal("expected error when streamMgr is nil")
	}
	if !strings.Contains(err.Error(), "no active stream sessions") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestPollStream_NonexistentSession(t *testing.T) {
	m := testManager()
	_, err := m.PollStream("nonexistent-session", 1000)
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// --------------- ControlStream ---------------

func TestControlStream_NilStreamMgr(t *testing.T) {
	m := &Manager{
		clients:   make(map[string]*kgo.Client),
		admins:    make(map[string]*kadm.Client),
		logger:    slog.Default(),
		streamMgr: nil,
	}
	_, err := m.ControlStream("some-session", "pause")
	if err == nil {
		t.Fatal("expected error when streamMgr is nil")
	}
	if !strings.Contains(err.Error(), "no active stream sessions") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestControlStream_NonexistentSession(t *testing.T) {
	m := testManager()
	_, err := m.ControlStream("nonexistent-session", "pause")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestControlStream_UnknownAction(t *testing.T) {
	m := testManager()
	// Add a fake session so we get past the lookup
	m.streamMgr.sessions["test-session"] = &StreamSession{
		ID:     "test-session",
		Status: "running",
	}
	_, err := m.ControlStream("test-session", "invalid-action")
	if err == nil {
		t.Fatal("expected error for unknown action")
	}
	if !strings.Contains(err.Error(), "unknown action") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// --------------- ListStreams ---------------

func TestListStreams_NilStreamMgr(t *testing.T) {
	m := &Manager{
		clients:   make(map[string]*kgo.Client),
		admins:    make(map[string]*kadm.Client),
		logger:    slog.Default(),
		streamMgr: nil,
	}
	result := m.ListStreams()
	if result != nil {
		t.Errorf("expected nil for nil streamMgr, got %v", result)
	}
}

func TestListStreams_EmptyStreamMgr(t *testing.T) {
	m := testManager()
	result := m.ListStreams()
	if result != nil {
		t.Errorf("expected nil for empty sessions, got %v", result)
	}
}

// --------------- generateSessionID ---------------

func TestGenerateSessionID_Prefix(t *testing.T) {
	id := generateSessionID()
	if !strings.HasPrefix(id, "stream-") {
		t.Errorf("expected 'stream-' prefix, got %q", id)
	}
}

func TestGenerateSessionID_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := generateSessionID()
		if seen[id] {
			t.Fatalf("duplicate session ID generated: %q", id)
		}
		seen[id] = true
	}
}
