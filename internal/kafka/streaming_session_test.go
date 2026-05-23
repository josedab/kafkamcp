package kafka

import (
	"strings"
	"sync"
	"testing"
)

func TestGetStreamSession_NilStreamMgr(t *testing.T) {
	t.Parallel()
	m := &Manager{}
	_, ok := m.GetStreamSession("some-id")
	if ok {
		t.Error("expected false for nil streamMgr")
	}
}

func TestGetStreamSession_UnknownID(t *testing.T) {
	t.Parallel()
	m := &Manager{streamMgr: newStreamSessionManager()}
	_, ok := m.GetStreamSession("nonexistent")
	if ok {
		t.Error("expected false for unknown session ID")
	}
}

func TestGetStreamSession_KnownID(t *testing.T) {
	t.Parallel()
	m := &Manager{streamMgr: newStreamSessionManager()}
	m.streamMgr.sessions["stream-abc"] = &StreamSession{
		ID:      "stream-abc",
		Topic:   "orders",
		GroupID: "group-1",
		AgentID: "agent-x",
		Status:  "running",
	}

	sess, ok := m.GetStreamSession("stream-abc")
	if !ok {
		t.Fatal("expected true for known session")
	}
	if sess.ID != "stream-abc" {
		t.Errorf("expected stream-abc, got %s", sess.ID)
	}
	if sess.Topic != "orders" {
		t.Errorf("expected orders, got %s", sess.Topic)
	}
	if sess.AgentID != "agent-x" {
		t.Errorf("expected agent-x, got %s", sess.AgentID)
	}
}

func TestListStreams_NilManager(t *testing.T) {
	t.Parallel()
	m := &Manager{}
	sessions := m.ListStreams()
	if sessions != nil {
		t.Errorf("expected nil for nil streamMgr, got %v", sessions)
	}
}

func TestListStreams_ReturnsSessions(t *testing.T) {
	t.Parallel()
	m := &Manager{streamMgr: newStreamSessionManager()}
	m.streamMgr.sessions["s1"] = &StreamSession{
		ID: "s1", Topic: "t1", GroupID: "g1", Status: "running", BatchSize: 10,
	}
	m.streamMgr.sessions["s2"] = &StreamSession{
		ID: "s2", Topic: "t2", GroupID: "g2", Status: "paused", BatchSize: 20,
	}

	sessions := m.ListStreams()
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}

	found := make(map[string]bool)
	for _, s := range sessions {
		found[s.ID] = true
		if s.ID == "s1" {
			if s.Topic != "t1" || s.GroupID != "g1" || s.Status != "running" {
				t.Errorf("session s1 fields incorrect: %+v", s)
			}
		}
	}
	if !found["s1"] || !found["s2"] {
		t.Error("expected both sessions to be returned")
	}
}

func TestGetStreamSession_FieldsCopied(t *testing.T) {
	t.Parallel()
	m := &Manager{streamMgr: newStreamSessionManager()}
	m.streamMgr.sessions["s1"] = &StreamSession{
		ID:      "s1",
		Topic:   "my-topic",
		GroupID: "my-group",
		AgentID: "my-agent",
		Status:  "running",
	}

	sess, ok := m.GetStreamSession("s1")
	if !ok {
		t.Fatal("expected session found")
	}
	if sess.Topic != "my-topic" {
		t.Errorf("expected my-topic, got %s", sess.Topic)
	}
	if sess.GroupID != "my-group" {
		t.Errorf("expected my-group, got %s", sess.GroupID)
	}
	if sess.Status != "running" {
		t.Errorf("expected running, got %s", sess.Status)
	}
}

func TestConcurrentGetStreamSession(t *testing.T) {
	t.Parallel()
	m := &Manager{streamMgr: newStreamSessionManager()}
	m.streamMgr.sessions["s1"] = &StreamSession{
		ID: "s1", Topic: "t1", GroupID: "g1", Status: "running",
	}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok := m.GetStreamSession("s1")
			if !ok {
				t.Error("expected session to be found")
			}
		}()
	}
	wg.Wait()
}

func TestGenerateSessionID_HasPrefix(t *testing.T) {
	t.Parallel()
	id, err := generateSessionID()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(id, "stream-") {
		t.Errorf("expected stream- prefix, got %q", id)
	}
}
