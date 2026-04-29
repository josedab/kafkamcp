package sessionreplay

import (
	"log/slog"
	"testing"
)

func TestStartAndEndSession(t *testing.T) {
	e := NewEngine(slog.Default())
	s := e.StartSession("agent-1", nil)
	if s.Status != "recording" {
		t.Errorf("expected recording, got %s", s.Status)
	}
	if err := e.EndSession(s.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := e.GetSession(s.ID)
	if got.Status != "completed" {
		t.Errorf("expected completed, got %s", got.Status)
	}
}

func TestRecordCall(t *testing.T) {
	e := NewEngine(slog.Default())
	s := e.StartSession("agent-1", nil)
	if err := e.RecordCall(s.ID, "kafka_list_topics", map[string]any{"filter": "*"}, map[string]any{"count": 5}, false, "", 120); err != nil {
		t.Fatal(err)
	}
	got, _ := e.GetSession(s.ID)
	if len(got.Calls) != 1 {
		t.Errorf("expected 1 call, got %d", len(got.Calls))
	}
	if got.Calls[0].Tool != "kafka_list_topics" {
		t.Errorf("expected kafka_list_topics, got %s", got.Calls[0].Tool)
	}
}

func TestRecordCall_CompletedSession(t *testing.T) {
	e := NewEngine(slog.Default())
	s := e.StartSession("agent-1", nil)
	_ = e.EndSession(s.ID)
	err := e.RecordCall(s.ID, "kafka_list_topics", nil, nil, false, "", 100)
	if err == nil {
		t.Error("expected error for recording on completed session")
	}
}

func TestReplay(t *testing.T) {
	e := NewEngine(slog.Default())
	s := e.StartSession("agent-1", nil)
	_ = e.RecordCall(s.ID, "kafka_list_topics", nil, nil, false, "", 100)
	_ = e.RecordCall(s.ID, "kafka_consume", nil, nil, true, "timeout", 6000)
	_ = e.EndSession(s.ID)

	result, err := e.Replay(s.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalCalls != 2 {
		t.Errorf("expected 2 calls, got %d", result.TotalCalls)
	}
	if result.ErrorCalls != 1 {
		t.Errorf("expected 1 error call, got %d", result.ErrorCalls)
	}
	if len(result.Bottlenecks) != 1 {
		t.Errorf("expected 1 bottleneck, got %d", len(result.Bottlenecks))
	}
}

func TestListSessions(t *testing.T) {
	e := NewEngine(slog.Default())
	e.StartSession("agent-1", nil)
	e.StartSession("agent-2", nil)
	e.StartSession("agent-1", nil)

	all := e.ListSessions("", 50)
	if len(all) != 3 {
		t.Errorf("expected 3 sessions, got %d", len(all))
	}

	filtered := e.ListSessions("agent-1", 50)
	if len(filtered) != 2 {
		t.Errorf("expected 2 sessions for agent-1, got %d", len(filtered))
	}
}
