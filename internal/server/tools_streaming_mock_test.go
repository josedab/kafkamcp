package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleStreamList =====================

func TestHandleStreamList_Empty(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListStreamsFn: func() []kafka.StreamSession {
			return nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{"action": "list"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "total_count") {
		t.Error("expected total_count in result")
	}
}

func TestHandleStreamList_MultipleSessions(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListStreamsFn: func() []kafka.StreamSession {
			return []kafka.StreamSession{
				{ID: "s1", Topic: "orders", Status: "running"},
				{ID: "s2", Topic: "events", Status: "paused"},
			}
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{"action": "list"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "s1") || !strings.Contains(text, "s2") {
		t.Error("expected both session IDs in result")
	}
}

// ===================== handleStreamPoll =====================

func TestHandleStreamPoll_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PollStreamFn: func(sessionID string, timeoutMs int) (*kafka.StreamResult, error) {
			return &kafka.StreamResult{
				SessionID: sessionID,
				Messages: []kafka.Message{
					{Partition: 0, Offset: 1, Key: "k1", Value: "v1"},
				},
				Count:  1,
				Status: "running",
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{
		"action": "poll", "session_id": "session-123",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleStreamPoll_InvalidSession(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		PollStreamFn: func(sessionID string, timeoutMs int) (*kafka.StreamResult, error) {
			return nil, fmt.Errorf("session %q not found", sessionID)
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{
		"action": "poll", "session_id": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleStreamPoll_MissingSessionID(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{
		"action": "poll",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleStreamControl =====================

func TestHandleStreamControl_Pause(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ControlStreamFn: func(sessionID, action string) (*kafka.StreamResult, error) {
			return &kafka.StreamResult{SessionID: sessionID, Status: "paused"}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{
		"action": "pause", "session_id": "s1",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleStreamControl_Resume(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ControlStreamFn: func(sessionID, action string) (*kafka.StreamResult, error) {
			return &kafka.StreamResult{SessionID: sessionID, Status: "running"}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{
		"action": "resume", "session_id": "s1",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleStreamControl_Stop(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ControlStreamFn: func(sessionID, action string) (*kafka.StreamResult, error) {
			return &kafka.StreamResult{SessionID: sessionID, Status: "stopped"}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{
		"action": "stop", "session_id": "s1",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleStreamControl_InvalidSession(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ControlStreamFn: func(sessionID, action string) (*kafka.StreamResult, error) {
			return nil, fmt.Errorf("session not found")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{
		"action": "stop", "session_id": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleStreamControl_MissingSessionID(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{
		"action": "pause",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleStream_UnknownAction_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleStream(context.Background(), makeToolRequest(map[string]any{
		"action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
