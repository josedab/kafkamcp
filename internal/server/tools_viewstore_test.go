package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/viewstore"
)

func newTestServerWithViewStore(t *testing.T, mock *kafka.MockKafkaManager) *Server {
	t.Helper()
	s := newTestServerWithMock(t, mock)
	s.viewStoreEngine = viewstore.NewEngine(s.logger)
	return s
}

// ===================== handleViewCreate =====================

func TestHandleViewCreate_MissingName(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	result, _ := s.handleViewCreate(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"source_topic": "orders"}, time.Now())
	assertIsError(t, result)
}

func TestHandleViewCreate_MissingSourceTopic(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	result, _ := s.handleViewCreate(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"name": "my-view"}, time.Now())
	assertIsError(t, result)
}

func TestHandleViewCreate_ValidParams(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	result, err := s.handleViewCreate(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"name": "test-view", "source_topic": "orders"}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	if !strings.Contains(text, "test-view") {
		t.Errorf("expected view name in result, got: %s", text)
	}
}

func TestHandleViewCreate_WithAggregation(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	result, err := s.handleViewCreate(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{
			"name":             "agg-view",
			"source_topic":     "metrics",
			"aggregation_type": "sum",
			"key_field":        "device_id",
		}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleViewQuery =====================

func TestHandleViewQuery_MissingViewID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	result, _ := s.handleViewQuery(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{}, time.Now())
	assertIsError(t, result)
}

func TestHandleViewQuery_NonexistentView(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	result, _ := s.handleViewQuery(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"view_id": "nonexistent"}, time.Now())
	assertIsError(t, result)
}

func TestHandleViewQuery_ValidQuery(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})

	// Create a view first
	view, _ := s.viewStoreEngine.CreateView("query-test", "orders", "_key", "", "", 0, 0, nil, nil, "", "", "agent-1")
	s.viewStoreEngine.IngestRecord(view.ID, "key1", map[string]any{"amount": 100.0}, 1)

	result, err := s.handleViewQuery(context.Background(), makeToolRequest(map[string]any{
		"key_exact": "key1",
	}), "agent-1", map[string]any{"view_id": view.ID}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	var qr map[string]any
	json.Unmarshal([]byte(text), &qr)
	if qr["total_count"].(float64) != 1 {
		t.Errorf("expected 1 result, got %v", qr["total_count"])
	}
}

// ===================== handleViewStatus =====================

func TestHandleViewStatus_InvalidAction(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	result, _ := s.handleViewStatus(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "invalid"}, time.Now())
	assertIsError(t, result)
}

func TestHandleViewStatus_ListAction(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	s.viewStoreEngine.CreateView("list-test", "orders", "_key", "", "", 0, 0, nil, nil, "", "", "agent-1")

	result, err := s.handleViewStatus(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "list"}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleViewStatus_GetMissingID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	result, _ := s.handleViewStatus(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "get"}, time.Now())
	assertIsError(t, result)
}

func TestHandleViewStatus_GetUnknownID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	result, _ := s.handleViewStatus(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "get", "view_id": "nonexistent"}, time.Now())
	assertIsError(t, result)
}

func TestHandleViewStatus_StatsValidID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	view, _ := s.viewStoreEngine.CreateView("stats-test", "orders", "_key", "", "", 0, 0, nil, nil, "", "", "agent-1")

	result, err := s.handleViewStatus(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "stats", "view_id": view.ID}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	var stats map[string]any
	json.Unmarshal([]byte(text), &stats)
	if stats["view_name"] != "stats-test" {
		t.Errorf("expected view_name 'stats-test', got %v", stats["view_name"])
	}
}

func TestHandleViewStatus_StatsMissingID(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	result, _ := s.handleViewStatus(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "stats"}, time.Now())
	assertIsError(t, result)
}

func TestHandleViewStatus_DeleteAction(t *testing.T) {
	t.Parallel()
	s := newTestServerWithViewStore(t, &kafka.MockKafkaManager{})
	view, _ := s.viewStoreEngine.CreateView("delete-test", "orders", "_key", "", "", 0, 0, nil, nil, "", "", "agent-1")

	result, err := s.handleViewStatus(context.Background(), makeToolRequest(nil), "agent-1",
		map[string]any{"action": "delete", "view_id": view.ID}, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	// Verify deleted
	_, getErr := s.viewStoreEngine.GetView(view.ID)
	if getErr == nil {
		t.Error("view should be deleted")
	}
}
