package server

import (
	"context"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/schema"
)

// ===================== handleSchemaDiff =====================

func TestHandleSchemaDiff_MissingSubject_Mock(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleSchemaDiff(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSchemaDiff_NoSchemaRegistry(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleSchemaDiff(context.Background(), makeToolRequest(map[string]any{
		"subject":      "orders-value",
		"version_from": "1",
		"version_to":   "latest",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "Schema Registry") {
		t.Error("expected Schema Registry error")
	}
}

func TestHandleSchemaDiff_MissingVersionFrom(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	sc, _ := schema.NewClient("http://localhost:8081", "", "", 100, 60)
	s.schemaClients["default"] = sc
	result, err := s.handleSchemaDiff(context.Background(), makeToolRequest(map[string]any{
		"subject": "orders-value",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSchemaDiff_AuthDenied(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, withDefaultDeny)
	result, err := s.handleSchemaDiff(context.Background(), makeToolRequest(map[string]any{
		"subject":      "orders-value",
		"version_from": "1",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleWatchUnsubscribe =====================

func TestHandleWatchUnsubscribe_MissingID(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWatch(context.Background(), makeToolRequest(map[string]any{
		"action": "unsubscribe",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWatchUnsubscribe_NilManager(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	s.watchManager = nil
	result, err := s.handleWatch(context.Background(), makeToolRequest(map[string]any{
		"action":          "unsubscribe",
		"subscription_id": "sub-123",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWatchUnsubscribe_NotFound(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWatch(context.Background(), makeToolRequest(map[string]any{
		"action":          "unsubscribe",
		"subscription_id": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleWatchList =====================

func TestHandleWatchList_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWatch(context.Background(), makeToolRequest(map[string]any{
		"action": "list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "subscriptions") {
		t.Error("expected subscriptions in result")
	}
}

func TestHandleWatchList_NilManager(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	s.watchManager = nil
	result, err := s.handleWatch(context.Background(), makeToolRequest(map[string]any{
		"action": "list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleWatchSubscribe =====================

func TestHandleWatchSubscribe_MissingPattern(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWatch(context.Background(), makeToolRequest(map[string]any{
		"action": "subscribe",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleWatchSubscribe_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWatch(context.Background(), makeToolRequest(map[string]any{
		"action":        "subscribe",
		"topic_pattern": "orders.*",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "subscribed") {
		t.Error("expected subscribed status")
	}
}

func TestHandleWatchSubscribe_NilManager(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	s.watchManager = nil
	result, err := s.handleWatch(context.Background(), makeToolRequest(map[string]any{
		"action":        "subscribe",
		"topic_pattern": "orders.*",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleWatch unknown action =====================

func TestHandleWatch_UnknownAction_Mock(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleWatch(context.Background(), makeToolRequest(map[string]any{
		"action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleRemediateDLQ =====================

func TestHandleRemediateDLQ_MissingTopic_Mock(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleRemediateDLQ(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
