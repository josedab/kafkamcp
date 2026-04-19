package server

import (
	"context"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleSuggestSchemaEvolution =====================

func TestHandleSuggestSchemaEvolution_MissingSubject(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleSuggestSchemaEvolution(context.Background(), makeToolRequest(map[string]any{
		"change": "add field age",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "subject") {
		t.Error("expected error mentioning 'subject'")
	}
}

func TestHandleSuggestSchemaEvolution_MissingChange(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleSuggestSchemaEvolution(context.Background(), makeToolRequest(map[string]any{
		"subject": "orders-value",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "change") {
		t.Error("expected error mentioning 'change'")
	}
}

func TestHandleSuggestSchemaEvolution_NoSchemaClient(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	// No schema clients configured, so sc will be nil

	result, err := s.handleSuggestSchemaEvolution(context.Background(), makeToolRequest(map[string]any{
		"subject": "orders-value",
		"change":  "add field status",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "suggestions") {
		t.Errorf("expected 'suggestions' in result, got: %s", text)
	}
}

func TestHandleSuggestSchemaEvolution_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleSuggestSchemaEvolution(context.Background(), makeToolRequest(map[string]any{
		"subject": "orders-value",
		"change":  "add field phone_number",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "count") {
		t.Errorf("expected 'count' in result, got: %s", text)
	}
	if !strings.Contains(text, "orders-value") {
		t.Errorf("expected subject in result, got: %s", text)
	}
}

func TestHandleSuggestSchemaEvolution_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock

	result, err := s.handleSuggestSchemaEvolution(context.Background(), makeToolRequest(map[string]any{
		"subject": "orders-value",
		"change":  "add field",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleSuggestSchemaEvolution_LengthValidation(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	// Very long subject
	longSubject := strings.Repeat("a", 10000)
	result, err := s.handleSuggestSchemaEvolution(context.Background(), makeToolRequest(map[string]any{
		"subject": longSubject,
		"change":  "add field",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
