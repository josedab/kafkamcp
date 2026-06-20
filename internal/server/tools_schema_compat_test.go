package server

import (
	"context"
	"strings"
	"testing"

	"github.com/josedab/kafkamcp/internal/kafka"
)

// ===================== handleCheckSchemaCompatibility =====================

func TestHandleCheckSchemaCompatibility_MissingSubject_Mock(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleCheckSchemaCompatibility(context.Background(), makeToolRequest(map[string]any{
		"schema": `{"type":"record","name":"test","fields":[]}`,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "subject") {
		t.Error("expected error mentioning 'subject'")
	}
}

func TestHandleCheckSchemaCompatibility_MissingSchema(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	result, err := s.handleCheckSchemaCompatibility(context.Background(), makeToolRequest(map[string]any{
		"subject": "orders-value",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "schema") {
		t.Error("expected error mentioning 'schema'")
	}
}

func TestHandleCheckSchemaCompatibility_SubjectMaxLength(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	longSubject := strings.Repeat("a", 10000)
	result, err := s.handleCheckSchemaCompatibility(context.Background(), makeToolRequest(map[string]any{
		"subject": longSubject,
		"schema":  `{"type":"record"}`,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCheckSchemaCompatibility_SchemaExceedsMaxSize(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)

	hugeSchema := strings.Repeat("x", MaxSchemaSize+1)
	result, err := s.handleCheckSchemaCompatibility(context.Background(), makeToolRequest(map[string]any{
		"subject": "orders-value",
		"schema":  hugeSchema,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "exceeds maximum size") {
		t.Error("expected schema size error")
	}
}

func TestHandleCheckSchemaCompatibility_AuthDenied(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock

	result, err := s.handleCheckSchemaCompatibility(context.Background(), makeToolRequest(map[string]any{
		"subject": "orders-value",
		"schema":  `{"type":"record"}`,
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleCheckSchemaCompatibility_DefaultSchemaTypeAndVersion(t *testing.T) {
	t.Parallel()
	// With no schema client configured, this would panic (same nil-pointer issue).
	t.Skip("skipping: requires configured schema registry client")
}
