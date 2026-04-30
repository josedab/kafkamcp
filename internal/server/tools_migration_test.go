package server

import (
	"context"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleMigrationExecute =====================

func TestHandleMigrationExecute_MissingPlanID(t *testing.T) {
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	result, err := s.handleMigrationExecute(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "plan_id") {
		t.Errorf("expected plan_id error, got: %s", text)
	}
}

func TestHandleMigrationExecute_NonexistentPlan(t *testing.T) {
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	result, err := s.handleMigrationExecute(context.Background(), makeToolRequest(map[string]any{
		"plan_id": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleMigrationStatus =====================

func TestHandleMigrationStatus_MissingPlanID(t *testing.T) {
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	result, err := s.handleMigrationStatus(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "plan_id") {
		t.Errorf("expected plan_id error, got: %s", text)
	}
}

func TestHandleMigrationStatus_NonexistentPlan(t *testing.T) {
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	result, err := s.handleMigrationStatus(context.Background(), makeToolRequest(map[string]any{
		"plan_id": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleMigrationStatus_InvalidView(t *testing.T) {
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	result, err := s.handleMigrationStatus(context.Background(), makeToolRequest(map[string]any{
		"plan_id": "some-plan",
		"view":    "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleMigrationRollback =====================

func TestHandleMigrationRollback_MissingPlanID(t *testing.T) {
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	result, err := s.handleMigrationRollback(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "plan_id") {
		t.Errorf("expected plan_id error, got: %s", text)
	}
}

func TestHandleMigrationRollback_NonexistentPlan(t *testing.T) {
	s := newTestServerWithMock(t, &kafka.MockKafkaManager{})
	result, err := s.handleMigrationRollback(context.Background(), makeToolRequest(map[string]any{
		"plan_id": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
