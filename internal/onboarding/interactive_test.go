package onboarding

import (
	"context"
	"log/slog"
	"testing"
)

func TestInteractiveOnboarder_StartSession(t *testing.T) {
	t.Parallel()
	engine := NewEngine(slog.Default())
	onboarder := NewInteractiveOnboarder(engine)

	session := onboarder.StartSession(context.Background(), "agent-1")
	if session.Status != "active" {
		t.Errorf("expected active, got %s", session.Status)
	}
	if len(session.Steps) < 5 {
		t.Errorf("expected at least 5 steps, got %d", len(session.Steps))
	}
	if session.Steps[0].Status != "active" {
		t.Errorf("expected first step to be active, got %s", session.Steps[0].Status)
	}
}

func TestInteractiveOnboarder_AdvanceStep(t *testing.T) {
	t.Parallel()
	engine := NewEngine(slog.Default())
	onboarder := NewInteractiveOnboarder(engine)
	session := onboarder.StartSession(context.Background(), "agent-1")

	result, err := onboarder.AdvanceStep(session.ID, "Found 10 topics")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NextStep == nil {
		t.Fatal("expected next step")
	}
	if session.Steps[0].Status != "completed" {
		t.Errorf("expected step 0 completed, got %s", session.Steps[0].Status)
	}
}

func TestInteractiveOnboarder_SkipStep(t *testing.T) {
	t.Parallel()
	engine := NewEngine(slog.Default())
	onboarder := NewInteractiveOnboarder(engine)
	session := onboarder.StartSession(context.Background(), "agent-1")

	result, err := onboarder.SkipStep(session.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if session.Steps[0].Status != "skipped" {
		t.Errorf("expected step 0 skipped, got %s", session.Steps[0].Status)
	}
	if result.NextStep == nil {
		t.Fatal("expected next step after skip")
	}
}

func TestInteractiveOnboarder_CompleteAll(t *testing.T) {
	t.Parallel()
	engine := NewEngine(slog.Default())
	onboarder := NewInteractiveOnboarder(engine)
	session := onboarder.StartSession(context.Background(), "agent-1")

	for i := 0; i < len(session.Steps); i++ {
		result, err := onboarder.AdvanceStep(session.ID, "done")
		if err != nil {
			t.Fatalf("step %d: unexpected error: %v", i, err)
		}
		if i == len(session.Steps)-1 {
			if session.Status != "completed" {
				t.Errorf("expected completed after all steps, got %s", session.Status)
			}
			if len(result.Suggestions) == 0 {
				t.Error("expected suggestions on completion")
			}
		}
	}
}

func TestInteractiveOnboarder_GetProgress(t *testing.T) {
	t.Parallel()
	engine := NewEngine(slog.Default())
	onboarder := NewInteractiveOnboarder(engine)
	session := onboarder.StartSession(context.Background(), "agent-1")

	_, _ = onboarder.AdvanceStep(session.ID, "done")
	_, _ = onboarder.SkipStep(session.ID)

	progress := onboarder.GetProgress(session.ID)
	if progress["completed"].(int) != 1 {
		t.Errorf("expected 1 completed, got %v", progress["completed"])
	}
	if progress["skipped"].(int) != 1 {
		t.Errorf("expected 1 skipped, got %v", progress["skipped"])
	}
}

func TestInteractiveOnboarder_GetCurrentStep(t *testing.T) {
	t.Parallel()
	engine := NewEngine(slog.Default())
	onboarder := NewInteractiveOnboarder(engine)
	session := onboarder.StartSession(context.Background(), "agent-1")

	step, err := onboarder.GetCurrentStep(session.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if step.Number != 1 {
		t.Errorf("expected step 1, got %d", step.Number)
	}
}

func TestGenerateContextualHelp(t *testing.T) {
	t.Parallel()
	step := InteractiveStep{
		Number:    1,
		Title:     "Connect to Kafka",
		Guidance:  "Let's connect!",
		ToolToUse: "kafka_list_topics",
		ToolArgs:  `{"cluster": "default"}`,
		Tips:      []string{"Check port 9092"},
	}
	help := GenerateContextualHelp(step, SessionContext{})
	if help == "" {
		t.Error("expected non-empty help text")
	}
}

func TestInteractiveOnboarder_SessionNotFound(t *testing.T) {
	t.Parallel()
	engine := NewEngine(slog.Default())
	onboarder := NewInteractiveOnboarder(engine)

	_, err := onboarder.GetSession("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent session")
	}
}
