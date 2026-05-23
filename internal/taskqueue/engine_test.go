package taskqueue

import (
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestPublishAndClaim(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	task, err := e.Publish("build-queue", "compile", "agent-1", PriorityHigh, 3, map[string]any{"repo": "test"}, 0)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if task.Status != "pending" {
		t.Errorf("expected pending, got %s", task.Status)
	}

	claimed, err := e.Claim("agent-2", "build-queue")
	if err != nil {
		t.Fatalf("Claim failed: %v", err)
	}
	if claimed.ClaimedBy != "agent-2" {
		t.Errorf("expected claimed by agent-2, got %s", claimed.ClaimedBy)
	}
}

func TestPriorityOrdering(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	_, _ = e.Publish("q", "low-task", "a1", PriorityLow, 0, nil, 0)
	_, _ = e.Publish("q", "critical-task", "a1", PriorityCritical, 0, nil, 0)

	claimed, _ := e.Claim("worker", "q")
	if claimed.Type != "critical-task" {
		t.Errorf("expected critical task first, got %s", claimed.Type)
	}
}

func TestCompleteAndRetry(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	_, _ = e.Publish("q", "task1", "a1", PriorityMedium, 2, nil, 0)
	task, _ := e.Claim("worker", "q")

	// Fail it (should retry)
	result, _ := e.Complete("worker", task.ID, "failed", nil, "error")
	if result.Status != "pending" {
		t.Errorf("expected pending after retry, got %s", result.Status)
	}

	// Claim and fail again (should go to dead letter)
	task2, _ := e.Claim("worker", "q")
	result2, _ := e.Complete("worker", task2.ID, "failed", nil, "error again")
	if result2.Status != "dead_letter" {
		t.Errorf("expected dead_letter, got %s", result2.Status)
	}
}

func TestStats(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	_, _ = e.Publish("build", "t1", "a1", PriorityMedium, 3, nil, 0)
	_, _ = e.Publish("build", "t2", "a1", PriorityHigh, 3, nil, 0)

	stats := e.Stats("build")
	if len(stats) != 1 {
		t.Fatalf("expected 1 queue stats, got %d", len(stats))
	}
	if stats[0].Pending != 2 {
		t.Errorf("expected 2 pending, got %d", stats[0].Pending)
	}
}
