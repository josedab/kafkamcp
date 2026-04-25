package costattribution

import (
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestRecordAndGetCost(t *testing.T) {
	e := NewEngine(testLogger())

	cost := e.RecordMetrics("topic", "orders", 1024*1024, 512*1024, 1000, 500, 1024*1024*1024)
	if cost.TotalCostMonth <= 0 {
		t.Error("expected positive monthly cost")
	}

	retrieved, err := e.GetLiveCost("topic", "orders")
	if err != nil {
		t.Fatalf("GetLiveCost failed: %v", err)
	}
	if retrieved.MessagesIn != 1000 {
		t.Errorf("expected 1000 messages in, got %d", retrieved.MessagesIn)
	}
}

func TestCreateAllocation(t *testing.T) {
	e := NewEngine(testLogger())

	e.RecordMetrics("topic", "orders", 0, 0, 0, 0, 1024*1024*1024)

	alloc, err := e.CreateAllocation("commerce-team", "team-commerce", []string{"orders"}, nil)
	if err != nil {
		t.Fatalf("CreateAllocation failed: %v", err)
	}
	if alloc.Name != "commerce-team" {
		t.Errorf("expected commerce-team, got %s", alloc.Name)
	}
}

func TestCreateBudget(t *testing.T) {
	e := NewEngine(testLogger())

	budget, err := e.CreateBudget("dev-budget", "team-dev", 500.0, nil, nil)
	if err != nil {
		t.Fatalf("CreateBudget failed: %v", err)
	}
	if budget.MonthlyLimit != 500.0 {
		t.Errorf("expected limit 500, got %f", budget.MonthlyLimit)
	}
	if budget.Status != "ok" {
		t.Errorf("expected status ok, got %s", budget.Status)
	}
}

func TestBudgetAlerts(t *testing.T) {
	e := NewEngine(testLogger())

	// Record large usage
	e.RecordMetrics("topic", "big-topic", 0, 0, 0, 0, 1024*1024*1024*100)

	budget, _ := e.CreateBudget("small-budget", "team-a", 0.01, []string{"big-topic"}, nil)
	if budget.Status == "ok" && budget.CurrentSpend > budget.MonthlyLimit {
		t.Error("expected budget alert for exceeded spend")
	}
}
