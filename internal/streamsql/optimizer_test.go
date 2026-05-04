package streamsql

import (
	"log/slog"
	"testing"
)

func TestOptimizeSimpleQuery(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	plan, err := e.OptimizeQuery("SELECT * FROM orders LIMIT 100", nil, nil, nil)
	if err != nil {
		t.Fatalf("OptimizeQuery: %v", err)
	}
	if plan.EstimatedCost <= 0 {
		t.Error("expected positive cost")
	}
	if len(plan.Steps) == 0 {
		t.Error("expected execution steps")
	}
	if plan.BudgetStatus != "within_budget" {
		t.Errorf("expected within_budget, got %s", plan.BudgetStatus)
	}
}

func TestOptimizeWithFilter(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	stats := &TopicStats{
		Topic: "events", Partitions: 12,
		MessagesPerSec: 5000, AvgMessageSizeKB: 2.0,
	}

	plan, err := e.OptimizeQuery("SELECT user_id, COUNT(*) FROM events WHERE status = 'active' GROUP BY user_id", stats, nil, nil)
	if err != nil {
		t.Fatalf("OptimizeQuery: %v", err)
	}

	// Should have scan, filter, aggregate steps
	if len(plan.Steps) < 3 {
		t.Errorf("expected at least 3 steps, got %d", len(plan.Steps))
	}

	// Should have optimizations
	if len(plan.Optimizations) == 0 {
		t.Error("expected optimizations")
	}
}

func TestOptimizeWithBudget(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	budget := e.CreateBudget("test-budget", 0.001, 1.0, 0.8)

	plan, err := e.OptimizeQuery("SELECT * FROM large_topic", &TopicStats{
		MessagesPerSec: 100000, AvgMessageSizeKB: 5.0, Partitions: 24,
	}, nil, budget)
	if err != nil {
		t.Fatalf("OptimizeQuery: %v", err)
	}
	if plan.BudgetStatus == "within_budget" {
		t.Log("query cost:", plan.EstimatedCost, "budget:", budget.MaxCostPerQuery)
	}
}

func TestBudgetCRUD(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	budget := e.CreateBudget("team-budget", 10.0, 100.0, 0.8)
	if budget.ID == "" {
		t.Error("expected budget ID")
	}

	b, err := e.GetBudget(budget.ID)
	if err != nil {
		t.Fatalf("GetBudget: %v", err)
	}
	if b.MaxCostPerQuery != 10.0 {
		t.Errorf("expected 10.0, got %f", b.MaxCostPerQuery)
	}

	budgets := e.ListBudgets()
	if len(budgets) != 1 {
		t.Errorf("expected 1 budget, got %d", len(budgets))
	}
}

func TestOptimizeWithWindow(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	plan, err := e.OptimizeQuery("SELECT COUNT(*) FROM events WINDOW TUMBLING 60s", nil, nil, nil)
	if err != nil {
		t.Fatalf("OptimizeQuery: %v", err)
	}

	// Should have a window step
	hasWindow := false
	for _, step := range plan.Steps {
		if step.Operation == "window" {
			hasWindow = true
		}
	}
	if !hasWindow {
		t.Error("expected window step in plan")
	}
}

func TestOptimizeInvalidSQL(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	_, err := e.OptimizeQuery("", nil, nil, nil)
	if err == nil {
		t.Error("expected error for empty SQL")
	}

	_, err = e.OptimizeQuery("INSERT INTO orders VALUES (1)", nil, nil, nil)
	if err == nil {
		t.Error("expected error for non-SELECT query")
	}
}

func TestOptimizeBudgetOverLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	// Create a very tight budget
	budget := e.CreateBudget("tight", 0.0001, 0.001, 0.5)

	plan, err := e.OptimizeQuery("SELECT * FROM big_topic", &TopicStats{
		MessagesPerSec: 50000, AvgMessageSizeKB: 10.0, Partitions: 48,
	}, nil, budget)
	if err != nil {
		t.Fatalf("OptimizeQuery: %v", err)
	}
	if plan.BudgetStatus != "over_budget" {
		t.Errorf("expected over_budget status, got %s", plan.BudgetStatus)
	}
}

func TestOptimizeWithLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	plan, err := e.OptimizeQuery("SELECT * FROM orders LIMIT 10", nil, nil, nil)
	if err != nil {
		t.Fatalf("OptimizeQuery: %v", err)
	}

	hasLimit := false
	for _, step := range plan.Steps {
		if step.Operation == "limit" {
			hasLimit = true
		}
	}
	if !hasLimit {
		t.Error("expected limit step in plan")
	}
}

func TestBudgetNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	_, err := e.GetBudget("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent budget")
	}
}
