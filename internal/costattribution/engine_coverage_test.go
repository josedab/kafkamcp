package costattribution

import (
	"testing"
)

// --- GetAllLiveCosts ---

func TestGetAllLiveCostsEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	costs := e.GetAllLiveCosts()
	if len(costs) != 0 {
		t.Errorf("expected 0, got %d", len(costs))
	}
}

func TestGetAllLiveCostsMultiple(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordMetrics("topic", "t1", 1024, 512, 100, 50, 1024*1024)
	e.RecordMetrics("topic", "t2", 2048, 1024, 200, 100, 2048*1024)

	costs := e.GetAllLiveCosts()
	if len(costs) != 2 {
		t.Errorf("expected 2, got %d", len(costs))
	}
	// Should be sorted by cost descending
	if costs[0].TotalCostMonth < costs[1].TotalCostMonth {
		t.Error("expected sorted descending by cost")
	}
}

func TestGetAllLiveCostsAggregated(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordMetrics("topic", "t1", 1024, 0, 10, 0, 1024)
	e.RecordMetrics("topic", "t1", 1024, 0, 10, 0, 2048)

	costs := e.GetAllLiveCosts()
	if len(costs) != 1 {
		t.Errorf("expected 1 (aggregated), got %d", len(costs))
	}
	if costs[0].MessagesIn != 20 {
		t.Errorf("expected 20 messages in (aggregated), got %d", costs[0].MessagesIn)
	}
}

// --- GetAllocations ---

func TestGetAllocationsEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	allocs := e.GetAllocations()
	if len(allocs) != 0 {
		t.Errorf("expected 0, got %d", len(allocs))
	}
}

func TestGetAllocationsMultiple(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.CreateAllocation("team-a", "owner-a", nil, nil)
	e.CreateAllocation("team-b", "owner-b", nil, nil)

	allocs := e.GetAllocations()
	if len(allocs) != 2 {
		t.Errorf("expected 2, got %d", len(allocs))
	}
}

// --- GetBudgets ---

func TestGetBudgetsEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	budgets := e.GetBudgets()
	if len(budgets) != 0 {
		t.Errorf("expected 0, got %d", len(budgets))
	}
}

func TestGetBudgetsActiveListed(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.CreateBudget("b1", "owner", 100, nil, nil)
	e.CreateBudget("b2", "owner", 200, nil, nil)

	budgets := e.GetBudgets()
	if len(budgets) != 2 {
		t.Errorf("expected 2, got %d", len(budgets))
	}
}

func TestGetBudgetsWithStatus(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordMetrics("topic", "expensive", 0, 0, 0, 0, 1024*1024*1024*100)

	budget, _ := e.CreateBudget("tight", "owner", 0.001, []string{"expensive"}, nil)
	if budget.Status == "ok" && budget.CurrentSpend > budget.MonthlyLimit {
		t.Error("expected non-ok status when spend > limit")
	}
}

// --- SetPricing ---

func TestSetPricingOverrideDefaults(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.SetPricing(PricingTier{
		Provider:        "custom",
		StoragePerGBHr:  0.50,
		NetworkPerGBHr:  0.10,
		ComputePerCPUHr: 1.00,
		PartitionPerHr:  0.01,
	})

	cost := e.RecordMetrics("topic", "t1", 0, 0, 0, 0, 1024*1024*1024)
	if cost.StorageCostHr == 0 {
		t.Error("expected non-zero storage cost with custom pricing")
	}
}

func TestSetPricingZeroPrices(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.SetPricing(PricingTier{})

	cost := e.RecordMetrics("topic", "t1", 1024, 1024, 100, 100, 1024*1024*1024)
	// With zero pricing, costs should reflect zero rates (except compute is multiplied by 0.1)
	if cost.StorageCostHr != 0 {
		t.Errorf("expected 0 storage cost with zero pricing, got %f", cost.StorageCostHr)
	}
}

// --- checkBudgetAlerts ---

func TestCheckBudgetAlerts80Percent(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordMetrics("topic", "t1", 0, 0, 0, 0, 1024*1024*1024*10)

	// Get the cost to calibrate budget
	cost, _ := e.GetLiveCost("topic", "t1")
	// Set budget just above current spend to get warning
	limit := cost.TotalCostMonth * 1.15
	budget, _ := e.CreateBudget("warn", "owner", limit, []string{"t1"}, nil)
	if budget.Status != "warning" {
		t.Errorf("expected warning status at ~87%%, got %s (spend=%.4f limit=%.4f)", budget.Status, budget.CurrentSpend, budget.MonthlyLimit)
	}
}

func TestCheckBudgetAlertsExceeded(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordMetrics("topic", "t1", 0, 0, 0, 0, 1024*1024*1024*100)

	budget, _ := e.CreateBudget("exceeded", "owner", 0.001, []string{"t1"}, nil)
	if budget.Status != "exceeded" {
		t.Errorf("expected exceeded status, got %s", budget.Status)
	}
}

func TestCheckBudgetAlertsFiredOnRecordMetrics(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	budget, _ := e.CreateBudget("b", "owner", 0.001, []string{"t1"}, nil)
	if budget.Status != "ok" {
		t.Errorf("expected ok initially, got %s", budget.Status)
	}

	// Record large metrics to trigger alert
	e.RecordMetrics("topic", "t1", 0, 0, 0, 0, 1024*1024*1024*100)

	budgets := e.GetBudgets()
	found := false
	for _, b := range budgets {
		if b.Name == "b" {
			found = true
			if b.Status == "ok" {
				t.Error("expected status to change after recording large metrics")
			}
		}
	}
	if !found {
		t.Error("budget not found")
	}
}

// --- budgetStatus ---

func TestBudgetStatusOk(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	budget := &Budget{MonthlyLimit: 100, CurrentSpend: 50}
	if e.budgetStatus(budget) != "ok" {
		t.Error("expected ok at 50%")
	}
}

func TestBudgetStatusWarning(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	budget := &Budget{MonthlyLimit: 100, CurrentSpend: 85}
	if e.budgetStatus(budget) != "warning" {
		t.Error("expected warning at 85%")
	}
}

func TestBudgetStatusCritical(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	budget := &Budget{MonthlyLimit: 100, CurrentSpend: 95}
	if e.budgetStatus(budget) != "critical" {
		t.Error("expected critical at 95%")
	}
}

func TestBudgetStatusExceeded(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	budget := &Budget{MonthlyLimit: 100, CurrentSpend: 150}
	if e.budgetStatus(budget) != "exceeded" {
		t.Error("expected exceeded at 150%")
	}
}

func TestBudgetStatusZeroLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	budget := &Budget{MonthlyLimit: 0, CurrentSpend: 50}
	if e.budgetStatus(budget) != "ok" {
		t.Error("expected ok for zero limit (no division by zero)")
	}
}

func TestBudgetStatusNegativeLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	budget := &Budget{MonthlyLimit: -10, CurrentSpend: 50}
	if e.budgetStatus(budget) != "ok" {
		t.Error("expected ok for negative limit")
	}
}

// --- Edge cases ---

func TestCreateAllocationEmptyName(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.CreateAllocation("", "owner", nil, nil)
	if err == nil {
		t.Error("expected error for empty name")
	}
}

func TestCreateBudgetInvalidLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.CreateBudget("b", "owner", 0, nil, nil)
	if err == nil {
		t.Error("expected error for zero limit")
	}
}

func TestCreateBudgetNegativeLimit(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.CreateBudget("b", "owner", -100, nil, nil)
	if err == nil {
		t.Error("expected error for negative limit")
	}
}

func TestCreateBudgetDefaultThresholds(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	budget, _ := e.CreateBudget("b", "owner", 100, nil, nil)
	if len(budget.AlertThresholds) != 4 {
		t.Errorf("expected 4 default thresholds, got %d", len(budget.AlertThresholds))
	}
}

func TestGetLiveCostNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.GetLiveCost("topic", "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent resource")
	}
}

func TestCreateAllocationWithResources(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordMetrics("topic", "orders", 1024, 512, 100, 50, 1024*1024*1024)
	e.RecordMetrics("topic", "users", 512, 256, 50, 25, 512*1024*1024)

	alloc, err := e.CreateAllocation("commerce", "team-a", []string{"orders", "users"}, map[string]string{"env": "prod"})
	if err != nil {
		t.Fatalf("CreateAllocation failed: %v", err)
	}
	if alloc.TotalCostMonth <= 0 {
		t.Error("expected positive total cost")
	}
	if len(alloc.ResourceCosts) != 2 {
		t.Errorf("expected 2 resource costs, got %d", len(alloc.ResourceCosts))
	}
}
