package finops

import (
	"log/slog"
	"testing"
)

func newTestDashboard() *Dashboard {
	d := NewDashboard(slog.Default())
	// Seed some topic costs
	d.RecordTopicCost("orders", 50.0, 25.0, 12, 500)
	d.RecordTopicCost("events", 30.0, 15.0, 6, 200)
	d.RecordTopicCost("logs", 100.0, 200.0, 24, 5) // underutilized, high storage
	d.RecordTopicCost("clicks", 20.0, 5.0, 48, 3)  // over-partitioned
	return d
}

func TestRecordTopicCost(t *testing.T) {
	t.Parallel()
	d := NewDashboard(slog.Default())
	d.RecordTopicCost("test", 10.0, 5.0, 4, 100)

	summary := d.GetSummary()
	if summary["topic_count"].(int) != 1 {
		t.Errorf("expected 1 topic, got %v", summary["topic_count"])
	}
}

func TestTakeSnapshot(t *testing.T) {
	t.Parallel()
	d := newTestDashboard()

	snap := d.TakeSnapshot("aws-msk")
	if snap.TotalMonthly <= 0 {
		t.Error("expected positive total cost")
	}
	if snap.Provider != "aws-msk" {
		t.Errorf("expected aws-msk, got %s", snap.Provider)
	}
	if len(snap.TopicBreakdown) != 4 {
		t.Errorf("expected 4 topics, got %d", len(snap.TopicBreakdown))
	}

	// Second snapshot should show trend
	d.RecordTopicCost("orders", 60.0, 30.0, 12, 600)
	snap2 := d.TakeSnapshot("aws-msk")
	if snap2.TrendDirection == "" {
		t.Error("expected trend direction")
	}
}

func TestGetRightsizeRecommendations(t *testing.T) {
	t.Parallel()
	d := newTestDashboard()

	recs := d.GetRightsizeRecommendations()
	if len(recs) == 0 {
		t.Error("expected optimization recommendations")
	}

	// Should have recommendations for logs (underutilized, high storage) and clicks (over-partitioned)
	found := map[string]bool{}
	for _, r := range recs {
		found[r.Type] = true
		if r.Savings <= 0 {
			t.Errorf("expected positive savings for %s", r.Topic)
		}
	}
}

func TestForecastSpend(t *testing.T) {
	t.Parallel()
	d := newTestDashboard()
	d.TakeSnapshot("aws-msk")
	d.TakeSnapshot("aws-msk")

	forecast := d.ForecastSpend(6)
	if forecast.CurrentMonthly <= 0 {
		t.Error("expected positive current monthly")
	}
	if len(forecast.Projections) != 6 {
		t.Errorf("expected 6 projections, got %d", len(forecast.Projections))
	}
	for _, p := range forecast.Projections {
		if p.Projected <= 0 {
			t.Errorf("expected positive projection for %s", p.Month)
		}
		if p.LowerBound > p.Projected || p.UpperBound < p.Projected {
			t.Errorf("bounds should contain projected value")
		}
	}
}

func TestSetBudget(t *testing.T) {
	t.Parallel()
	d := newTestDashboard()

	alert, err := d.SetBudget("monthly-limit", 500)
	if err != nil {
		t.Fatal(err)
	}
	if alert.Status == "" {
		t.Error("expected status")
	}

	// Exceeded budget
	alert2, _ := d.SetBudget("tight-budget", 50)
	if alert2.Status != "exceeded" {
		t.Errorf("expected exceeded for tight budget, got %s", alert2.Status)
	}

	budgets := d.GetBudgets()
	if len(budgets) != 2 {
		t.Errorf("expected 2 budgets, got %d", len(budgets))
	}
}

func TestSetBudgetValidation(t *testing.T) {
	t.Parallel()
	d := NewDashboard(slog.Default())

	if _, err := d.SetBudget("", 100); err == nil {
		t.Error("expected error for empty name")
	}
	if _, err := d.SetBudget("test", -1); err == nil {
		t.Error("expected error for negative threshold")
	}
}

func TestGetSummary(t *testing.T) {
	t.Parallel()
	d := newTestDashboard()

	summary := d.GetSummary()
	if summary["total_monthly_cost"].(float64) <= 0 {
		t.Error("expected positive total cost")
	}
	if summary["topic_count"].(int) != 4 {
		t.Errorf("expected 4 topics, got %v", summary["topic_count"])
	}
	if summary["optimization_count"].(int) == 0 {
		t.Error("expected optimization recommendations")
	}
}

func TestGetSnapshots(t *testing.T) {
	t.Parallel()
	d := newTestDashboard()
	d.TakeSnapshot("provider-a")
	d.TakeSnapshot("provider-a")
	d.TakeSnapshot("provider-a")

	snaps := d.GetSnapshots(2)
	if len(snaps) != 2 {
		t.Errorf("expected 2 snapshots, got %d", len(snaps))
	}
}
