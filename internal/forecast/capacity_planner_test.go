package forecast

import (
	"testing"
	"time"
)

func TestPredictiveCapacityPlan_Basic(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)

	// Record some metrics
	base := time.Now().Add(-30 * 24 * time.Hour)
	for i := 0; i < 30; i++ {
		ts := base.Add(time.Duration(i) * 24 * time.Hour)
		f.RecordMetric(Metric{Topic: "orders", MetricName: "storage", Value: float64(1000 + i*100), Timestamp: ts})
		f.RecordMetric(Metric{Topic: "orders", MetricName: "throughput", Value: float64(500 + i*10), Timestamp: ts})
		f.RecordMetric(Metric{Topic: "events", MetricName: "storage", Value: float64(2000 + i*50), Timestamp: ts})
	}

	plan := f.PredictiveCapacityPlan("test-cluster", 30, 3, 1000, 0.10, 500)
	if plan.Cluster != "test-cluster" {
		t.Errorf("expected test-cluster, got %s", plan.Cluster)
	}
	if len(plan.TopicForecasts) < 2 {
		t.Errorf("expected at least 2 topic forecasts, got %d", len(plan.TopicForecasts))
	}
	if plan.CostProjection.Currency != "USD" {
		t.Errorf("expected USD currency, got %s", plan.CostProjection.Currency)
	}
	if plan.RiskAssessment.OverallRisk == "" {
		t.Error("expected risk assessment")
	}
}

func TestPredictiveCapacityPlan_Empty(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)

	plan := f.PredictiveCapacityPlan("empty-cluster", 30, 3, 1000, 0.10, 500)
	if plan == nil {
		t.Fatal("expected non-nil plan even for empty metrics")
	}
	if len(plan.TopicForecasts) != 0 {
		t.Errorf("expected 0 forecasts, got %d", len(plan.TopicForecasts))
	}
}

func TestPredictiveCapacityPlan_RapidGrowth(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)

	base := time.Now().Add(-30 * 24 * time.Hour)
	for i := 0; i < 30; i++ {
		ts := base.Add(time.Duration(i) * 24 * time.Hour)
		// Exponential-ish growth
		f.RecordMetric(Metric{Topic: "hot-topic", MetricName: "storage", Value: float64(1000 * (1 + float64(i)*0.1)), Timestamp: ts})
	}

	plan := f.PredictiveCapacityPlan("test", 30, 3, 1, 0.10, 500)

	// Should detect growth
	if len(plan.TopicForecasts) == 0 {
		t.Fatal("expected topic forecasts")
	}
}
