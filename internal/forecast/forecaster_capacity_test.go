package forecast

import (
	"math"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// ExponentialSmoothing
// ---------------------------------------------------------------------------

func TestExponentialSmoothing(t *testing.T) {
	values := []float64{10, 20, 30, 40, 50}
	result := ExponentialSmoothing(values, 0.3, 3)

	if len(result) != 3 {
		t.Fatalf("expected 3 predictions, got %d", len(result))
	}

	// Trend from last 3 values: (50-30)/2 = 10
	// Each prediction should increase by trend.
	for i := 1; i < len(result); i++ {
		if result[i] <= result[i-1] {
			t.Fatalf("predictions should increase with upward trend, got %v", result)
		}
	}
}

func TestExponentialSmoothingEmpty(t *testing.T) {
	result := ExponentialSmoothing(nil, 0.5, 5)
	if result != nil {
		t.Fatalf("expected nil for empty input, got %v", result)
	}
}

func TestExponentialSmoothingSingleValue(t *testing.T) {
	result := ExponentialSmoothing([]float64{42}, 0.5, 3)
	if len(result) != 3 {
		t.Fatalf("expected 3 predictions, got %d", len(result))
	}
	// Single value, no trend (< 3 points) → all predictions equal smoothed (42).
	for i, v := range result {
		if math.Abs(v-42) > 1e-9 {
			t.Fatalf("prediction[%d] should be 42, got %f", i, v)
		}
	}
}

func TestExponentialSmoothingAlphaEdgeCases(t *testing.T) {
	values := []float64{10, 20, 30}

	// alpha <= 0 → defaults to 0.3
	r1 := ExponentialSmoothing(values, -1, 2)
	r2 := ExponentialSmoothing(values, 0, 2)
	if len(r1) != 2 || len(r2) != 2 {
		t.Fatal("should still produce predictions with invalid alpha")
	}

	// alpha = 1 → smoothed = last value
	r3 := ExponentialSmoothing(values, 1.0, 1)
	if len(r3) != 1 {
		t.Fatalf("expected 1 prediction, got %d", len(r3))
	}

	// alpha > 1 → defaults to 0.3
	r4 := ExponentialSmoothing(values, 2.0, 1)
	if len(r4) != 1 {
		t.Fatalf("expected 1 prediction, got %d", len(r4))
	}
}

func TestExponentialSmoothingZeroHorizon(t *testing.T) {
	result := ExponentialSmoothing([]float64{1, 2, 3}, 0.5, 0)
	if len(result) != 0 {
		t.Fatalf("expected 0 predictions for zero horizon, got %d", len(result))
	}
}

// ---------------------------------------------------------------------------
// GenerateCapacityPlan
// ---------------------------------------------------------------------------

func TestGenerateCapacityPlan(t *testing.T) {
	f := NewForecaster(30)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Record throughput and storage metrics for a topic.
	for d := 0; d < 10; d++ {
		f.RecordMetric(Metric{
			Topic: "orders", MetricName: "throughput",
			Value: 1000 + float64(d)*100, Timestamp: base.AddDate(0, 0, d),
		})
		f.RecordMetric(Metric{
			Topic: "orders", MetricName: "storage",
			Value: float64(d+1) * 1024 * 1024 * 100, // growing storage
			Timestamp: base.AddDate(0, 0, d),
		})
	}

	plan := f.GenerateCapacityPlan("test-cluster", 30, 1000)

	if plan.Cluster != "test-cluster" {
		t.Fatalf("expected cluster=test-cluster, got %q", plan.Cluster)
	}
	if plan.Timestamp == "" {
		t.Fatal("timestamp should not be empty")
	}
	if len(plan.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(plan.Topics))
	}
	tc := plan.Topics[0]
	if tc.Topic != "orders" {
		t.Fatalf("expected topic=orders, got %q", tc.Topic)
	}
	if tc.CurrentThroughput <= 0 {
		t.Fatalf("expected positive current throughput, got %f", tc.CurrentThroughput)
	}
	if tc.PredictedThroughput <= 0 {
		t.Fatalf("expected positive predicted throughput, got %f", tc.PredictedThroughput)
	}
	if len(plan.Recommendations) == 0 {
		t.Fatal("expected at least one recommendation")
	}
}

func TestGenerateCapacityPlanEmptyMetrics(t *testing.T) {
	f := NewForecaster(30)
	plan := f.GenerateCapacityPlan("empty-cluster", 7, 500)

	if plan.Cluster != "empty-cluster" {
		t.Fatalf("expected cluster=empty-cluster, got %q", plan.Cluster)
	}
	if len(plan.Topics) != 0 {
		t.Fatalf("expected 0 topics, got %d", len(plan.Topics))
	}
	// Should still produce a healthy recommendation.
	if len(plan.Recommendations) == 0 {
		t.Fatal("expected at least one recommendation")
	}
	if plan.Recommendations[0].Priority != "low" {
		t.Fatalf("expected low priority healthy recommendation, got %q", plan.Recommendations[0].Priority)
	}
}

func TestGenerateCapacityPlanDefaults(t *testing.T) {
	f := NewForecaster(30)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for d := 0; d < 5; d++ {
		f.RecordMetric(Metric{
			Topic: "t", MetricName: "throughput",
			Value: 100, Timestamp: base.AddDate(0, 0, d),
		})
	}

	// horizonDays=0 and maxStorageGB=0 should use defaults (30 and 1000).
	plan := f.GenerateCapacityPlan("c", 0, 0)
	if plan == nil {
		t.Fatal("plan should not be nil")
	}
	if len(plan.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(plan.Topics))
	}
}

func TestGenerateCapacityPlanMultipleTopics(t *testing.T) {
	f := NewForecaster(30)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, topic := range []string{"topic-a", "topic-b"} {
		for d := 0; d < 5; d++ {
			f.RecordMetric(Metric{
				Topic: topic, MetricName: "throughput",
				Value: 200, Timestamp: base.AddDate(0, 0, d),
			})
		}
	}

	plan := f.GenerateCapacityPlan("multi", 7, 1000)
	if len(plan.Topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(plan.Topics))
	}
	if plan.ClusterCapacity.TotalPartitions != 2 {
		t.Fatalf("expected TotalPartitions=2, got %d", plan.ClusterCapacity.TotalPartitions)
	}
}

// ---------------------------------------------------------------------------
// splitMetricKey
// ---------------------------------------------------------------------------

func TestSplitMetricKey(t *testing.T) {
	tests := []struct {
		input string
		want  [2]string
	}{
		{"orders:throughput", [2]string{"orders", "throughput"}},
		{"my-topic:storage", [2]string{"my-topic", "storage"}},
		{"a:b:c", [2]string{"a:b", "c"}},         // splits on last colon
		{"nocolon", [2]string{"nocolon", ""}},     // no colon
		{":leading", [2]string{"", "leading"}},    // leading colon
		{"trailing:", [2]string{"trailing", ""}},   // trailing colon
		{"", [2]string{"", ""}},                    // empty string
	}

	for _, tc := range tests {
		got := splitMetricKey(tc.input)
		if got != tc.want {
			t.Errorf("splitMetricKey(%q) = %v, want %v", tc.input, got, tc.want)
		}
	}
}

// ---------------------------------------------------------------------------
// extractValues
// ---------------------------------------------------------------------------

func TestExtractValues(t *testing.T) {
	points := []Metric{
		{Value: 1.1},
		{Value: 2.2},
		{Value: 3.3},
	}
	vals := extractValues(points)
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d", len(vals))
	}
	expected := []float64{1.1, 2.2, 3.3}
	for i, v := range vals {
		if math.Abs(v-expected[i]) > 1e-9 {
			t.Errorf("extractValues[%d] = %f, want %f", i, v, expected[i])
		}
	}
}

func TestExtractValuesEmpty(t *testing.T) {
	vals := extractValues(nil)
	if len(vals) != 0 {
		t.Fatalf("expected 0 values for nil input, got %d", len(vals))
	}
}
