package forecast

import (
	"math"
	"testing"
	"time"
)

func TestNewForecaster(t *testing.T) {
	t.Parallel()
	f := NewForecaster(7)
	if f.retentionDays != 7 {
		t.Fatalf("expected retentionDays=7, got %d", f.retentionDays)
	}
	if f.metrics == nil {
		t.Fatal("metrics map should be initialized")
	}
}

func TestNewForecasterDefaultRetention(t *testing.T) {
	t.Parallel()
	f := NewForecaster(0)
	if f.retentionDays != 30 {
		t.Fatalf("expected default retentionDays=30, got %d", f.retentionDays)
	}
}

func TestRecordMetricAndHistory(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	now := time.Now()

	f.RecordMetric(Metric{Topic: "orders", MetricName: "throughput", Value: 100, Timestamp: now})
	f.RecordMetric(Metric{Topic: "orders", MetricName: "throughput", Value: 200, Timestamp: now.Add(time.Hour)})

	history := f.GetMetricHistory("orders", "throughput")
	if len(history) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(history))
	}
	if history[0].Value != 100 || history[1].Value != 200 {
		t.Fatalf("unexpected metric values: %v", history)
	}
}

func TestRecordMetricRingBuffer(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	now := time.Now()

	for i := 0; i < 1100; i++ {
		f.RecordMetric(Metric{
			Topic:      "test",
			MetricName: "count",
			Value:      float64(i),
			Timestamp:  now.Add(time.Duration(i) * time.Minute),
		})
	}

	history := f.GetMetricHistory("test", "count")
	if len(history) != maxMetricsPerKey {
		t.Fatalf("expected %d metrics after ring buffer trim, got %d", maxMetricsPerKey, len(history))
	}
	// Oldest retained should be 100 (1100 - 1000).
	if history[0].Value != 100 {
		t.Fatalf("expected oldest value=100, got %.0f", history[0].Value)
	}
}

func TestGetMetricHistoryEmpty(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	history := f.GetMetricHistory("nonexistent", "metric")
	if len(history) != 0 {
		t.Fatalf("expected empty history, got %d", len(history))
	}
}

func TestGetMetricHistoryReturnsCopy(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	f.RecordMetric(Metric{Topic: "t", MetricName: "m", Value: 1, Timestamp: time.Now()})

	h := f.GetMetricHistory("t", "m")
	h[0].Value = 999

	h2 := f.GetMetricHistory("t", "m")
	if h2[0].Value != 1 {
		t.Fatal("GetMetricHistory should return a copy, not a reference")
	}
}

func TestForecastTopicInsufficientData(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	f.RecordMetric(Metric{Topic: "t", MetricName: "m", Value: 10, Timestamp: time.Now()})

	_, err := f.ForecastTopic("t", "m", 7)
	if err == nil {
		t.Fatal("expected error for insufficient data")
	}
}

func TestForecastTopicNoData(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	_, err := f.ForecastTopic("missing", "metric", 7)
	if err == nil {
		t.Fatal("expected error for missing data")
	}
}

func TestForecastTopicLinearGrowth(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Linear growth: y = 10*day + 100
	for d := 0; d < 30; d++ {
		f.RecordMetric(Metric{
			Topic:      "orders",
			MetricName: "throughput",
			Value:      100 + float64(d)*10,
			Timestamp:  base.AddDate(0, 0, d),
		})
	}

	fc, err := f.ForecastTopic("orders", "throughput", 7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if fc.Topic != "orders" || fc.MetricName != "throughput" {
		t.Fatalf("wrong topic/metric in forecast")
	}
	if fc.Trend != "growing" {
		t.Fatalf("expected trend=growing, got %q", fc.Trend)
	}
	if fc.GrowthRate <= 0 {
		t.Fatalf("expected positive growth rate, got %f", fc.GrowthRate)
	}
	if len(fc.PredictedValues) != 7 {
		t.Fatalf("expected 7 predicted points, got %d", len(fc.PredictedValues))
	}

	// Day 30 prediction should be ~100 + 30*10 = 400.
	firstPred := fc.PredictedValues[0].Value
	if math.Abs(firstPred-400) > 1 {
		t.Fatalf("expected first prediction ≈400, got %.2f", firstPred)
	}

	// Confidence bounds should bracket or equal the prediction.
	if fc.PredictedValues[0].ConfidenceLow > fc.PredictedValues[0].Value {
		t.Fatal("confidence low should be at or below predicted value")
	}
	if fc.PredictedValues[0].ConfidenceHigh < fc.PredictedValues[0].Value {
		t.Fatal("confidence high should be at or above predicted value")
	}

	if len(fc.Recommendations) == 0 {
		t.Fatal("expected at least one recommendation for growing trend")
	}
}

func TestForecastTopicShrinking(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for d := 0; d < 20; d++ {
		f.RecordMetric(Metric{
			Topic:      "logs",
			MetricName: "rate",
			Value:      500 - float64(d)*15,
			Timestamp:  base.AddDate(0, 0, d),
		})
	}

	fc, err := f.ForecastTopic("logs", "rate", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fc.Trend != "shrinking" {
		t.Fatalf("expected trend=shrinking, got %q", fc.Trend)
	}
}

func TestForecastTopicStable(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	for d := 0; d < 20; d++ {
		f.RecordMetric(Metric{
			Topic:      "health",
			MetricName: "latency",
			Value:      50,
			Timestamp:  base.AddDate(0, 0, d),
		})
	}

	fc, err := f.ForecastTopic("health", "latency", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fc.Trend != "stable" {
		t.Fatalf("expected trend=stable, got %q", fc.Trend)
	}
}

func TestLinearRegression(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	points := []Metric{
		{Value: 10, Timestamp: base},
		{Value: 20, Timestamp: base.AddDate(0, 0, 1)},
		{Value: 30, Timestamp: base.AddDate(0, 0, 2)},
	}

	slope, intercept := linearRegression(points)
	if math.Abs(slope-10) > 0.001 {
		t.Fatalf("expected slope=10, got %f", slope)
	}
	if math.Abs(intercept-10) > 0.001 {
		t.Fatalf("expected intercept=10, got %f", intercept)
	}
}

func TestLinearRegressionSinglePoint(t *testing.T) {
	t.Parallel()
	points := []Metric{
		{Value: 42, Timestamp: time.Now()},
	}
	slope, intercept := linearRegression(points)
	if slope != 0 {
		t.Fatalf("expected slope=0 for single point, got %f", slope)
	}
	if intercept != 42 {
		t.Fatalf("expected intercept=42 for single point, got %f", intercept)
	}
}

func TestLinearRegressionEmpty(t *testing.T) {
	t.Parallel()
	slope, intercept := linearRegression(nil)
	if slope != 0 || intercept != 0 {
		t.Fatalf("expected (0,0) for empty input, got (%f,%f)", slope, intercept)
	}
}

func TestDetectTrend(t *testing.T) {
	t.Parallel()
	tests := []struct {
		slope, mean float64
		want        string
	}{
		{10, 100, "growing"},
		{-10, 100, "shrinking"},
		{0.001, 100, "stable"},
		{5, 0, "growing"},
		{-5, 0, "shrinking"},
		{0, 0, "stable"},
	}
	for _, tc := range tests {
		got := detectTrend(tc.slope, tc.mean)
		if got != tc.want {
			t.Errorf("detectTrend(%f, %f) = %q, want %q", tc.slope, tc.mean, got, tc.want)
		}
	}
}

func TestDetectSeasonalityNone(t *testing.T) {
	t.Parallel()
	// Too few points.
	points := make([]Metric, 5)
	base := time.Now()
	for i := range points {
		points[i] = Metric{Value: float64(i), Timestamp: base.Add(time.Duration(i) * time.Hour)}
	}
	if s := detectSeasonality(points); s != "none" {
		t.Fatalf("expected none for few points, got %q", s)
	}
}

func TestDetectSeasonalityWeekly(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	points := make([]Metric, 28)
	for i := 0; i < 28; i++ {
		// Weekly pattern: higher values on every 7th index.
		val := 100.0
		if i%7 == 0 {
			val = 200.0
		}
		points[i] = Metric{Value: val, Timestamp: base.AddDate(0, 0, i)}
	}
	s := detectSeasonality(points)
	if s != "weekly" {
		t.Fatalf("expected weekly seasonality, got %q", s)
	}
}

func TestGenerateForecastRecommendations(t *testing.T) {
	t.Parallel()
	growing := generateForecastRecommendations("t1", "growing", 2.0)
	if len(growing) == 0 {
		t.Fatal("expected recommendations for growing trend")
	}

	rapid := generateForecastRecommendations("t2", "growing", 10.0)
	if len(rapid) < 2 {
		t.Fatal("expected extra recommendation for rapid growth")
	}

	shrinking := generateForecastRecommendations("t3", "shrinking", -1.0)
	if len(shrinking) == 0 {
		t.Fatal("expected recommendations for shrinking trend")
	}

	stable := generateForecastRecommendations("t4", "stable", 0.0)
	if len(stable) == 0 {
		t.Fatal("expected recommendations for stable trend")
	}
}

func TestMetricKey(t *testing.T) {
	t.Parallel()
	got := metricKey("orders", "throughput")
	if got != "orders:throughput" {
		t.Fatalf("expected orders:throughput, got %s", got)
	}
}

func TestForecastTimestampFormat(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for d := 0; d < 5; d++ {
		f.RecordMetric(Metric{Topic: "t", MetricName: "m", Value: float64(d), Timestamp: base.AddDate(0, 0, d)})
	}
	fc, err := f.ForecastTopic("t", "m", 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fc.Timestamp == "" {
		t.Fatal("forecast timestamp should not be empty")
	}
	// Predicted point timestamp should parse as RFC3339.
	if _, err := time.Parse(time.RFC3339, fc.PredictedValues[0].Timestamp); err != nil {
		t.Fatalf("predicted timestamp not RFC3339: %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()
	f := NewForecaster(30)
	base := time.Now()

	// Seed enough data for forecasting.
	for i := 0; i < 10; i++ {
		f.RecordMetric(Metric{Topic: "t", MetricName: "m", Value: float64(i), Timestamp: base.Add(time.Duration(i) * 24 * time.Hour)})
	}

	done := make(chan struct{})
	for g := 0; g < 4; g++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < 100; i++ {
				f.RecordMetric(Metric{Topic: "t", MetricName: "m", Value: float64(i), Timestamp: time.Now()})
				f.GetMetricHistory("t", "m")
				_, _ = f.ForecastTopic("t", "m", 3)
			}
		}()
	}
	for g := 0; g < 4; g++ {
		<-done
	}
}
