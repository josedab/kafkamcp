package timeseries

import (
	"log/slog"
	"testing"
	"time"
)

func TestRecordAndQuery(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	now := time.Now()
	for i := 0; i < 100; i++ {
		e.Record(DataPoint{
			Timestamp: now.Add(time.Duration(-i) * time.Minute),
			Value:     float64(100 + i%10),
			Metric:    MetricThroughputIn,
			Source:    "orders",
		})
	}

	result, err := e.Query(MetricThroughputIn, "orders", "1h")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if result.Stats.Count == 0 {
		t.Fatal("expected data points")
	}
	if result.Stats.Min < 100 || result.Stats.Max > 109 {
		t.Errorf("unexpected stats: min=%f max=%f", result.Stats.Min, result.Stats.Max)
	}
}

func TestCompare(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	now := time.Now()
	// Record data across two windows
	for i := 0; i < 200; i++ {
		val := float64(100)
		if i < 60 {
			val = 150 // recent hour has higher values
		}
		e.Record(DataPoint{
			Timestamp: now.Add(time.Duration(-i) * time.Minute),
			Value:     val,
			Metric:    MetricConsumerLag,
			Source:    "group-1",
		})
	}

	result, err := e.Compare(MetricConsumerLag, "group-1", "24h", "1h")
	if err != nil {
		t.Fatalf("Compare failed: %v", err)
	}
	if result.Assessment == "" {
		t.Error("expected assessment")
	}
}

func TestBaseline(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	now := time.Now()
	// Normal baseline
	for i := 60; i < 200; i++ {
		e.Record(DataPoint{
			Timestamp: now.Add(time.Duration(-i) * time.Minute),
			Value:     float64(100),
			Metric:    MetricLatencyP50,
			Source:    "service-a",
		})
	}
	// Current window with spikes
	for i := 0; i < 60; i++ {
		val := 100.0
		if i%10 == 0 {
			val = 500.0
		}
		e.Record(DataPoint{
			Timestamp: now.Add(time.Duration(-i) * time.Minute),
			Value:     val,
			Metric:    MetricLatencyP50,
			Source:    "service-a",
		})
	}

	result, err := e.Baseline(MetricLatencyP50, "service-a", "24h", "1h")
	if err != nil {
		t.Fatalf("Baseline failed: %v", err)
	}
	if result.HealthScore > 95 {
		t.Errorf("expected degraded health, got %f", result.HealthScore)
	}
}

func TestListSeries(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.Record(DataPoint{Timestamp: time.Now(), Value: 1, Metric: MetricThroughputIn, Source: "t1"})
	e.Record(DataPoint{Timestamp: time.Now(), Value: 2, Metric: MetricConsumerLag, Source: "g1"})

	series := e.ListSeries()
	if len(series) != 2 {
		t.Fatalf("expected 2 series, got %d", len(series))
	}
}

func TestInvalidWindow(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	_, err := e.Query(MetricThroughputIn, "test", "99h")
	if err == nil {
		t.Fatal("expected error for invalid window")
	}
}

func TestComputeTrend(t *testing.T) {
	t.Parallel()
	points := []DataPoint{
		{Timestamp: time.Now().Add(-10 * time.Minute), Value: 10},
		{Timestamp: time.Now().Add(-5 * time.Minute), Value: 20},
		{Timestamp: time.Now(), Value: 30},
	}
	trend := computeTrend(points)
	if trend.Direction != "increasing" {
		t.Errorf("expected increasing, got %s", trend.Direction)
	}
}
