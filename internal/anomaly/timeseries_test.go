package anomaly

import (
	"math"
	"testing"
	"time"
)

func TestTimeSeriesAnalyzerAnalyze(t *testing.T) {
	now := time.Now()

	t.Run("enough_data", func(t *testing.T) {
		a := NewTimeSeriesAnalyzer()
		for i := 0; i < 20; i++ {
			a.RecordPoint(MetricConsumerLag, "src", float64(i), now.Add(time.Duration(i)*time.Second))
		}
		result, err := a.Analyze(MetricConsumerLag, "src", now.Add(-time.Minute))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.DataPoints != 20 {
			t.Errorf("expected 20 data points, got %d", result.DataPoints)
		}
		if result.Metric != "consumer_lag" {
			t.Errorf("expected metric consumer_lag, got %s", result.Metric)
		}
		if result.Source != "src" {
			t.Errorf("expected source src, got %s", result.Source)
		}
		if len(result.ChartData) != 20 {
			t.Errorf("expected 20 chart points, got %d", len(result.ChartData))
		}
	})

	t.Run("insufficient_data", func(t *testing.T) {
		a := NewTimeSeriesAnalyzer()
		a.RecordPoint(MetricConsumerLag, "src", 1.0, now)
		a.RecordPoint(MetricConsumerLag, "src", 2.0, now.Add(time.Second))
		_, err := a.Analyze(MetricConsumerLag, "src", now.Add(-time.Minute))
		if err == nil {
			t.Fatal("expected error for insufficient data")
		}
	})

	t.Run("no_data", func(t *testing.T) {
		a := NewTimeSeriesAnalyzer()
		_, err := a.Analyze(MetricConsumerLag, "missing", now.Add(-time.Minute))
		if err == nil {
			t.Fatal("expected error for missing metric")
		}
	})
}

func TestMannKendallTest(t *testing.T) {
	t.Run("increasing_trend", func(t *testing.T) {
		values := make([]float64, 30)
		for i := range values {
			values[i] = float64(i) * 2
		}
		result := mannKendallTest(values)
		if result.Direction != "increasing" {
			t.Errorf("expected increasing, got %s", result.Direction)
		}
		if !result.Significant {
			t.Error("expected significant trend")
		}
		if result.Slope <= 0 {
			t.Errorf("expected positive slope, got %f", result.Slope)
		}
	})

	t.Run("decreasing_trend", func(t *testing.T) {
		values := make([]float64, 30)
		for i := range values {
			values[i] = 100 - float64(i)*2
		}
		result := mannKendallTest(values)
		if result.Direction != "decreasing" {
			t.Errorf("expected decreasing, got %s", result.Direction)
		}
		if !result.Significant {
			t.Error("expected significant trend")
		}
		if result.Slope >= 0 {
			t.Errorf("expected negative slope, got %f", result.Slope)
		}
	})

	t.Run("no_trend", func(t *testing.T) {
		values := []float64{1, 2, 1, 2, 1, 2, 1, 2}
		result := mannKendallTest(values)
		if result.Direction != "no_trend" {
			t.Errorf("expected no_trend, got %s", result.Direction)
		}
	})

	t.Run("too_few_points", func(t *testing.T) {
		result := mannKendallTest([]float64{1, 2, 3})
		if result.Direction != "no_trend" {
			t.Errorf("expected no_trend for <4 points, got %s", result.Direction)
		}
		if result.Significant {
			t.Error("should not be significant with <4 points")
		}
	})
}

func TestDetectSeasonalityFFT(t *testing.T) {
	t.Run("seasonal_data", func(t *testing.T) {
		n := 64
		values := make([]float64, n)
		for i := range values {
			values[i] = math.Sin(2*math.Pi*float64(i)/16.0) * 10
		}
		result := detectSeasonalityFFT(values)
		if !result.Detected {
			t.Error("expected seasonality to be detected")
		}
		if result.Amplitude <= 0 {
			t.Error("expected positive amplitude")
		}
		if result.Confidence <= 0.3 {
			t.Errorf("expected confidence > 0.3, got %f", result.Confidence)
		}
	})

	t.Run("non_seasonal", func(t *testing.T) {
		// Single impulse produces flat spectrum — no dominant frequency
		values := make([]float64, 32)
		values[0] = 100.0
		result := detectSeasonalityFFT(values)
		if result.Detected {
			t.Error("did not expect seasonality in impulse data")
		}
	})

	t.Run("too_few_points", func(t *testing.T) {
		result := detectSeasonalityFFT([]float64{1, 2, 3, 4, 5})
		if result.Detected {
			t.Error("should not detect seasonality with <8 points")
		}
	})
}

func TestPearsonCorrelation(t *testing.T) {
	t.Run("perfect_positive", func(t *testing.T) {
		x := []float64{1, 2, 3, 4, 5}
		y := []float64{2, 4, 6, 8, 10}
		r := pearsonCorrelation(x, y)
		if math.Abs(r-1.0) > 0.001 {
			t.Errorf("expected ~1.0, got %f", r)
		}
	})

	t.Run("perfect_negative", func(t *testing.T) {
		x := []float64{1, 2, 3, 4, 5}
		y := []float64{10, 8, 6, 4, 2}
		r := pearsonCorrelation(x, y)
		if math.Abs(r-(-1.0)) > 0.001 {
			t.Errorf("expected ~-1.0, got %f", r)
		}
	})

	t.Run("no_correlation", func(t *testing.T) {
		x := []float64{1, 2, 3, 4, 5}
		y := []float64{5, 5, 5, 5, 5}
		r := pearsonCorrelation(x, y)
		if math.Abs(r) > 0.001 {
			t.Errorf("expected ~0, got %f", r)
		}
	})

	t.Run("single_point", func(t *testing.T) {
		r := pearsonCorrelation([]float64{1}, []float64{2})
		if r != 0 {
			t.Errorf("expected 0 for single point, got %f", r)
		}
	})
}

func TestSpearmanCorrelation(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		x := []float64{1, 2, 3, 4, 5}
		y := []float64{2, 4, 6, 8, 10}
		r := spearmanCorrelation(x, y)
		if math.Abs(r-1.0) > 0.001 {
			t.Errorf("expected ~1.0, got %f", r)
		}
	})
}

func TestComputeStats(t *testing.T) {
	t.Run("normal_values", func(t *testing.T) {
		values := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		stats := computeStats(values)
		if stats.Count != 10 {
			t.Errorf("expected count 10, got %d", stats.Count)
		}
		if math.Abs(stats.Mean-5.5) > 0.01 {
			t.Errorf("expected mean 5.5, got %f", stats.Mean)
		}
		if stats.Min != 1 {
			t.Errorf("expected min 1, got %f", stats.Min)
		}
		if stats.Max != 10 {
			t.Errorf("expected max 10, got %f", stats.Max)
		}
		if stats.P95 <= stats.Mean {
			t.Error("p95 should be above mean")
		}
		if stats.P99 <= stats.P95 {
			t.Error("p99 should be >= p95")
		}
	})

	t.Run("single_value", func(t *testing.T) {
		stats := computeStats([]float64{42})
		if stats.Count != 1 {
			t.Errorf("expected count 1, got %d", stats.Count)
		}
		if stats.Mean != 42 {
			t.Errorf("expected mean 42, got %f", stats.Mean)
		}
		if stats.Min != 42 || stats.Max != 42 {
			t.Error("min and max should equal the single value")
		}
	})
}

func TestCorrelateMetrics(t *testing.T) {
	now := time.Now()

	t.Run("valid_correlation", func(t *testing.T) {
		a := NewTimeSeriesAnalyzer()
		for i := 0; i < 10; i++ {
			ts := now.Add(time.Duration(i) * time.Second)
			a.RecordPoint(MetricConsumerLag, "s1", float64(i), ts)
			a.RecordPoint(MetricThroughput, "s2", float64(i)*2, ts)
		}
		corr, err := a.CorrelateMetrics(MetricConsumerLag, "s1", MetricThroughput, "s2", now.Add(-time.Minute))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if math.Abs(corr.Pearson-1.0) > 0.001 {
			t.Errorf("expected pearson ~1.0, got %f", corr.Pearson)
		}
		if corr.Strength != "strong" {
			t.Errorf("expected strong correlation, got %s", corr.Strength)
		}
		if corr.Direction != "positive" {
			t.Errorf("expected positive direction, got %s", corr.Direction)
		}
	})

	t.Run("missing_data", func(t *testing.T) {
		a := NewTimeSeriesAnalyzer()
		a.RecordPoint(MetricConsumerLag, "s1", 1.0, now)
		_, err := a.CorrelateMetrics(MetricConsumerLag, "s1", MetricThroughput, "missing", now.Add(-time.Minute))
		if err == nil {
			t.Fatal("expected error for missing metric")
		}
	})
}

// --- findOutliers tests ---

func TestFindOutliersNoOutliers(t *testing.T) {
	// Normal distribution, no outliers
	values := []float64{10, 11, 10.5, 11.2, 10.8, 10.3, 11.1, 10.7}
	points := makeDataPoints(values)
	outliers := findOutliers(points, values)
	if len(outliers) != 0 {
		t.Errorf("expected 0 outliers for normal distribution, got %d", len(outliers))
	}
}

func TestFindOutliersClearOutlier(t *testing.T) {
	values := []float64{10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 100}
	points := makeDataPoints(values)
	outliers := findOutliers(points, values)
	if len(outliers) == 0 {
		t.Error("expected at least 1 outlier")
	}
	for _, o := range outliers {
		if o.Value == 100 {
			if o.Type != "spike" {
				t.Errorf("expected spike type, got %s", o.Type)
			}
		}
	}
}

func TestFindOutliersDropOutlier(t *testing.T) {
	values := []float64{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 0}
	points := makeDataPoints(values)
	outliers := findOutliers(points, values)
	found := false
	for _, o := range outliers {
		if o.Value == 0 {
			found = true
			if o.Type != "drop" {
				t.Errorf("expected drop type, got %s", o.Type)
			}
		}
	}
	if !found {
		t.Error("expected drop outlier for value 0")
	}
}

func TestFindOutliersAllSameValues(t *testing.T) {
	values := []float64{5, 5, 5, 5, 5}
	points := makeDataPoints(values)
	outliers := findOutliers(points, values)
	if outliers != nil && len(outliers) > 0 {
		t.Errorf("expected no outliers for constant values, got %d", len(outliers))
	}
}

// --- computeStats tests ---

func TestComputeStatsEmpty(t *testing.T) {
	stats := computeStats([]float64{})
	if stats.Count != 0 {
		t.Errorf("expected count 0 for empty, got %d", stats.Count)
	}
}

func TestComputeStatsNegativeValues(t *testing.T) {
	values := []float64{-10, -5, 0, 5, 10}
	stats := computeStats(values)
	if stats.Count != 5 {
		t.Errorf("expected count 5, got %d", stats.Count)
	}
	if math.Abs(stats.Mean-0) > 0.01 {
		t.Errorf("expected mean 0, got %f", stats.Mean)
	}
	if stats.Min != -10 {
		t.Errorf("expected min -10, got %f", stats.Min)
	}
	if stats.Max != 10 {
		t.Errorf("expected max 10, got %f", stats.Max)
	}
}

func TestComputeStatsSingleValue(t *testing.T) {
	stats := computeStats([]float64{42})
	if stats.StdDev != 0 {
		t.Errorf("expected stddev 0 for single value, got %f", stats.StdDev)
	}
	if stats.Median != 42 {
		t.Errorf("expected median 42, got %f", stats.Median)
	}
}

func TestComputeStatsTwoValues(t *testing.T) {
	stats := computeStats([]float64{0, 100})
	if stats.Count != 2 {
		t.Errorf("expected count 2, got %d", stats.Count)
	}
	if math.Abs(stats.Mean-50) > 0.01 {
		t.Errorf("expected mean 50, got %f", stats.Mean)
	}
}

// --- spearmanCorrelation tests ---

func TestSpearmanPerfectNegative(t *testing.T) {
	x := []float64{1, 2, 3, 4, 5}
	y := []float64{10, 8, 6, 4, 2}
	r := spearmanCorrelation(x, y)
	if math.Abs(r-(-1.0)) > 0.001 {
		t.Errorf("expected spearman ~-1.0, got %f", r)
	}
}

func TestSpearmanTies(t *testing.T) {
	x := []float64{1, 2, 2, 4, 5}
	y := []float64{5, 4, 3, 2, 1}
	r := spearmanCorrelation(x, y)
	if r >= 0 {
		t.Errorf("expected negative spearman for inverse with ties, got %f", r)
	}
}

func TestSpearmanSinglePoint(t *testing.T) {
	r := spearmanCorrelation([]float64{1}, []float64{2})
	if r != 0 {
		t.Errorf("expected 0 for single point, got %f", r)
	}
}

func TestSpearmanIdenticalSeries(t *testing.T) {
	x := []float64{5, 5, 5, 5}
	y := []float64{5, 5, 5, 5}
	r := spearmanCorrelation(x, y)
	// All same values means ranks are all tied; correlation should be 0
	if math.IsNaN(r) {
		t.Error("expected non-NaN for identical constant series")
	}
}

// --- Correlation strength/direction ---

func TestCorrelateModerate(t *testing.T) {
	now := time.Now()
	a := NewTimeSeriesAnalyzer()
	vals := []float64{1, 3, 2, 5, 4, 6, 5, 8}
	for i, v := range vals {
		ts := now.Add(time.Duration(i) * time.Second)
		a.RecordPoint(MetricConsumerLag, "s1", v, ts)
		a.RecordPoint(MetricThroughput, "s2", v*1.5+float64(i%3), ts)
	}
	corr, err := a.CorrelateMetrics(MetricConsumerLag, "s1", MetricThroughput, "s2", now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if corr.Strength == "none" {
		t.Error("expected at least weak correlation")
	}
}

func TestCorrelateNegative(t *testing.T) {
	now := time.Now()
	a := NewTimeSeriesAnalyzer()
	for i := 0; i < 10; i++ {
		ts := now.Add(time.Duration(i) * time.Second)
		a.RecordPoint(MetricConsumerLag, "s1", float64(i), ts)
		a.RecordPoint(MetricThroughput, "s2", float64(10-i), ts)
	}
	corr, err := a.CorrelateMetrics(MetricConsumerLag, "s1", MetricThroughput, "s2", now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if corr.Direction != "negative" {
		t.Errorf("expected negative direction, got %s", corr.Direction)
	}
}

func TestCorrelateInsufficientData(t *testing.T) {
	now := time.Now()
	a := NewTimeSeriesAnalyzer()
	a.RecordPoint(MetricConsumerLag, "s1", 1.0, now)
	a.RecordPoint(MetricThroughput, "s2", 2.0, now)
	_, err := a.CorrelateMetrics(MetricConsumerLag, "s1", MetricThroughput, "s2", now.Add(-time.Minute))
	if err == nil {
		t.Fatal("expected error for insufficient data")
	}
}

// --- Pearson additional tests ---

func TestPearsonMismatchedLengths(t *testing.T) {
	r := pearsonCorrelation([]float64{1, 2, 3}, []float64{1, 2})
	if r != 0 {
		t.Errorf("expected 0 for mismatched lengths, got %f", r)
	}
}

func TestPearsonConstantValues(t *testing.T) {
	x := []float64{5, 5, 5, 5, 5}
	y := []float64{1, 2, 3, 4, 5}
	r := pearsonCorrelation(x, y)
	if r != 0 {
		t.Errorf("expected 0 for constant x, got %f", r)
	}
}

// --- Ranks test ---

func TestRanks(t *testing.T) {
	r := ranks([]float64{30, 10, 20})
	if r[0] != 3 || r[1] != 1 || r[2] != 2 {
		t.Errorf("expected [3,1,2], got %v", r)
	}
}

// Helper to create DataPoints from values
func makeDataPoints(values []float64) []DataPoint {
	now := time.Now()
	points := make([]DataPoint, len(values))
	for i, v := range values {
		points[i] = DataPoint{
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Value:     v,
			Metric:    MetricConsumerLag,
			Source:    "test",
		}
	}
	return points
}
