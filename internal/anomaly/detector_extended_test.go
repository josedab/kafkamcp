package anomaly

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestDefaultConfig(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()

	if cfg.SampleIntervalSec != 10 {
		t.Errorf("SampleIntervalSec = %d, want 10", cfg.SampleIntervalSec)
	}
	if cfg.BaselineWindowSec != 3600 {
		t.Errorf("BaselineWindowSec = %d, want 3600", cfg.BaselineWindowSec)
	}
	if cfg.Sensitivity != SensitivityMedium {
		t.Errorf("Sensitivity = %q, want %q", cfg.Sensitivity, SensitivityMedium)
	}
	if cfg.ZScoreThreshold != 3.0 {
		t.Errorf("ZScoreThreshold = %f, want 3.0", cfg.ZScoreThreshold)
	}
	if cfg.IQRMultiplier != 1.5 {
		t.Errorf("IQRMultiplier = %f, want 1.5", cfg.IQRMultiplier)
	}
	if cfg.MaxAlerts != 1000 {
		t.Errorf("MaxAlerts = %d, want 1000", cfg.MaxAlerts)
	}
	if cfg.BufferSize != 4096 {
		t.Errorf("BufferSize = %d, want 4096", cfg.BufferSize)
	}
}

func TestNewDetectorAppliesDefaults(t *testing.T) {
	t.Parallel()
	d := NewDetector(DetectorConfig{}, testLogger())

	if d.config.BufferSize != DefaultConfig().BufferSize {
		t.Errorf("BufferSize = %d, want %d", d.config.BufferSize, DefaultConfig().BufferSize)
	}
	if d.config.MaxAlerts != DefaultConfig().MaxAlerts {
		t.Errorf("MaxAlerts = %d, want %d", d.config.MaxAlerts, DefaultConfig().MaxAlerts)
	}
	if d.config.ZScoreThreshold != DefaultConfig().ZScoreThreshold {
		t.Errorf("ZScoreThreshold = %f, want %f", d.config.ZScoreThreshold, DefaultConfig().ZScoreThreshold)
	}
	if d.config.IQRMultiplier != DefaultConfig().IQRMultiplier {
		t.Errorf("IQRMultiplier = %f, want %f", d.config.IQRMultiplier, DefaultConfig().IQRMultiplier)
	}
	if d.config.SampleIntervalSec != DefaultConfig().SampleIntervalSec {
		t.Errorf("SampleIntervalSec = %d, want %d", d.config.SampleIntervalSec, DefaultConfig().SampleIntervalSec)
	}
	if d.config.BaselineWindowSec != DefaultConfig().BaselineWindowSec {
		t.Errorf("BaselineWindowSec = %d, want %d", d.config.BaselineWindowSec, DefaultConfig().BaselineWindowSec)
	}
	if d.buffers == nil {
		t.Error("buffers map should be initialized")
	}
}

func TestRingBufferNewAndCapacity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		size int
	}{
		{"size_1", 1},
		{"size_10", 10},
		{"size_100", 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := newRingBuffer(tt.size)
			if rb.size != tt.size {
				t.Errorf("size = %d, want %d", rb.size, tt.size)
			}
			if rb.count != 0 {
				t.Errorf("count = %d, want 0", rb.count)
			}
			if rb.head != 0 {
				t.Errorf("head = %d, want 0", rb.head)
			}
			if len(rb.data) != tt.size {
				t.Errorf("data len = %d, want %d", len(rb.data), tt.size)
			}
		})
	}
}

func TestRingBufferAddWrapAround(t *testing.T) {
	t.Parallel()
	rb := newRingBuffer(3)
	now := time.Now()

	// Add exactly capacity items
	for i := 0; i < 3; i++ {
		rb.Add(DataPoint{Timestamp: now.Add(time.Duration(i) * time.Second), Value: float64(i)})
	}
	if rb.count != 3 {
		t.Errorf("count = %d, want 3", rb.count)
	}

	// Add more to trigger wrap-around
	for i := 3; i < 7; i++ {
		rb.Add(DataPoint{Timestamp: now.Add(time.Duration(i) * time.Second), Value: float64(i)})
	}
	if rb.count != 3 {
		t.Errorf("count after wrap = %d, want 3 (capped at capacity)", rb.count)
	}

	snap := rb.Snapshot(now)
	if len(snap) != 3 {
		t.Fatalf("snapshot len = %d, want 3", len(snap))
	}
	// Should contain the last 3 values: 4, 5, 6
	for i, dp := range snap {
		expected := float64(4 + i)
		if dp.Value != expected {
			t.Errorf("snap[%d].Value = %f, want %f", i, dp.Value, expected)
		}
	}
}

func TestRingBufferSnapshotFiltering(t *testing.T) {
	t.Parallel()
	rb := newRingBuffer(10)
	now := time.Now()

	for i := 0; i < 10; i++ {
		rb.Add(DataPoint{Timestamp: now.Add(time.Duration(i) * time.Minute), Value: float64(i)})
	}

	// Snapshot with future time should return nothing
	future := now.Add(1 * time.Hour)
	snap := rb.Snapshot(future)
	if len(snap) != 0 {
		t.Errorf("expected 0 items for future cutoff, got %d", len(snap))
	}

	// Snapshot with time.Time{} should return all
	snap = rb.Snapshot(time.Time{})
	if len(snap) != 10 {
		t.Errorf("expected 10 items for zero time, got %d", len(snap))
	}

	// Snapshot with mid-range cutoff
	mid := now.Add(5 * time.Minute)
	snap = rb.Snapshot(mid)
	if len(snap) != 5 {
		t.Errorf("expected 5 items, got %d", len(snap))
	}
}

func TestRingBufferSingleCapacity(t *testing.T) {
	t.Parallel()
	rb := newRingBuffer(1)
	now := time.Now()

	rb.Add(DataPoint{Timestamp: now, Value: 1.0})
	rb.Add(DataPoint{Timestamp: now.Add(time.Second), Value: 2.0})

	snap := rb.Snapshot(time.Time{})
	if len(snap) != 1 {
		t.Fatalf("expected 1 item, got %d", len(snap))
	}
	if snap[0].Value != 2.0 {
		t.Errorf("expected value 2.0, got %f", snap[0].Value)
	}
}

func TestMeanStddevExtended(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		values   []float64
		wantMean float64
		wantStd  float64
		tolerance float64
	}{
		{"two_values", []float64{0, 10}, 5, 5, 0.01},
		{"negative_values", []float64{-5, -3, -1, 1, 3, 5}, 0, 3.0, 0.5},
		{"identical_values", []float64{42, 42, 42, 42}, 42, 0, 0.01},
		{"large_spread", []float64{1, 1000000}, 500000.5, 499999.5, 0.5},
		{"single_zero", []float64{0}, 0, 0, 0.01},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mean, std := meanStddev(tt.values)
			if math.Abs(mean-tt.wantMean) > tt.tolerance {
				t.Errorf("mean = %v, want %v", mean, tt.wantMean)
			}
			if math.Abs(std-tt.wantStd) > tt.tolerance {
				t.Errorf("stddev = %v, want ~%v", std, tt.wantStd)
			}
		})
	}
}

func TestAddAlertCapAtMax(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.MaxAlerts = 5
	d := NewDetector(cfg, testLogger())

	now := time.Now()
	for i := 0; i < 10; i++ {
		d.addAlert(Alert{
			ID:         fmt.Sprintf("alert-%d", i),
			Severity:   "warning",
			DetectedAt: now.Add(time.Duration(i) * time.Second),
		})
	}

	alerts := d.GetAlerts(time.Time{}, "", "", 0)
	if len(alerts) != 5 {
		t.Errorf("expected 5 alerts (capped), got %d", len(alerts))
	}
	// Should keep the most recent alerts (5-9)
	if alerts[0].ID != "alert-9" {
		t.Errorf("expected most recent alert first, got %s", alerts[0].ID)
	}
}

func TestGenerateMessage(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultConfig(), testLogger())

	tests := []struct {
		name      string
		dp        DataPoint
		mean      float64
		zScore    float64
		wantContains string
	}{
		{
			name:         "consumer_lag_spike",
			dp:           DataPoint{Metric: MetricConsumerLag, Source: "my-group", Value: 500},
			mean:         100,
			zScore:       5.0,
			wantContains: "Consumer lag spike",
		},
		{
			name:         "consumer_lag_drop",
			dp:           DataPoint{Metric: MetricConsumerLag, Source: "my-group", Value: 10},
			mean:         100,
			zScore:       -5.0,
			wantContains: "Consumer lag drop",
		},
		{
			name:         "throughput_spike",
			dp:           DataPoint{Metric: MetricThroughput, Source: "my-topic", Value: 5000},
			mean:         1000,
			zScore:       4.0,
			wantContains: "Throughput spike",
		},
		{
			name:         "throughput_drop",
			dp:           DataPoint{Metric: MetricThroughput, Source: "my-topic", Value: 100},
			mean:         1000,
			zScore:       -4.0,
			wantContains: "Throughput drop",
		},
		{
			name:         "error_rate_spike",
			dp:           DataPoint{Metric: MetricErrorRate, Source: "my-topic", Value: 0.15},
			mean:         0.01,
			zScore:       6.0,
			wantContains: "Error rate spike",
		},
		{
			name:         "error_rate_drop",
			dp:           DataPoint{Metric: MetricErrorRate, Source: "my-topic", Value: 0.001},
			mean:         0.05,
			zScore:       -3.0,
			wantContains: "Error rate drop",
		},
		{
			name:         "unknown_metric",
			dp:           DataPoint{Metric: "custom_metric", Source: "src", Value: 50},
			mean:         10,
			zScore:       5.0,
			wantContains: "Anomaly detected",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := d.generateMessage(tt.dp, tt.mean, tt.zScore)
			if msg == "" {
				t.Error("expected non-empty message")
			}
			if !containsSubstring(msg, tt.wantContains) {
				t.Errorf("message %q does not contain %q", msg, tt.wantContains)
			}
		})
	}
}

func TestGenerateHypothesis(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultConfig(), testLogger())

	tests := []struct {
		name   string
		dp     DataPoint
		mean   float64
		zScore float64
	}{
		{"lag_above", DataPoint{Metric: MetricConsumerLag, Value: 500}, 100, 5.0},
		{"lag_below", DataPoint{Metric: MetricConsumerLag, Value: 10}, 100, -5.0},
		{"throughput_above", DataPoint{Metric: MetricThroughput, Value: 5000}, 1000, 4.0},
		{"throughput_below", DataPoint{Metric: MetricThroughput, Value: 100}, 1000, -4.0},
		{"error_above", DataPoint{Metric: MetricErrorRate, Value: 0.15}, 0.01, 6.0},
		{"error_below", DataPoint{Metric: MetricErrorRate, Value: 0.001}, 0.05, -3.0},
		{"unknown", DataPoint{Metric: "custom", Value: 50}, 10, 5.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := d.generateHypothesis(tt.dp, tt.mean, tt.zScore)
			if h == "" {
				t.Error("expected non-empty hypothesis")
			}
		})
	}
}

func TestIQROutlier(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultConfig(), testLogger())

	data := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	tests := []struct {
		name    string
		value   float64
		wantOut bool
	}{
		{"within_range", 5, false},
		{"at_median", 5.5, false},
		{"extreme_high", 100, true},
		{"extreme_low", -50, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := d.iqrOutlier(tt.value, data)
			if got != tt.wantOut {
				t.Errorf("iqrOutlier(%f) = %v, want %v", tt.value, got, tt.wantOut)
			}
		})
	}
}

func TestIQROutlierZeroIQR(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultConfig(), testLogger())
	data := []float64{5, 5, 5, 5, 5}
	if d.iqrOutlier(100, data) {
		t.Error("expected false when IQR is zero")
	}
}

func TestPercentileExtended(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		data []float64
		p    float64
		want float64
	}{
		{"empty", nil, 50, 0},
		{"single", []float64{42}, 50, 42},
		{"p0", []float64{1, 2, 3, 4, 5}, 0, 1},
		{"p100", []float64{1, 2, 3, 4, 5}, 100, 5},
		{"p25", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 25, 3.25},
		{"p75", []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 75, 7.75},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := percentile(tt.data, tt.p)
			if math.Abs(got-tt.want) > 0.01 {
				t.Errorf("percentile(%v, %f) = %f, want %f", tt.data, tt.p, got, tt.want)
			}
		})
	}
}

func TestIngestPointCreatesBuffer(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultConfig(), testLogger())

	now := time.Now()
	dp := DataPoint{
		Timestamp: now,
		Value:     100,
		Metric:    MetricConsumerLag,
		Source:    "test-topic",
	}
	d.IngestPoint(dp)

	d.buffersMu.RLock()
	defer d.buffersMu.RUnlock()
	key := d.bufferKey(MetricConsumerLag, "test-topic")
	buf, ok := d.buffers[key]
	if !ok {
		t.Fatal("expected buffer to be created")
	}
	if buf.count != 1 {
		t.Errorf("expected 1 item in buffer, got %d", buf.count)
	}
}

func TestIngestPointTriggersAlert(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.BaselineWindowSec = 3600
	d := NewDetector(cfg, testLogger())

	now := time.Now()
	// Build baseline of 20 stable points
	for i := 0; i < 20; i++ {
		d.IngestPoint(DataPoint{
			Timestamp: now.Add(time.Duration(i) * time.Second),
			Value:     100,
			Metric:    MetricConsumerLag,
			Source:    "test-group",
		})
	}

	// Inject anomalous point
	d.IngestPoint(DataPoint{
		Timestamp: now.Add(21 * time.Second),
		Value:     100000,
		Metric:    MetricConsumerLag,
		Source:    "test-group",
	})

	alerts := d.GetAlerts(time.Time{}, "", "", 0)
	if len(alerts) == 0 {
		t.Error("expected at least one alert for extreme value")
	}
}

func TestGetStatusAfterIngestion(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultConfig(), testLogger())

	d.IngestPoint(DataPoint{
		Timestamp: time.Now(),
		Value:     50,
		Metric:    MetricThroughput,
		Source:    "topic-a",
	})

	status := d.GetStatus()
	if status["active_metrics"].(int) != 1 {
		t.Errorf("expected 1 active metric, got %v", status["active_metrics"])
	}
}

func TestDetectAnomalySensitivity(t *testing.T) {
	t.Parallel()
	// Use a baseline with meaningful variance so z-scores are realistic
	baseline := make([]DataPoint, 100)
	for i := range baseline {
		baseline[i] = DataPoint{Value: 100 + float64(i%20), Metric: MetricThroughput, Source: "t"}
	}
	// mean ~ 109.5, stddev ~ 5.77

	tests := []struct {
		name        string
		sensitivity string
		value       float64
		wantAlert   bool
	}{
		// z~2.3 exceeds high threshold (2.0) but not low (4.0)
		{"high_sensitivity_moderate_spike", SensitivityHigh, 123, true},
		// z~2.3 doesn't exceed low threshold (4.0)
		{"low_sensitivity_moderate_spike", SensitivityLow, 123, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Sensitivity = tt.sensitivity
			d := NewDetector(cfg, testLogger())

			dp := DataPoint{Value: tt.value, Metric: MetricThroughput, Source: "t", Timestamp: time.Now()}
			alert := d.detectAnomaly(dp, baseline)
			if tt.wantAlert && alert == nil {
				t.Error("expected alert, got nil")
			}
			if !tt.wantAlert && alert != nil {
				t.Errorf("expected no alert, got %+v", alert)
			}
		})
	}
}

func TestBufferKey(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultConfig(), testLogger())

	key := d.bufferKey(MetricConsumerLag, "my-group")
	if key != "consumer_lag:my-group" {
		t.Errorf("bufferKey = %q, want %q", key, "consumer_lag:my-group")
	}
}

func TestGetAlertsWithLimit(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultConfig(), testLogger())
	now := time.Now()

	for i := 0; i < 10; i++ {
		d.addAlert(Alert{
			ID:         fmt.Sprintf("limit-%d", i),
			Severity:   "warning",
			DetectedAt: now.Add(time.Duration(i) * time.Minute),
		})
	}

	alerts := d.GetAlerts(time.Time{}, "", "", 3)
	if len(alerts) != 3 {
		t.Errorf("expected 3 alerts with limit, got %d", len(alerts))
	}
}

func TestGetAlertsFilterByMetric(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultConfig(), testLogger())
	now := time.Now()

	d.addAlert(Alert{ID: "1", Metric: MetricConsumerLag, Severity: "warning", DetectedAt: now})
	d.addAlert(Alert{ID: "2", Metric: MetricThroughput, Severity: "warning", DetectedAt: now})
	d.addAlert(Alert{ID: "3", Metric: MetricConsumerLag, Severity: "critical", DetectedAt: now})

	alerts := d.GetAlerts(time.Time{}, "", string(MetricConsumerLag), 0)
	if len(alerts) != 2 {
		t.Errorf("expected 2 consumer_lag alerts, got %d", len(alerts))
	}
}

// containsSubstring is a test helper.
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsCheck(s, substr))
}

func containsCheck(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
