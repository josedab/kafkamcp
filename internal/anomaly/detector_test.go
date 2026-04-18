package anomaly

import (
	"math"
	"testing"
	"time"
)

func TestMeanStddev(t *testing.T) {
	tests := []struct {
		name     string
		values   []float64
		wantMean float64
		wantStd  float64
	}{
		{"empty", nil, 0, 0},
		{"single", []float64{5}, 5, 0},
		{"uniform", []float64{10, 10, 10}, 10, 0},
		{"varied", []float64{2, 4, 4, 4, 5, 5, 7, 9}, 5, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mean, std := meanStddev(tt.values)
			if math.Abs(mean-tt.wantMean) > 0.01 {
				t.Errorf("mean = %v, want %v", mean, tt.wantMean)
			}
			if math.Abs(std-tt.wantStd) > 0.5 {
				t.Errorf("stddev = %v, want ~%v", std, tt.wantStd)
			}
		})
	}
}

func TestRingBuffer(t *testing.T) {
	rb := newRingBuffer(5)
	now := time.Now()

	for i := 0; i < 8; i++ {
		rb.Add(DataPoint{Timestamp: now.Add(time.Duration(i) * time.Second), Value: float64(i)})
	}

	all := rb.Snapshot(now)
	if len(all) != 5 {
		t.Errorf("expected 5 items, got %d", len(all))
	}

	// Only recent items
	recent := rb.Snapshot(now.Add(5 * time.Second))
	if len(recent) != 3 {
		t.Errorf("expected 3 recent items, got %d", len(recent))
	}
}

func TestDetectAnomaly(t *testing.T) {
	d := NewDetector(DefaultConfig(), nil)

	// Build a baseline of stable values around 100
	baseline := make([]DataPoint, 100)
	for i := range baseline {
		baseline[i] = DataPoint{Value: 100 + float64(i%5), Metric: MetricConsumerLag, Source: "test-topic"}
	}

	// Normal value - no alert
	normal := DataPoint{Value: 103, Metric: MetricConsumerLag, Source: "test-topic", Timestamp: time.Now()}
	alert := d.detectAnomaly(normal, baseline)
	if alert != nil {
		t.Errorf("expected no alert for normal value, got %+v", alert)
	}

	// Extreme value - should trigger alert
	extreme := DataPoint{Value: 500, Metric: MetricConsumerLag, Source: "test-topic", Timestamp: time.Now()}
	alert = d.detectAnomaly(extreme, baseline)
	if alert == nil {
		t.Error("expected alert for extreme value")
	} else {
		if alert.Severity != "critical" {
			t.Errorf("expected critical severity, got %s", alert.Severity)
		}
		if alert.Hypothesis == "" {
			t.Error("expected a hypothesis message")
		}
	}
}

func TestGetAlerts(t *testing.T) {
	d := NewDetector(DefaultConfig(), nil)

	now := time.Now()
	d.addAlert(Alert{ID: "a1", Severity: "warning", DetectedAt: now.Add(-2 * time.Hour)})
	d.addAlert(Alert{ID: "a2", Severity: "critical", DetectedAt: now.Add(-30 * time.Minute)})
	d.addAlert(Alert{ID: "a3", Severity: "warning", DetectedAt: now.Add(-5 * time.Minute)})

	// All alerts
	alerts := d.GetAlerts(time.Time{}, "", "", 0)
	if len(alerts) != 3 {
		t.Errorf("expected 3, got %d", len(alerts))
	}

	// Only critical
	alerts = d.GetAlerts(time.Time{}, "critical", "", 0)
	if len(alerts) != 1 {
		t.Errorf("expected 1 critical, got %d", len(alerts))
	}

	// Recent only
	alerts = d.GetAlerts(now.Add(-1*time.Hour), "", "", 0)
	if len(alerts) != 2 {
		t.Errorf("expected 2 recent, got %d", len(alerts))
	}
}

func TestDetectorStatus(t *testing.T) {
	d := NewDetector(DefaultConfig(), nil)
	status := d.GetStatus()
	if status["active_metrics"].(int) != 0 {
		t.Error("expected 0 active metrics initially")
	}
}

func TestPercentile(t *testing.T) {
	data := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	p50 := percentile(data, 50)
	if math.Abs(p50-5.5) > 0.01 {
		t.Errorf("p50 = %v, want 5.5", p50)
	}
}
