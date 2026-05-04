package perfguard

import (
	"log/slog"
	"testing"
	"time"
)

func TestBaselineComputation(t *testing.T) {
	g := NewGuard(GuardConfig{BaselineWindow: 10, DetectionWindow: 5, CUSUMThreshold: 5.0, CUSUMSlack: 0.5, WarningThreshold: 20, CriticalThreshold: 50, MaxRegressions: 100}, slog.Default())

	// Add baseline samples
	for i := 0; i < 10; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 10.0 + float64(i%3)})
	}

	baseline := g.GetBaseline("agent-1", MetricProduceLatency)
	if baseline == nil {
		t.Fatal("expected baseline after 10 samples")
	}
	if baseline.SampleCount != 10 {
		t.Errorf("SampleCount = %d, want 10", baseline.SampleCount)
	}
	if baseline.Mean < 9 || baseline.Mean > 12 {
		t.Errorf("Mean = %.2f, expected ~10-11", baseline.Mean)
	}
}

func TestRegressionDetection(t *testing.T) {
	g := NewGuard(GuardConfig{BaselineWindow: 10, DetectionWindow: 5, CUSUMThreshold: 2.0, CUSUMSlack: 0.2, WarningThreshold: 20, CriticalThreshold: 50, AutoThrottle: true, ThrottleDuration: "5m", MaxRegressions: 100}, slog.Default())

	// Build stable baseline (zero variance)
	for i := 0; i < 10; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 10.0})
	}

	// Introduce regression (10x latency spike)
	var regression *Regression
	for i := 0; i < 30; i++ {
		r := g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 100.0})
		if r != nil {
			regression = r
		}
	}

	if regression == nil {
		t.Fatal("expected regression detection for 5x latency spike")
	}
	if regression.Severity != "critical" {
		t.Errorf("Severity = %q, want critical", regression.Severity)
	}
	if regression.ChangePercent < 100 {
		t.Errorf("ChangePercent = %.1f, expected > 100", regression.ChangePercent)
	}
}

func TestAutoThrottle(t *testing.T) {
	g := NewGuard(GuardConfig{BaselineWindow: 10, DetectionWindow: 5, CUSUMThreshold: 2.0, CUSUMSlack: 0.2, WarningThreshold: 20, CriticalThreshold: 50, AutoThrottle: true, ThrottleDuration: "5m", MaxRegressions: 100}, slog.Default())

	// Build stable baseline
	for i := 0; i < 10; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 10.0})
	}

	// Trigger critical regression with large spike
	for i := 0; i < 30; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 100.0})
	}

	if !g.IsThrottled("agent-1") {
		t.Error("expected agent-1 to be throttled")
	}

	// Unthrottle
	if err := g.UnthrottleAgent("agent-1"); err != nil {
		t.Fatalf("UnthrottleAgent: %v", err)
	}
	if g.IsThrottled("agent-1") {
		t.Error("expected agent-1 to be unthrottled")
	}
}

func TestNoRegressionWithinNorms(t *testing.T) {
	g := NewGuard(DefaultGuardConfig(), slog.Default())

	// Build baseline and keep values in normal range
	for i := 0; i < 200; i++ {
		r := g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 10.0 + float64(i%2)})
		if r != nil {
			t.Errorf("unexpected regression at sample %d", i)
		}
	}
}

func TestListRegressions(t *testing.T) {
	g := NewGuard(GuardConfig{BaselineWindow: 10, DetectionWindow: 5, CUSUMThreshold: 2.0, CUSUMSlack: 0.2, WarningThreshold: 20, CriticalThreshold: 50, MaxRegressions: 100}, slog.Default())

	for i := 0; i < 10; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 10.0})
	}
	for i := 0; i < 30; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 100.0})
	}

	regs := g.ListRegressions("agent-1", 10)
	if len(regs) == 0 {
		t.Error("expected at least one regression")
	}
}

func TestThrottleExpiry(t *testing.T) {
	g := NewGuard(DefaultGuardConfig(), slog.Default())

	g.mu.Lock()
	g.throttles["agent-1"] = &ThrottleStatus{
		AgentID:   "agent-1",
		Throttled: true,
		ExpiresAt: time.Now().Add(-1 * time.Minute), // Already expired
	}
	g.mu.Unlock()

	if g.IsThrottled("agent-1") {
		t.Error("expired throttle should not be active")
	}
}

func TestStats(t *testing.T) {
	g := NewGuard(DefaultGuardConfig(), slog.Default())
	g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 10.0})
	stats := g.Stats()
	if stats["tracked_metrics"].(int) != 1 {
		t.Errorf("tracked_metrics = %v, want 1", stats["tracked_metrics"])
	}
}

func TestComputeMean(t *testing.T) {
	if m := computeMean([]float64{1, 2, 3, 4, 5}); m != 3.0 {
		t.Errorf("computeMean = %f, want 3.0", m)
	}
}

func TestPercentile(t *testing.T) {
	vals := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	p50 := percentile(vals, 50)
	if p50 != 5 {
		t.Errorf("p50 = %f, want 5", p50)
	}
}
