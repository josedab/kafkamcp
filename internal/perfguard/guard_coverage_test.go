package perfguard

import (
	"log/slog"
	"math"
	"sort"
	"testing"
	"time"
)

// --- GetThrottleStatus ---

func TestGetThrottleStatus_Throttled(t *testing.T) {
	t.Parallel()
	g := NewGuard(DefaultGuardConfig(), slog.Default())

	g.mu.Lock()
	g.throttles["agent-1"] = &ThrottleStatus{
		AgentID:       "agent-1",
		Throttled:     true,
		Reason:        "high latency",
		ThrottledAt:   time.Now(),
		ExpiresAt:     time.Now().Add(5 * time.Minute),
		ThrottledRate: 10,
	}
	g.mu.Unlock()

	ts := g.GetThrottleStatus("agent-1")
	if ts == nil {
		t.Fatal("expected non-nil throttle status")
	}
	if !ts.Throttled {
		t.Error("expected Throttled=true")
	}
	if ts.Reason != "high latency" {
		t.Errorf("expected reason 'high latency', got %q", ts.Reason)
	}
	if ts.ThrottledRate != 10 {
		t.Errorf("expected rate 10, got %d", ts.ThrottledRate)
	}
}

func TestGetThrottleStatus_NotThrottled(t *testing.T) {
	t.Parallel()
	g := NewGuard(DefaultGuardConfig(), slog.Default())

	ts := g.GetThrottleStatus("agent-1")
	if ts != nil {
		t.Errorf("expected nil for non-throttled agent, got %+v", ts)
	}
}

// --- ResetBaseline ---

func TestResetBaseline_ClearsSamples(t *testing.T) {
	t.Parallel()
	g := NewGuard(GuardConfig{BaselineWindow: 10, DetectionWindow: 5, CUSUMThreshold: 5.0, CUSUMSlack: 0.5, WarningThreshold: 20, CriticalThreshold: 50, MaxRegressions: 100}, slog.Default())

	for i := 0; i < 15; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 10.0})
	}

	baseline := g.GetBaseline("agent-1", MetricProduceLatency)
	if baseline == nil {
		t.Fatal("expected baseline before reset")
	}

	g.ResetBaseline("agent-1", MetricProduceLatency)

	baseline = g.GetBaseline("agent-1", MetricProduceLatency)
	if baseline != nil {
		t.Error("expected nil baseline after reset")
	}
}

func TestResetBaseline_CUSUMResets(t *testing.T) {
	t.Parallel()
	g := NewGuard(GuardConfig{BaselineWindow: 10, DetectionWindow: 5, CUSUMThreshold: 5.0, CUSUMSlack: 0.5, WarningThreshold: 20, CriticalThreshold: 50, MaxRegressions: 100}, slog.Default())

	for i := 0; i < 15; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 10.0})
	}

	// Record some anomalous values to build CUSUM
	for i := 0; i < 5; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 20.0})
	}

	g.ResetBaseline("agent-1", MetricProduceLatency)

	g.mu.RLock()
	am := g.metrics["agent-1:produce_latency_ms"]
	g.mu.RUnlock()
	if am.cusumPos != 0 || am.cusumNeg != 0 {
		t.Errorf("expected CUSUM reset to 0, got pos=%f neg=%f", am.cusumPos, am.cusumNeg)
	}
}

func TestResetBaseline_WhileThrottleActive(t *testing.T) {
	t.Parallel()
	g := NewGuard(GuardConfig{BaselineWindow: 10, DetectionWindow: 5, CUSUMThreshold: 2.0, CUSUMSlack: 0.2, WarningThreshold: 20, CriticalThreshold: 50, AutoThrottle: true, ThrottleDuration: "5m", MaxRegressions: 100}, slog.Default())

	// Build baseline
	for i := 0; i < 10; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 10.0})
	}

	// Trigger throttle
	for i := 0; i < 30; i++ {
		g.RecordSample(Sample{AgentID: "agent-1", Metric: MetricProduceLatency, Value: 100.0})
	}

	if !g.IsThrottled("agent-1") {
		t.Fatal("expected throttle before reset")
	}

	// Reset baseline - throttle should still be active
	g.ResetBaseline("agent-1", MetricProduceLatency)

	if !g.IsThrottled("agent-1") {
		t.Error("expected throttle to remain active after baseline reset")
	}

	baseline := g.GetBaseline("agent-1", MetricProduceLatency)
	if baseline != nil {
		t.Error("expected nil baseline after reset")
	}
}

func TestResetBaseline_NonExistentMetric(t *testing.T) {
	t.Parallel()
	g := NewGuard(DefaultGuardConfig(), slog.Default())
	// Should not panic
	g.ResetBaseline("agent-1", MetricProduceLatency)
}

// --- percentile ---

func TestPercentile_EmptySlice(t *testing.T) {
	t.Parallel()
	result := percentile([]float64{}, 50)
	if result != 0 {
		t.Errorf("expected 0 for empty slice, got %f", result)
	}
}

func TestPercentile_SingleElement(t *testing.T) {
	t.Parallel()
	result := percentile([]float64{42.0}, 50)
	if result != 42.0 {
		t.Errorf("expected 42.0, got %f", result)
	}

	result = percentile([]float64{42.0}, 99)
	if result != 42.0 {
		t.Errorf("expected 42.0 for p99 single element, got %f", result)
	}
}

func TestPercentile_EvenCount(t *testing.T) {
	t.Parallel()
	vals := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	sort.Float64s(vals)

	p50 := percentile(vals, 50)
	if p50 != 5 {
		t.Errorf("p50 = %f, want 5", p50)
	}
}

func TestPercentile_OddCount(t *testing.T) {
	t.Parallel()
	vals := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	sort.Float64s(vals)

	p50 := percentile(vals, 50)
	if p50 < 4 || p50 > 5 {
		t.Errorf("p50 = %f, expected ~4-5", p50)
	}
}

func TestPercentile_KnownDistribution(t *testing.T) {
	t.Parallel()
	// Create 100 values from 1 to 100
	vals := make([]float64, 100)
	for i := range vals {
		vals[i] = float64(i + 1)
	}
	sort.Float64s(vals)

	p50 := percentile(vals, 50)
	if p50 != 50 {
		t.Errorf("p50 = %f, want 50", p50)
	}

	p95 := percentile(vals, 95)
	if p95 != 95 {
		t.Errorf("p95 = %f, want 95", p95)
	}

	p99 := percentile(vals, 99)
	if p99 != 99 {
		t.Errorf("p99 = %f, want 99", p99)
	}
}

func TestPercentile_IdenticalValues(t *testing.T) {
	t.Parallel()
	vals := []float64{5.0, 5.0, 5.0, 5.0, 5.0}
	sort.Float64s(vals)

	for _, p := range []float64{50, 95, 99} {
		result := percentile(vals, p)
		if result != 5.0 {
			t.Errorf("p%.0f = %f, want 5.0 for identical values", p, result)
		}
	}
}

// --- ListRegressions improvements ---

func TestListRegressions_EmptyAgent(t *testing.T) {
	t.Parallel()
	g := NewGuard(GuardConfig{BaselineWindow: 10, DetectionWindow: 5, CUSUMThreshold: 2.0, CUSUMSlack: 0.2, WarningThreshold: 20, CriticalThreshold: 50, MaxRegressions: 100}, slog.Default())

	// Build baseline and trigger regression for two agents
	for _, agent := range []string{"agent-1", "agent-2"} {
		for i := 0; i < 10; i++ {
			g.RecordSample(Sample{AgentID: agent, Metric: MetricProduceLatency, Value: 10.0})
		}
		for i := 0; i < 30; i++ {
			g.RecordSample(Sample{AgentID: agent, Metric: MetricProduceLatency, Value: 100.0})
		}
	}

	// Empty agent should return all
	all := g.ListRegressions("", 100)
	if len(all) == 0 {
		t.Error("expected regressions for empty agent filter")
	}

	agent1 := g.ListRegressions("agent-1", 100)
	for _, r := range agent1 {
		if r.AgentID != "agent-1" {
			t.Errorf("expected agent-1 only, got %s", r.AgentID)
		}
	}
}

func TestListRegressions_DefaultLimit(t *testing.T) {
	t.Parallel()
	g := NewGuard(DefaultGuardConfig(), slog.Default())
	regs := g.ListRegressions("", 0)
	// Empty guard returns empty (possibly nil) slice
	if len(regs) != 0 {
		t.Errorf("expected 0 regressions, got %d", len(regs))
	}
}

// --- computeMean / computeStdDev ---

func TestComputeMean_Empty(t *testing.T) {
	t.Parallel()
	if m := computeMean(nil); m != 0 {
		t.Errorf("expected 0, got %f", m)
	}
}

func TestComputeStdDev_Single(t *testing.T) {
	t.Parallel()
	sd := computeStdDev([]float64{5.0}, 5.0)
	if sd != 0 {
		t.Errorf("expected 0 stddev for single value, got %f", sd)
	}
}

func TestComputeStdDev_Known(t *testing.T) {
	t.Parallel()
	vals := []float64{2, 4, 4, 4, 5, 5, 7, 9}
	mean := computeMean(vals)
	sd := computeStdDev(vals, mean)
	if math.Abs(sd-2.0) > 0.2 {
		t.Errorf("expected stddev ~2.0, got %f", sd)
	}
}
