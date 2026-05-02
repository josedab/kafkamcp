package selfheal

import "testing"

// Additional tests for edge cases in computeHealthScore.

func TestComputeHealthScore_AllPenaltiesApplied(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	// lag > 1M (-50), commitRate=0 (-30), rebalances>10 (-25), members=0 (-40)
	// total: 100 - 50 - 30 - 25 - 40 = -45 → clamped to 0
	h := m.EvaluateHealth("worst-group", 2_000_000, 0, 0, 15)
	if h.HealthScore != 0 {
		t.Errorf("expected health score=0 (all penalties), got %d", h.HealthScore)
	}
	if h.Status != "dead" {
		t.Errorf("expected dead status, got %s", h.Status)
	}
}

func TestComputeHealthScore_ModerateLag(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	// lag=150_000 (-30), commitRate=2.0 (0), rebalances=3 (-5), members=2 (0)
	// 100 - 30 - 5 = 65
	h := m.EvaluateHealth("moderate", 150_000, 2, 2.0, 3)
	if h.HealthScore != 65 {
		t.Errorf("expected health score=65, got %d", h.HealthScore)
	}
	if h.Status != "degraded" {
		t.Errorf("expected degraded (65<76), got %s", h.Status)
	}
}

func TestComputeHealthScore_LowCommitRate(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	// lag=0 (0), commitRate=0.5 (-15), rebalances=0 (0), members=1 (0)
	// 100 - 15 = 85
	h := m.EvaluateHealth("low-commit", 0, 1, 0.5, 0)
	if h.HealthScore != 85 {
		t.Errorf("expected health score=85, got %d", h.HealthScore)
	}
	if h.Status != "healthy" {
		t.Errorf("expected healthy (85>=76), got %s", h.Status)
	}
}

func TestComputeHealthScore_SmallLag(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	// lag=15_000 (-15), commitRate=5.0 (0), rebalances=6 (-15), members=3 (0)
	// 100 - 15 - 15 = 70
	h := m.EvaluateHealth("small-lag", 15_000, 3, 5.0, 6)
	if h.HealthScore != 70 {
		t.Errorf("expected health score=70, got %d", h.HealthScore)
	}
	if h.Status != "degraded" {
		t.Errorf("expected degraded, got %s", h.Status)
	}
}

func TestEvaluateHealth_DeadStatus(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	// score < 25 → dead
	h := m.EvaluateHealth("dead-group", 2_000_000, 0, 0, 20)
	if h.Status != "dead" {
		t.Errorf("expected dead, got %s", h.Status)
	}
}

func TestLagTrend_Growing(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	m.EvaluateHealth("trend-test", 1000, 1, 1.0, 0)
	h := m.EvaluateHealth("trend-test", 2000, 1, 1.0, 0)
	if h.Metrics.LagTrend != "growing" {
		t.Errorf("expected growing trend, got %s", h.Metrics.LagTrend)
	}
}

func TestLagTrend_Shrinking(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	m.EvaluateHealth("trend-test2", 5000, 1, 1.0, 0)
	h := m.EvaluateHealth("trend-test2", 1000, 1, 1.0, 0)
	if h.Metrics.LagTrend != "shrinking" {
		t.Errorf("expected shrinking trend, got %s", h.Metrics.LagTrend)
	}
}

func TestLagTrend_Stable(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	m.EvaluateHealth("trend-test3", 1000, 1, 1.0, 0)
	h := m.EvaluateHealth("trend-test3", 1000, 1, 1.0, 0)
	if h.Metrics.LagTrend != "stable" {
		t.Errorf("expected stable trend, got %s", h.Metrics.LagTrend)
	}
}

func TestDetectIssues_NoIssuesWhenHealthy(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	h := m.EvaluateHealth("healthy-group", 100, 3, 5.0, 0)
	if len(h.Issues) != 0 {
		t.Errorf("expected 0 issues for healthy group, got %d", len(h.Issues))
	}
}

func TestDetectIssues_WarningRebalance(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	h := m.EvaluateHealth("rebalance-warn", 0, 3, 5.0, 7)
	found := false
	for _, issue := range h.Issues {
		if issue.Type == "rebalance_storm" && issue.Severity == "warning" {
			found = true
		}
	}
	if !found {
		t.Error("expected warning-level rebalance_storm for 7 rebalances")
	}
}
