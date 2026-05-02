package selfheal

import (
	"testing"
)

func TestNewMonitor(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	if m == nil {
		t.Fatal("expected non-nil monitor")
	}
	if len(m.ListPolicies()) != 0 {
		t.Fatal("expected no policies")
	}
}

func TestAddPolicy(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	m.AddPolicy(Policy{GroupPattern: "test-*", MaxLag: 1000, AutoRemediate: true, Actions: []string{"restart_consumer"}})
	policies := m.ListPolicies()
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].GroupPattern != "test-*" {
		t.Fatalf("expected pattern test-*, got %s", policies[0].GroupPattern)
	}
}

func TestEvaluateHealth_Healthy(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("group-a", 100, 3, 10.0, 0)
	if gh.Status != "healthy" {
		t.Fatalf("expected healthy, got %s", gh.Status)
	}
	if gh.HealthScore < 75 {
		t.Fatalf("expected score >= 75, got %d", gh.HealthScore)
	}
	if gh.GroupID != "group-a" {
		t.Fatalf("expected group-a, got %s", gh.GroupID)
	}
	if gh.Timestamp == "" {
		t.Fatal("expected non-empty timestamp")
	}
}

func TestEvaluateHealth_Critical(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("group-b", 2_000_000, 0, 0, 15)
	if gh.Status != "dead" {
		t.Fatalf("expected dead, got %s (score=%d)", gh.Status, gh.HealthScore)
	}
	if gh.HealthScore >= 25 {
		t.Fatalf("expected score < 25, got %d", gh.HealthScore)
	}
}

func TestEvaluateHealth_Degraded(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("group-c", 50_000, 2, 5.0, 6)
	if gh.Status != "degraded" {
		t.Fatalf("expected degraded, got %s (score=%d)", gh.Status, gh.HealthScore)
	}
}

func TestDetectIssues_HighLag(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("lag-group", 500_000, 2, 5.0, 0)
	found := false
	for _, issue := range gh.Issues {
		if issue.Type == "high_lag" {
			found = true
			if issue.Severity != "warning" {
				t.Fatalf("expected warning severity for lag 500k, got %s", issue.Severity)
			}
		}
	}
	if !found {
		t.Fatal("expected high_lag issue")
	}
}

func TestDetectIssues_CriticalHighLag(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("lag-group2", 2_000_000, 2, 5.0, 0)
	for _, issue := range gh.Issues {
		if issue.Type == "high_lag" {
			if issue.Severity != "critical" {
				t.Fatalf("expected critical severity for lag 2M, got %s", issue.Severity)
			}
			return
		}
	}
	t.Fatal("expected high_lag issue")
}

func TestDetectIssues_NoCommits(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("nc-group", 5000, 2, 0, 0)
	found := false
	for _, issue := range gh.Issues {
		if issue.Type == "no_commits" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected no_commits issue")
	}
}

func TestDetectIssues_RebalanceStorm(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("rb-group", 100, 3, 10.0, 12)
	found := false
	for _, issue := range gh.Issues {
		if issue.Type == "rebalance_storm" && issue.Severity == "critical" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected critical rebalance_storm issue")
	}
}

func TestDetectIssues_ZombieConsumer(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("zombie-group", 100, 0, 0, 0)
	found := false
	for _, issue := range gh.Issues {
		if issue.Type == "zombie_consumer" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected zombie_consumer issue")
	}
}

func TestDetectIssues_StuckPartition(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("stuck-group", 20_000, 2, 0.3, 0)
	found := false
	for _, issue := range gh.Issues {
		if issue.Type == "stuck_partition" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected stuck_partition issue")
	}
}

func TestRemediation_RebalanceStorm(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("rem-rb", 100, 3, 10.0, 12)
	if gh.Remediation == nil {
		t.Fatal("expected remediation")
	}
	if gh.Remediation.Action != "restart_consumer" {
		t.Fatalf("expected restart_consumer, got %s", gh.Remediation.Action)
	}
}

func TestRemediation_Zombie(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("rem-zombie", 100, 0, 5.0, 0)
	if gh.Remediation == nil {
		t.Fatal("expected remediation")
	}
	if gh.Remediation.Action != "force_rebalance" {
		t.Fatalf("expected force_rebalance, got %s", gh.Remediation.Action)
	}
}

func TestRemediation_NoCommitsCritical(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("rem-nc", 500_000, 2, 0, 0)
	if gh.Remediation == nil {
		t.Fatal("expected remediation")
	}
	if gh.Remediation.Action != "reset_offsets" {
		t.Fatalf("expected reset_offsets, got %s", gh.Remediation.Action)
	}
}

func TestRemediation_AlertOnly(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("rem-alert", 500_000, 3, 10.0, 0)
	if gh.Remediation == nil {
		t.Fatal("expected remediation")
	}
	if gh.Remediation.Action != "alert_only" {
		t.Fatalf("expected alert_only, got %s", gh.Remediation.Action)
	}
}

func TestRemediation_Nil_WhenHealthy(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	gh := m.EvaluateHealth("healthy-group", 100, 3, 10.0, 0)
	if gh.Remediation != nil {
		t.Fatalf("expected nil remediation for healthy group, got %s", gh.Remediation.Action)
	}
}

func TestGetHealthHistory(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	m.EvaluateHealth("hist-group", 100, 3, 10.0, 0)
	m.EvaluateHealth("hist-group", 200, 3, 10.0, 0)

	history := m.GetHealthHistory("hist-group")
	if len(history) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(history))
	}
}

func TestGetHealthHistory_Empty(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	history := m.GetHealthHistory("unknown")
	if len(history) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(history))
	}
}

func TestHealthHistory_MaxEntries(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	for i := 0; i < 60; i++ {
		m.EvaluateHealth("capped-group", int64(i*100), 3, 10.0, 0)
	}
	history := m.GetHealthHistory("capped-group")
	if len(history) != 50 {
		t.Fatalf("expected 50 entries (max), got %d", len(history))
	}
}

func TestLagTrend(t *testing.T) {
	t.Parallel()
	m := NewMonitor()

	gh1 := m.EvaluateHealth("trend-group", 1000, 3, 10.0, 0)
	if gh1.Metrics.LagTrend != "stable" {
		t.Fatalf("expected stable trend initially, got %s", gh1.Metrics.LagTrend)
	}

	gh2 := m.EvaluateHealth("trend-group", 2000, 3, 10.0, 0)
	if gh2.Metrics.LagTrend != "growing" {
		t.Fatalf("expected growing trend, got %s", gh2.Metrics.LagTrend)
	}

	gh3 := m.EvaluateHealth("trend-group", 500, 3, 10.0, 0)
	if gh3.Metrics.LagTrend != "shrinking" {
		t.Fatalf("expected shrinking trend, got %s", gh3.Metrics.LagTrend)
	}

	gh4 := m.EvaluateHealth("trend-group", 500, 3, 10.0, 0)
	if gh4.Metrics.LagTrend != "stable" {
		t.Fatalf("expected stable trend, got %s", gh4.Metrics.LagTrend)
	}
}

func TestListPolicies_IsCopy(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	m.AddPolicy(Policy{GroupPattern: "a"})
	policies := m.ListPolicies()
	policies[0].GroupPattern = "modified"
	if m.ListPolicies()[0].GroupPattern != "a" {
		t.Fatal("ListPolicies should return a copy")
	}
}

func TestComputeHealthScore_Bounds(t *testing.T) {
	t.Parallel()
	m := NewMonitor()
	score := m.computeHealthScore(10_000_000, 0, 20, 0)
	if score < 0 || score > 100 {
		t.Fatalf("score out of bounds: %d", score)
	}
}
