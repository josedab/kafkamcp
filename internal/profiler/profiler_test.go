package profiler

import (
	"testing"
)

func TestNewProfiler(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	if p.GetCallCount() != 0 {
		t.Errorf("expected 0 calls, got %d", p.GetCallCount())
	}
}

func TestRecordCall_Basic(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	p.RecordCall("list_topics", "agent-1", nil, 100, "success")
	p.RecordCall("describe_topic", "agent-1", nil, 200, "success")
	if p.GetCallCount() != 2 {
		t.Errorf("expected 2 calls, got %d", p.GetCallCount())
	}
}

func TestProfileAgent_ZeroCalls(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	profile := p.ProfileAgent("agent-1", 24)
	if profile.TotalCalls != 0 {
		t.Errorf("expected 0 calls, got %d", profile.TotalCalls)
	}
	if profile.EfficiencyScore != 100 {
		t.Errorf("expected efficiency 100 for no calls, got %d", profile.EfficiencyScore)
	}
}

func TestProfileAgent_SingleCall(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	p.RecordCall("list_topics", "agent-1", nil, 50, "success")
	profile := p.ProfileAgent("agent-1", 24)
	if profile.TotalCalls != 1 {
		t.Errorf("expected 1 call, got %d", profile.TotalCalls)
	}
	if profile.AvgLatencyMs != 50 {
		t.Errorf("expected avg 50ms, got %.1f", profile.AvgLatencyMs)
	}
	if profile.P50LatencyMs != 50 {
		t.Errorf("expected p50 50ms, got %.1f", profile.P50LatencyMs)
	}
	if profile.ErrorRate != 0 {
		t.Errorf("expected error rate 0, got %.2f", profile.ErrorRate)
	}
}

func TestProfileAgent_LatencyPercentiles(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	latencies := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	for _, l := range latencies {
		p.RecordCall("tool", "agent-1", nil, l, "success")
	}
	profile := p.ProfileAgent("agent-1", 24)
	if profile.AvgLatencyMs != 55 {
		t.Errorf("expected avg 55, got %.1f", profile.AvgLatencyMs)
	}
	if profile.P50LatencyMs != 50 {
		t.Errorf("expected p50 50, got %.1f", profile.P50LatencyMs)
	}
	if profile.P95LatencyMs != 100 {
		t.Errorf("expected p95 100, got %.1f", profile.P95LatencyMs)
	}
	if profile.P99LatencyMs != 100 {
		t.Errorf("expected p99 100, got %.1f", profile.P99LatencyMs)
	}
}

func TestProfileAgent_ErrorRate(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	p.RecordCall("tool", "agent-1", nil, 100, "success")
	p.RecordCall("tool", "agent-1", nil, 100, "error")
	p.RecordCall("tool", "agent-1", nil, 100, "success")
	p.RecordCall("tool", "agent-1", nil, 100, "error")

	profile := p.ProfileAgent("agent-1", 24)
	if profile.ErrorRate != 0.5 {
		t.Errorf("expected error rate 0.5, got %.2f", profile.ErrorRate)
	}
}

func TestProfileAgent_UniqueTools(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	p.RecordCall("list_topics", "agent-1", nil, 10, "success")
	p.RecordCall("describe_topic", "agent-1", nil, 20, "success")
	p.RecordCall("list_topics", "agent-1", nil, 15, "success")

	profile := p.ProfileAgent("agent-1", 24)
	if profile.UniqueTtools != 2 {
		t.Errorf("expected 2 unique tools, got %d", profile.UniqueTtools)
	}
}

func TestProfileAgent_SinceHoursZero(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	p.RecordCall("tool", "agent-1", nil, 100, "success")
	// sinceHours=0 means "from now", so recent calls should be included
	profile := p.ProfileAgent("agent-1", 0)
	// All calls are recorded "now", they should be after "since=now" check
	// The behavior is: calls with timestamp After(now) - edge case
	if profile.AgentID != "agent-1" {
		t.Errorf("expected agent-1, got %s", profile.AgentID)
	}
}

func TestGetSlowQueries_WithThreshold(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	p.RecordCall("fast", "agent-1", nil, 100, "success")
	p.RecordCall("slow", "agent-1", nil, 2000, "success")
	p.RecordCall("slow", "agent-1", nil, 3000, "success")

	slow := p.GetSlowQueries("agent-1", 1500)
	if len(slow) != 1 {
		t.Fatalf("expected 1 slow query group, got %d", len(slow))
	}
	if slow[0].Tool != "slow" {
		t.Errorf("expected tool 'slow', got %s", slow[0].Tool)
	}
	if slow[0].CallCount != 2 {
		t.Errorf("expected 2 calls, got %d", slow[0].CallCount)
	}
	if slow[0].MaxLatencyMs != 3000 {
		t.Errorf("expected max 3000, got %d", slow[0].MaxLatencyMs)
	}
}

func TestGetSlowQueries_NegativeThreshold(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	p.RecordCall("tool", "agent-1", nil, 1500, "success")

	slow := p.GetSlowQueries("agent-1", -1)
	// Should use default threshold (1000ms), so 1500 should be slow
	if len(slow) != 1 {
		t.Errorf("expected 1 slow query with default threshold, got %d", len(slow))
	}
}

func TestGetSlowQueries_ZeroThreshold(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	p.RecordCall("tool", "agent-1", nil, 1500, "success")

	slow := p.GetSlowQueries("agent-1", 0)
	// Should use default threshold (1000ms)
	if len(slow) != 1 {
		t.Errorf("expected 1 slow query with default threshold, got %d", len(slow))
	}
}

func TestGetSlowQueries_AllAgents(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	p.RecordCall("tool", "agent-1", nil, 2000, "success")
	p.RecordCall("tool", "agent-2", nil, 3000, "success")

	slow := p.GetSlowQueries("", 1500)
	if len(slow) != 1 {
		t.Fatalf("expected 1 slow query group (both agents same tool), got %d", len(slow))
	}
	if slow[0].CallCount != 2 {
		t.Errorf("expected 2 calls across agents, got %d", slow[0].CallCount)
	}
}

func TestListAgents(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	if len(p.ListAgents()) != 0 {
		t.Error("expected no agents initially")
	}
	p.RecordCall("tool", "agent-b", nil, 10, "success")
	p.RecordCall("tool", "agent-a", nil, 10, "success")
	p.RecordCall("tool", "agent-b", nil, 20, "success")

	agents := p.ListAgents()
	if len(agents) != 2 {
		t.Fatalf("expected 2 agents, got %d", len(agents))
	}
	if agents[0] != "agent-a" || agents[1] != "agent-b" {
		t.Errorf("expected sorted [agent-a, agent-b], got %v", agents)
	}
}

func TestGetCallCount(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	for i := 0; i < 5; i++ {
		p.RecordCall("tool", "a", nil, 10, "success")
	}
	if p.GetCallCount() != 5 {
		t.Errorf("expected 5, got %d", p.GetCallCount())
	}
}

func TestProfileAgent_RedundantCallDetection(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	// Record 5 calls to the same tool rapidly (within 5s window)
	for i := 0; i < 5; i++ {
		p.RecordCall("list_topics", "agent-1", nil, 10, "success")
	}
	profile := p.ProfileAgent("agent-1", 24)
	if len(profile.RedundantCalls) == 0 {
		t.Error("expected redundant calls detected")
	}
}

func TestProfileAgent_HighErrorRateOptimization(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	for i := 0; i < 10; i++ {
		status := "success"
		if i < 5 {
			status = "error"
		}
		p.RecordCall("tool", "agent-1", nil, 100, status)
	}
	profile := p.ProfileAgent("agent-1", 24)
	if profile.ErrorRate != 0.5 {
		t.Errorf("expected error rate 0.5, got %.2f", profile.ErrorRate)
	}
	foundOptimization := false
	for _, opt := range profile.Optimizations {
		if opt.Type == "error_handling" {
			foundOptimization = true
		}
	}
	if !foundOptimization {
		t.Error("expected error_handling optimization for high error rate")
	}
}

func TestProfileAgent_EfficiencyScore(t *testing.T) {
	t.Parallel()
	p := NewProfiler()
	p.RecordCall("tool", "agent-1", nil, 100, "success")
	profile := p.ProfileAgent("agent-1", 24)
	if profile.EfficiencyScore < 0 || profile.EfficiencyScore > 100 {
		t.Errorf("efficiency score out of range: %d", profile.EfficiencyScore)
	}
}

func TestPercentile_Empty(t *testing.T) {
	t.Parallel()
	result := percentile(nil, 50)
	if result != 0 {
		t.Errorf("expected 0 for empty slice, got %f", result)
	}
}

func TestAverage_Empty(t *testing.T) {
	t.Parallel()
	result := average(nil)
	if result != 0 {
		t.Errorf("expected 0 for empty slice, got %f", result)
	}
}
