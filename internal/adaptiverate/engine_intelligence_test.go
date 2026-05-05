package adaptiverate

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// UpdateClusterHealth
// ---------------------------------------------------------------------------

func TestUpdateClusterHealth_Critical(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	// Create two agents with known base quotas.
	e.mu.Lock()
	p1 := e.getOrCreateProfile("agent-1")
	p1.BaseQuota = 100
	p1.DynamicQuota = 100
	p2 := e.getOrCreateProfile("agent-2")
	p2.BaseQuota = 200
	p2.DynamicQuota = 200
	e.mu.Unlock()

	e.UpdateClusterHealth(ClusterHealth{Status: "critical", CPUUtilization: 95, ErrorRate: 12})

	e.mu.RLock()
	defer e.mu.RUnlock()
	// multiplier 0.5 -> 50 and 100
	if p1.DynamicQuota != 50 {
		t.Errorf("agent-1: expected ~50, got %d", p1.DynamicQuota)
	}
	if p2.DynamicQuota != 100 {
		t.Errorf("agent-2: expected ~100, got %d", p2.DynamicQuota)
	}
}

func TestUpdateClusterHealth_Degraded(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	p.BaseQuota = 100
	p.DynamicQuota = 100
	e.mu.Unlock()

	e.UpdateClusterHealth(ClusterHealth{Status: "degraded", CPUUtilization: 80, ErrorRate: 6})

	e.mu.RLock()
	defer e.mu.RUnlock()
	// multiplier 0.75 -> 75
	if p.DynamicQuota != 75 {
		t.Errorf("expected ~75, got %d", p.DynamicQuota)
	}
}

func TestUpdateClusterHealth_Healthy(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	p.BaseQuota = 100
	p.DynamicQuota = 100
	e.mu.Unlock()

	e.UpdateClusterHealth(ClusterHealth{Status: "healthy", CPUUtilization: 20, ErrorRate: 0.5})

	e.mu.RLock()
	defer e.mu.RUnlock()
	// multiplier 1.2 -> 120
	if p.DynamicQuota != 120 {
		t.Errorf("expected ~120, got %d", p.DynamicQuota)
	}
}

func TestUpdateClusterHealth_AllProfilesAffected(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.mu.Lock()
	for _, id := range []string{"a", "b", "c"} {
		p := e.getOrCreateProfile(id)
		p.BaseQuota = 100
		p.DynamicQuota = 100
	}
	e.mu.Unlock()

	e.UpdateClusterHealth(ClusterHealth{Status: "critical", CPUUtilization: 95})

	e.mu.RLock()
	defer e.mu.RUnlock()
	for id, p := range e.profiles {
		if p.DynamicQuota != 50 {
			t.Errorf("%s: expected 50, got %d", id, p.DynamicQuota)
		}
	}
}

func TestUpdateClusterHealth_SkipsSmallChange(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	p.BaseQuota = 100
	// DynamicQuota already at the multiplier target; healthy with multiplier 1.0
	// means adjustedQuota=100, diff=0 which is not >10% of base -> skip.
	p.DynamicQuota = 100
	e.mu.Unlock()

	e.UpdateClusterHealth(ClusterHealth{Status: "healthy", CPUUtilization: 50, ErrorRate: 3})

	e.mu.RLock()
	defer e.mu.RUnlock()
	if p.DynamicQuota != 100 {
		t.Errorf("expected no change (100), got %d", p.DynamicQuota)
	}
}

// ---------------------------------------------------------------------------
// AnalyzeBehavior / detectPattern
// ---------------------------------------------------------------------------

func TestAnalyzeBehavior_NoAgents(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	patterns := e.AnalyzeBehavior()
	if len(patterns) != 0 {
		t.Errorf("expected 0 patterns, got %d", len(patterns))
	}
}

func TestAnalyzeBehavior_InsufficientData(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	p.Samples = []Sample{
		{Timestamp: time.Now(), Count: 1},
		{Timestamp: time.Now(), Count: 1},
	}
	e.mu.Unlock()

	patterns := e.AnalyzeBehavior()
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d", len(patterns))
	}
	if patterns[0].Pattern != "insufficient_data" {
		t.Errorf("expected insufficient_data, got %s", patterns[0].Pattern)
	}
}

func TestAnalyzeBehavior_DetectedPattern(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	// 10 uniform samples -> low CV -> should be "steady"
	for i := 0; i < 10; i++ {
		p.Samples = append(p.Samples, Sample{Timestamp: now.Add(time.Duration(-i) * time.Second), Count: 1})
	}
	p.AvgRate = 60.0  // 1/sec → 60/min
	p.StdDev = 0.01   // very small -> CV ≈ 0.01
	e.mu.Unlock()

	patterns := e.AnalyzeBehavior()
	if len(patterns) != 1 {
		t.Fatalf("expected 1 pattern, got %d", len(patterns))
	}
	if patterns[0].Pattern != "steady" {
		t.Errorf("expected steady, got %s", patterns[0].Pattern)
	}
}

func TestAnalyzeBehavior_MultipleAgents(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	for _, id := range []string{"a", "b", "c"} {
		p := e.getOrCreateProfile(id)
		p.Samples = []Sample{{Timestamp: now, Count: 1}} // insufficient
	}
	e.mu.Unlock()

	patterns := e.AnalyzeBehavior()
	if len(patterns) != 3 {
		t.Errorf("expected 3 patterns, got %d", len(patterns))
	}
}

func TestDetectPattern_Steady(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	for i := 0; i < 10; i++ {
		p.Samples = append(p.Samples, Sample{Timestamp: now.Add(time.Duration(-i) * time.Second), Count: 1})
	}
	p.AvgRate = 60.0
	p.StdDev = 0.1 // CV = 0.1/(60/60) = 0.1 -> steady
	e.mu.Unlock()

	patterns := e.AnalyzeBehavior()
	if patterns[0].Pattern != "steady" {
		t.Errorf("expected steady, got %s", patterns[0].Pattern)
	}
	if patterns[0].Confidence <= 0 {
		t.Error("expected positive confidence")
	}
}

func TestDetectPattern_Bursty(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	for i := 0; i < 10; i++ {
		p.Samples = append(p.Samples, Sample{Timestamp: now.Add(time.Duration(-i) * time.Second), Count: 1})
	}
	p.AvgRate = 60.0
	p.StdDev = 3.0 // CV = 3.0/1.0 = 3.0 -> bursty
	p.PeakRate = 120.0
	e.mu.Unlock()

	patterns := e.AnalyzeBehavior()
	if patterns[0].Pattern != "bursty" {
		t.Errorf("expected bursty, got %s", patterns[0].Pattern)
	}
}

func TestDetectPattern_Growing(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	// First half: low counts, second half: high counts -> ratio > 1.5
	for i := 0; i < 5; i++ {
		p.Samples = append(p.Samples, Sample{Timestamp: now.Add(time.Duration(-10+i) * time.Second), Count: 1})
	}
	for i := 0; i < 5; i++ {
		p.Samples = append(p.Samples, Sample{Timestamp: now.Add(time.Duration(-5+i) * time.Second), Count: 3})
	}
	p.AvgRate = 60.0
	p.StdDev = 0.8 // CV = 0.8 -> not bursty, checked first
	p.CurrentRate = 50.0
	e.mu.Unlock()

	patterns := e.AnalyzeBehavior()
	if patterns[0].Pattern != "growing" {
		t.Errorf("expected growing, got %s", patterns[0].Pattern)
	}
}

func TestDetectPattern_Declining(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	// First half: high counts, second half: low counts -> ratio < 0.5
	for i := 0; i < 5; i++ {
		p.Samples = append(p.Samples, Sample{Timestamp: now.Add(time.Duration(-10+i) * time.Second), Count: 4})
	}
	for i := 0; i < 5; i++ {
		p.Samples = append(p.Samples, Sample{Timestamp: now.Add(time.Duration(-5+i) * time.Second), Count: 1})
	}
	p.AvgRate = 60.0
	p.StdDev = 0.8 // CV = 0.8 -> moderate, not bursty
	p.CurrentRate = 10.0
	e.mu.Unlock()

	patterns := e.AnalyzeBehavior()
	if patterns[0].Pattern != "declining" {
		t.Errorf("expected declining, got %s", patterns[0].Pattern)
	}
}

func TestDetectPattern_Moderate(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	// Equal halves -> ratio ~1.0, CV between 0.5 and 2.0
	for i := 0; i < 10; i++ {
		p.Samples = append(p.Samples, Sample{Timestamp: now.Add(time.Duration(-i) * time.Second), Count: 2})
	}
	p.AvgRate = 120.0 // 2/sec -> 120/min
	p.StdDev = 1.5    // CV = 1.5/2.0 = 0.75 -> not bursty, not steady
	e.mu.Unlock()

	patterns := e.AnalyzeBehavior()
	if patterns[0].Pattern != "moderate" {
		t.Errorf("expected moderate, got %s", patterns[0].Pattern)
	}
}

// ---------------------------------------------------------------------------
// GetIntelligence
// ---------------------------------------------------------------------------

func TestGetIntelligence_NoAgents(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	intel := e.GetIntelligence()

	if intel.TotalActiveAgents != 0 {
		t.Errorf("expected 0 active agents, got %d", intel.TotalActiveAgents)
	}
	if intel.GlobalUtilization != 0 {
		t.Errorf("expected 0 utilization, got %.2f", intel.GlobalUtilization)
	}
	if intel.TotalRequestsPerMin != 0 {
		t.Errorf("expected 0 requests/min, got %.2f", intel.TotalRequestsPerMin)
	}
}

func TestGetIntelligence_AggregatesMultipleAgents(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	p1 := e.getOrCreateProfile("agent-1")
	p1.CurrentRate = 30
	p1.DynamicQuota = 60
	p1.LastRequest = now

	p2 := e.getOrCreateProfile("agent-2")
	p2.CurrentRate = 20
	p2.DynamicQuota = 40
	p2.LastRequest = now
	e.mu.Unlock()

	intel := e.GetIntelligence()

	if intel.TotalRequestsPerMin != 50 {
		t.Errorf("expected 50 requests/min, got %.2f", intel.TotalRequestsPerMin)
	}
	// GlobalUtilization = (50/100)*100 = 50
	if intel.GlobalUtilization != 50 {
		t.Errorf("expected 50%% utilization, got %.2f", intel.GlobalUtilization)
	}
}

func TestGetIntelligence_ActiveAgentsCutoff(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	active := e.getOrCreateProfile("active")
	active.LastRequest = now
	active.CurrentRate = 10
	active.DynamicQuota = 60

	stale := e.getOrCreateProfile("stale")
	stale.LastRequest = now.Add(-10 * time.Minute) // older than 5 min cutoff
	stale.CurrentRate = 5
	stale.DynamicQuota = 60
	e.mu.Unlock()

	intel := e.GetIntelligence()
	if intel.TotalActiveAgents != 1 {
		t.Errorf("expected 1 active agent, got %d", intel.TotalActiveAgents)
	}
}

func TestGetIntelligence_HighUtilizationRecommendation(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	p := e.getOrCreateProfile("agent-1")
	p.CurrentRate = 90
	p.DynamicQuota = 100
	p.LastRequest = now
	e.mu.Unlock()

	intel := e.GetIntelligence()
	found := false
	for _, r := range intel.Recommendations {
		if len(r) > 0 {
			found = true
		}
	}
	if !found {
		t.Error("expected recommendation for high utilization")
	}
}

func TestGetIntelligence_BurstyRecommendation(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	now := time.Now()
	e.mu.Lock()
	p := e.getOrCreateProfile("burst-agent")
	for i := 0; i < 10; i++ {
		p.Samples = append(p.Samples, Sample{Timestamp: now.Add(time.Duration(-i) * time.Second), Count: 1})
	}
	p.AvgRate = 60.0
	p.StdDev = 3.0 // CV=3.0 -> bursty, confidence = min(3/3,1) = 1.0 > 0.7
	p.PeakRate = 200.0
	p.CurrentRate = 10
	p.DynamicQuota = 60
	p.LastRequest = now
	e.mu.Unlock()

	intel := e.GetIntelligence()
	found := false
	for _, r := range intel.Recommendations {
		if r != "" {
			found = true
		}
	}
	if !found {
		t.Error("expected bursty recommendation")
	}
}

// ---------------------------------------------------------------------------
// AdjustQuota – additional edge cases
// ---------------------------------------------------------------------------

func TestAdjustQuota_DecreaseAction(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")

	resp, err := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "decrease", Amount: 5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.NewQuota != resp.PreviousQuota-5 {
		t.Errorf("expected decrease by 5, prev=%d new=%d", resp.PreviousQuota, resp.NewQuota)
	}
	if resp.Status != "applied" {
		t.Errorf("expected applied, got %s", resp.Status)
	}
}

func TestAdjustQuota_GrantCreditsAction(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")

	before := e.GetStatus("agent-1").BurstCredits
	_, err := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "grant_credits", Amount: 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	after := e.GetStatus("agent-1").BurstCredits
	if after <= before {
		t.Errorf("expected credits to increase, before=%.2f after=%.2f", before, after)
	}
}

func TestAdjustQuota_InvalidActionError(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "noop"})
	if err == nil {
		t.Error("expected error for unknown action")
	}
}

func TestAdjustQuota_EmptyAgentIDError(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.AdjustQuota(QuotaRequest{AgentID: "", Action: "reset"})
	if err == nil {
		t.Error("expected error for empty agent_id")
	}
}

func TestAllStatus_MultipleAgentsFields(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.RecordRequest("agent-1")
	e.RecordRequest("agent-2")
	e.RecordRequest("agent-3")

	statuses := e.AllStatus()
	if len(statuses) != 3 {
		t.Fatalf("expected 3 statuses, got %d", len(statuses))
	}

	ids := map[string]bool{}
	for _, s := range statuses {
		ids[s.AgentID] = true
		if s.BaseQuota != 60 {
			t.Errorf("%s: expected base quota 60, got %d", s.AgentID, s.BaseQuota)
		}
	}
	for _, id := range []string{"agent-1", "agent-2", "agent-3"} {
		if !ids[id] {
			t.Errorf("missing agent %s in AllStatus", id)
		}
	}
}
