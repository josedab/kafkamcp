package adaptiverate

import (
	"testing"
	"time"
)

// --- AllStatus ---

func TestAllStatusEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	statuses := e.AllStatus()
	if len(statuses) != 0 {
		t.Errorf("expected 0, got %d", len(statuses))
	}
}

func TestAllStatusMultipleAgents(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")
	e.RecordRequest("agent-2")
	e.RecordRequest("agent-2")

	statuses := e.AllStatus()
	if len(statuses) != 2 {
		t.Errorf("expected 2, got %d", len(statuses))
	}
}

func TestAllStatusDifferentStates(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")
	e.AdjustQuota(QuotaRequest{AgentID: "agent-2", Action: "increase", Amount: 100})

	statuses := e.AllStatus()
	if len(statuses) != 2 {
		t.Errorf("expected 2, got %d", len(statuses))
	}
}

// --- autoAdjustQuota ---

func TestAutoAdjustQuotaBurstDetection(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	profile := e.getOrCreateProfile("agent-1")
	profile.DynamicQuota = 10
	profile.CurrentRate = 9 // 90% of quota
	profile.BurstEvents = 4

	e.autoAdjustQuota(profile)
	if profile.DynamicQuota <= 10 {
		t.Errorf("expected quota increase, got %d", profile.DynamicQuota)
	}
}

func TestAutoAdjustQuotaMaxCeiling(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	profile := e.getOrCreateProfile("agent-1")
	profile.BaseQuota = 10
	profile.DynamicQuota = 30 // at 3x base
	profile.CurrentRate = 25
	profile.BurstEvents = 5

	e.autoAdjustQuota(profile)
	// Should warn about approaching ceiling but not increase past 3x
	if profile.DynamicQuota > 30 {
		t.Errorf("expected quota capped at ceiling, got %d", profile.DynamicQuota)
	}
}

func TestAutoAdjustQuotaLowUsageDecrease(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	profile := e.getOrCreateProfile("agent-1")
	profile.DynamicQuota = 60
	profile.BaseQuota = 60
	profile.TotalRequests = 1001
	profile.CurrentRate = 3 // 5% usage

	e.autoAdjustQuota(profile)
	if profile.DynamicQuota >= 60 {
		t.Errorf("expected quota decrease for low usage, got %d", profile.DynamicQuota)
	}
}

func TestAutoAdjustQuotaFloorEnforcement(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	profile := e.getOrCreateProfile("agent-1")
	profile.DynamicQuota = 31 // just above base/2=30
	profile.BaseQuota = 60
	profile.TotalRequests = 1001
	profile.CurrentRate = 2

	e.autoAdjustQuota(profile)
	if profile.DynamicQuota < profile.BaseQuota/2 {
		t.Errorf("expected quota not below floor %d, got %d", profile.BaseQuota/2, profile.DynamicQuota)
	}
}

// --- AdjustQuota ---

func TestAdjustQuotaIncrease(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")

	resp, err := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "increase", Amount: 20})
	if err != nil {
		t.Fatalf("AdjustQuota failed: %v", err)
	}
	if resp.NewQuota != resp.PreviousQuota+20 {
		t.Errorf("expected increase by 20, got %d -> %d", resp.PreviousQuota, resp.NewQuota)
	}
}

func TestAdjustQuotaDecrease(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")

	resp, _ := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "decrease", Amount: 10})
	if resp.NewQuota != resp.PreviousQuota-10 {
		t.Errorf("expected decrease by 10, got %d -> %d", resp.PreviousQuota, resp.NewQuota)
	}
}

func TestAdjustQuotaDecreaseFloor(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")

	// Try to decrease more than allowed (floor = base/4 = 15)
	e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "decrease", Amount: 999})
	status := e.GetStatus("agent-1")
	if status.DynamicQuota < 15 {
		t.Errorf("expected floor of 15, got %d", status.DynamicQuota)
	}
}

func TestAdjustQuotaIncreaseCeiling(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")

	// Try to increase past ceiling (5x base = 300)
	e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "increase", Amount: 9999})
	status := e.GetStatus("agent-1")
	if status.DynamicQuota > 300 {
		t.Errorf("expected ceiling of 300, got %d", status.DynamicQuota)
	}
}

func TestAdjustQuotaReset(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")
	e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "increase", Amount: 100})

	resp, _ := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "reset"})
	if resp.NewQuota != 60 {
		t.Errorf("expected reset to base 60, got %d", resp.NewQuota)
	}
}

func TestAdjustQuotaGrantCredits(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")

	resp, err := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "grant_credits", Amount: 5})
	if err != nil {
		t.Fatalf("AdjustQuota failed: %v", err)
	}
	if resp.Status != "applied" {
		t.Errorf("expected applied, got %s", resp.Status)
	}
}

func TestAdjustQuotaGrantCreditsCapped(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")

	// Grant massive credits - should be capped
	e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "grant_credits", Amount: 9999})
	status := e.GetStatus("agent-1")
	if status.BurstCredits > status.MaxCredits*2 {
		t.Errorf("expected credits capped at 2x max, got %.0f", status.BurstCredits)
	}
}

func TestAdjustQuotaInvalidAction(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "invalid"})
	if err == nil {
		t.Error("expected error for invalid action")
	}
}

func TestAdjustQuotaEmptyAgentID(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	_, err := e.AdjustQuota(QuotaRequest{AgentID: "", Action: "increase"})
	if err == nil {
		t.Error("expected error for empty agent_id")
	}
}

func TestAdjustQuotaDefaultAmount(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")

	resp, _ := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "increase", Amount: 0})
	// Default amount = base/4 = 15
	if resp.NewQuota != resp.PreviousQuota+15 {
		t.Errorf("expected default increase of 15, got %d -> %d", resp.PreviousQuota, resp.NewQuota)
	}
}

// --- Allow ---

func TestAllowWithinQuota(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	allowed := e.Allow("agent-1")
	if !allowed {
		t.Error("expected allowed within quota")
	}
}

func TestAllowExceededUseBurstCredits(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	profile := e.getOrCreateProfile("agent-1")
	profile.CurrentRate = 100 // exceed dynamic quota (60)
	profile.Credits = 5

	e.mu.Lock()
	allowed := false
	if profile.CurrentRate <= float64(profile.DynamicQuota) {
		allowed = true
	} else if profile.Credits >= 1.0 {
		profile.Credits--
		allowed = true
	}
	e.mu.Unlock()

	if !allowed {
		t.Error("expected allowed via burst credits")
	}
	if profile.Credits != 4 {
		t.Errorf("expected 4 credits remaining, got %.0f", profile.Credits)
	}
}

func TestAllowExceededNoCredits(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.mu.Lock()
	profile := e.getOrCreateProfile("agent-1")
	profile.CurrentRate = 100
	profile.Credits = 0
	e.mu.Unlock()

	allowed := e.Allow("agent-1")
	if allowed {
		t.Error("expected denied when exceeding quota with no credits")
	}
}

// --- refillCredits ---

func TestRefillCredits(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	profile := e.getOrCreateProfile("agent-1")
	profile.Credits = 5
	profile.LastRequest = time.Now().Add(-10 * time.Second)

	e.refillCredits(profile, time.Now())
	if profile.Credits <= 5 {
		t.Error("expected credits to increase after refill")
	}
}

func TestRefillCreditsCapped(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	profile := e.getOrCreateProfile("agent-1")
	profile.Credits = 19
	profile.LastRequest = time.Now().Add(-100 * time.Second)

	e.refillCredits(profile, time.Now())
	if profile.Credits > profile.MaxCredits {
		t.Errorf("expected credits capped at %f, got %f", profile.MaxCredits, profile.Credits)
	}
}

func TestRefillCreditsZeroLastRequest(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	profile := e.getOrCreateProfile("agent-1")
	profile.Credits = 5
	// LastRequest is zero value

	e.refillCredits(profile, time.Now())
	if profile.Credits != 5 {
		t.Errorf("expected no refill with zero LastRequest, got %f", profile.Credits)
	}
}

// --- GetStatus edge cases ---

func TestGetStatusNewAgent(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	status := e.GetStatus("new-agent")
	if status.BaseQuota != 60 {
		t.Errorf("expected base quota 60, got %d", status.BaseQuota)
	}
}

func TestGetStatusHighUtilization(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	for i := 0; i < 100; i++ {
		e.RecordRequest("busy-agent")
	}

	status := e.GetStatus("busy-agent")
	if status.TotalRequests != 100 {
		t.Errorf("expected 100 total, got %d", status.TotalRequests)
	}
}

func TestGetStatusDenyRate(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.mu.Lock()
	profile := e.getOrCreateProfile("agent-1")
	profile.TotalRequests = 100
	profile.DeniedRequests = 25
	e.mu.Unlock()

	status := e.GetStatus("agent-1")
	if status.DenyRate != 25 {
		t.Errorf("expected 25%% deny rate, got %.2f", status.DenyRate)
	}
}

// --- getOrCreateProfile ---

func TestGetOrCreateProfileNew(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.mu.Lock()
	profile := e.getOrCreateProfile("new-agent")
	e.mu.Unlock()

	if profile.BaseQuota != 60 {
		t.Errorf("expected base quota 60, got %d", profile.BaseQuota)
	}
	if profile.Credits != 10 {
		t.Errorf("expected 10 initial credits, got %f", profile.Credits)
	}
}

func TestGetOrCreateProfileExisting(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())
	e.RecordRequest("agent-1")

	e.mu.Lock()
	profile := e.getOrCreateProfile("agent-1")
	e.mu.Unlock()

	if profile.TotalRequests != 1 {
		t.Errorf("expected 1 total request, got %d", profile.TotalRequests)
	}
}
