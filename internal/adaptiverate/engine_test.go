package adaptiverate

import (
	"log/slog"
	"os"
	"testing"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestRecordAndAllow(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.RecordRequest("agent-1")
	allowed := e.Allow("agent-1")
	if !allowed {
		t.Error("expected request to be allowed")
	}
}

func TestQuotaStatus(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.RecordRequest("agent-1")
	e.RecordRequest("agent-1")

	status := e.GetStatus("agent-1")
	if status.AgentID != "agent-1" {
		t.Errorf("expected agent-1, got %s", status.AgentID)
	}
	if status.TotalRequests != 2 {
		t.Errorf("expected 2 total requests, got %d", status.TotalRequests)
	}
}

func TestAdjustQuota(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.RecordRequest("agent-1")

	resp, err := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "increase", Amount: 30})
	if err != nil {
		t.Fatalf("AdjustQuota failed: %v", err)
	}
	if resp.NewQuota <= resp.PreviousQuota {
		t.Error("expected quota to increase")
	}
}

func TestResetQuota(t *testing.T) {
	t.Parallel()
	e := NewEngine(testLogger())

	e.RecordRequest("agent-1")
	_, _ = e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "increase", Amount: 100})
	resp, _ := e.AdjustQuota(QuotaRequest{AgentID: "agent-1", Action: "reset"})

	if resp.NewQuota != 60 { // base quota
		t.Errorf("expected reset to base quota 60, got %d", resp.NewQuota)
	}
}
