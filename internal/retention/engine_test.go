package retention

import (
	"testing"
	"time"
)

func TestNewEngine(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	if e == nil {
		t.Fatal("NewEngine returned nil")
	}
}

func TestSetPolicy(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	err := e.SetPolicy(RetentionPolicy{
		Topic:              "orders",
		CurrentRetentionMs: 7 * 24 * 3600 * 1000,
		Compliance:         ComplianceNone,
	})
	if err != nil {
		t.Fatalf("SetPolicy error: %v", err)
	}

	p, ok := e.GetPolicy("orders")
	if !ok {
		t.Fatal("policy not found")
	}
	if p.Topic != "orders" {
		t.Errorf("Topic = %q, want orders", p.Topic)
	}
}

func TestSetPolicyEmptyTopic(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	err := e.SetPolicy(RetentionPolicy{})
	if err == nil {
		t.Error("expected error for empty topic")
	}
}

func TestSetPolicyComplianceViolation(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	err := e.SetPolicy(RetentionPolicy{
		Topic:             "medical-records",
		TargetRetentionMs: 1000, // Way too short for HIPAA
		Compliance:        ComplianceHIPAA,
	})
	if err == nil {
		t.Error("expected HIPAA compliance error")
	}
}

func TestAnalyze(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	_ = e.SetPolicy(RetentionPolicy{
		Topic:              "stale-topic",
		CurrentRetentionMs: 30 * 24 * 3600 * 1000,
		Compliance:         ComplianceNone,
	})

	recs := e.Analyze([]UsageMetrics{
		{
			Topic:           "stale-topic",
			ActiveConsumers: 0,
			LastReadTime:    time.Now().Add(-60 * 24 * time.Hour),
			StorageBytes:    1024 * 1024 * 1024,
			MessageCount:    1000000,
			BytesPerDay:     1024 * 1024,
		},
	})

	if len(recs) == 0 {
		t.Error("expected retention recommendations")
	}
	if recs[0].EstimatedSavingsPct <= 0 {
		t.Error("expected positive savings estimate")
	}
}

func TestRequestChange(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	_ = e.SetPolicy(RetentionPolicy{
		Topic:              "orders",
		CurrentRetentionMs: 30 * 24 * 3600 * 1000,
		Compliance:         ComplianceNone,
	})

	req, err := e.RequestChange("orders", 7*24*3600*1000, "reduce costs")
	if err != nil {
		t.Fatalf("RequestChange error: %v", err)
	}
	if req.Status != "pending" {
		t.Errorf("Status = %q, want pending", req.Status)
	}
}

func TestApproveChange(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	_ = e.SetPolicy(RetentionPolicy{
		Topic:              "orders",
		CurrentRetentionMs: 30 * 24 * 3600 * 1000,
		Compliance:         ComplianceNone,
	})

	req, _ := e.RequestChange("orders", 7*24*3600*1000, "reduce costs")
	err := e.ApproveChange(req.ID, "admin")
	if err != nil {
		t.Fatalf("ApproveChange error: %v", err)
	}

	requests := e.GetChangeRequests("approved")
	if len(requests) != 1 {
		t.Errorf("expected 1 approved request, got %d", len(requests))
	}
}

func TestRequestChangeComplianceViolation(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	_ = e.SetPolicy(RetentionPolicy{
		Topic:              "audit-logs",
		CurrentRetentionMs: 10 * 365 * 24 * 3600 * 1000,
		Compliance:         ComplianceSOX,
	})

	_, err := e.RequestChange("audit-logs", 1000, "oops")
	if err == nil {
		t.Error("expected compliance error")
	}
}
