package topicportal

import (
	"testing"
)

func TestNewEngine(t *testing.T) {
	t.Parallel()
	e := NewEngine(DefaultApprovalChainConfig(), nil)
	if e == nil {
		t.Fatal("NewEngine returned nil")
	}
}

func TestSubmitRequest(t *testing.T) {
	t.Parallel()
	e := NewEngine(DefaultApprovalChainConfig(), nil)
	req, err := e.SubmitRequest(TopicRequest{
		TopicName:     "prod.orders",
		RequestedBy:   "agent-1",
		Justification: "Need orders topic for analytics",
		Owner:         "analytics-team",
	})
	if err != nil {
		t.Fatalf("SubmitRequest error: %v", err)
	}
	if req.Status != StatusPending {
		t.Errorf("Status = %v, want pending", req.Status)
	}
	if len(req.ApprovalChain) != 2 {
		t.Errorf("expected 2 approval steps, got %d", len(req.ApprovalChain))
	}
}

func TestSubmitRequestAutoApprove(t *testing.T) {
	t.Parallel()
	e := NewEngine(DefaultApprovalChainConfig(), nil)
	req, err := e.SubmitRequest(TopicRequest{
		TopicName:     "dev.test-topic",
		RequestedBy:   "agent-1",
		Justification: "Development testing",
	})
	if err != nil {
		t.Fatalf("SubmitRequest error: %v", err)
	}
	if req.Status != StatusApproved {
		t.Errorf("Status = %v, want approved (auto-approve for dev.*)", req.Status)
	}
}

func TestSubmitRequestValidation(t *testing.T) {
	t.Parallel()
	e := NewEngine(DefaultApprovalChainConfig(), nil)
	if _, err := e.SubmitRequest(TopicRequest{RequestedBy: "a", Justification: "j"}); err == nil {
		t.Error("expected error for missing topic_name")
	}
	if _, err := e.SubmitRequest(TopicRequest{TopicName: "t", Justification: "j"}); err == nil {
		t.Error("expected error for missing requested_by")
	}
	if _, err := e.SubmitRequest(TopicRequest{TopicName: "t", RequestedBy: "a"}); err == nil {
		t.Error("expected error for missing justification")
	}
}

func TestApprovalChain(t *testing.T) {
	t.Parallel()
	e := NewEngine(DefaultApprovalChainConfig(), nil)
	req, _ := e.SubmitRequest(TopicRequest{
		TopicName:     "prod.orders",
		RequestedBy:   "agent-1",
		Justification: "Need orders topic",
	})

	// First approval
	err := e.Approve(req.ID, "lead-1", "LGTM")
	if err != nil {
		t.Fatalf("first Approve error: %v", err)
	}

	updated, _ := e.GetRequest(req.ID)
	if updated.Status != StatusPending {
		t.Errorf("after first approval, Status = %v, want pending", updated.Status)
	}
	if updated.CurrentStep != 1 {
		t.Errorf("CurrentStep = %d, want 1", updated.CurrentStep)
	}

	// Second approval
	err = e.Approve(req.ID, "admin-1", "Approved")
	if err != nil {
		t.Fatalf("second Approve error: %v", err)
	}

	updated, _ = e.GetRequest(req.ID)
	if updated.Status != StatusApproved {
		t.Errorf("after all approvals, Status = %v, want approved", updated.Status)
	}
}

func TestReject(t *testing.T) {
	t.Parallel()
	e := NewEngine(DefaultApprovalChainConfig(), nil)
	req, _ := e.SubmitRequest(TopicRequest{
		TopicName:     "prod.dangerous",
		RequestedBy:   "agent-1",
		Justification: "Reasons",
	})

	err := e.Reject(req.ID, "admin-1", "Not justified")
	if err != nil {
		t.Fatalf("Reject error: %v", err)
	}

	updated, _ := e.GetRequest(req.ID)
	if updated.Status != StatusRejected {
		t.Errorf("Status = %v, want rejected", updated.Status)
	}
}

func TestListRequests(t *testing.T) {
	t.Parallel()
	e := NewEngine(DefaultApprovalChainConfig(), nil)
	e.SubmitRequest(TopicRequest{TopicName: "prod.a", RequestedBy: "a", Justification: "j"})
	e.SubmitRequest(TopicRequest{TopicName: "dev.b", RequestedBy: "a", Justification: "j"}) // auto-approved

	pending := e.ListRequests(StatusPending, 10)
	if len(pending) != 1 {
		t.Errorf("expected 1 pending, got %d", len(pending))
	}

	approved := e.ListRequests(StatusApproved, 10)
	if len(approved) != 1 {
		t.Errorf("expected 1 approved, got %d", len(approved))
	}
}
