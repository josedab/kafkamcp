package portal

import (
	"log/slog"
	"testing"
)

func TestSubmitRequest(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	req, err := e.SubmitRequest(RequestTopicCreate, "user-1", "Create orders topic",
		"Need a new topic for order events", "prod", "orders",
		map[string]any{"partitions": 12, "replication_factor": 3})
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	if req.Status != StatusPending {
		t.Errorf("expected pending, got %s", req.Status)
	}
	if len(req.ApprovalChain) != 2 {
		t.Errorf("expected 2 approval steps (team_lead, admin), got %d", len(req.ApprovalChain))
	}
	if req.Requester != "user-1" {
		t.Errorf("expected user-1, got %s", req.Requester)
	}
}

func TestSubmitRequestAutoApprove(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	req, err := e.SubmitRequest(RequestAccessRevoke, "admin", "Revoke access",
		"Remove user-x access", "prod", "", nil)
	if err != nil {
		t.Fatalf("SubmitRequest: %v", err)
	}
	if req.Status != StatusApproved {
		t.Errorf("expected auto-approved, got %s", req.Status)
	}
	for _, step := range req.ApprovalChain {
		if step.Status != "approved" {
			t.Errorf("expected all approval steps to be approved for auto-approve, got %s", step.Status)
		}
	}
}

func TestReviewRequest(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	req, _ := e.SubmitRequest(RequestSchemaChange, "dev-1", "Add field",
		"Adding new field to schema", "prod", "users", nil)

	reviewed, err := e.ReviewRequest(req.ID, "lead-1", "approved", "Looks good")
	if err != nil {
		t.Fatalf("ReviewRequest: %v", err)
	}
	if reviewed.Status != StatusApproved {
		t.Errorf("expected approved, got %s", reviewed.Status)
	}
}

func TestReviewRequestReject(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	req, _ := e.SubmitRequest(RequestTopicDelete, "user-1", "Delete topic",
		"No longer needed", "prod", "old-topic", nil)

	reviewed, err := e.ReviewRequest(req.ID, "lead-1", "rejected", "Still in use")
	if err != nil {
		t.Fatalf("ReviewRequest: %v", err)
	}
	if reviewed.Status != StatusRejected {
		t.Errorf("expected rejected, got %s", reviewed.Status)
	}
}

func TestReviewRequestMultiStepApproval(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	// topic_delete requires team_lead, admin, security
	req, _ := e.SubmitRequest(RequestTopicDelete, "user-1", "Delete old topic",
		"", "prod", "old-topic", nil)

	if len(req.ApprovalChain) != 3 {
		t.Fatalf("expected 3 approval steps, got %d", len(req.ApprovalChain))
	}

	// Approve step 1 (team_lead)
	req, _ = e.ReviewRequest(req.ID, "lead-1", "approved", "ok")
	if req.Status != StatusPending {
		t.Errorf("expected still pending after 1/3 approvals, got %s", req.Status)
	}

	// Approve step 2 (admin)
	req, _ = e.ReviewRequest(req.ID, "admin-1", "approved", "ok")
	if req.Status != StatusPending {
		t.Errorf("expected still pending after 2/3 approvals, got %s", req.Status)
	}

	// Approve step 3 (security)
	req, _ = e.ReviewRequest(req.ID, "sec-1", "approved", "ok")
	if req.Status != StatusApproved {
		t.Errorf("expected approved after all approvals, got %s", req.Status)
	}
}

func TestReviewRequestValidation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	if _, err := e.ReviewRequest("nonexistent", "admin", "approved", ""); err == nil {
		t.Error("expected error for nonexistent request")
	}

	req, _ := e.SubmitRequest(RequestSchemaChange, "dev", "title", "", "", "", nil)
	if _, err := e.ReviewRequest(req.ID, "admin", "invalid_decision", ""); err == nil {
		t.Error("expected error for invalid decision")
	}

	// Approve, then try to review again
	e.ReviewRequest(req.ID, "admin", "approved", "")
	if _, err := e.ReviewRequest(req.ID, "admin", "approved", ""); err == nil {
		t.Error("expected error for reviewing already-approved request")
	}
}

func TestSubmitInvalidRequestType(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	_, err := e.SubmitRequest("nonexistent_type", "user", "title", "", "", "", nil)
	if err == nil {
		t.Error("expected error for invalid request type")
	}
}

func TestListRequests(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	e.SubmitRequest(RequestTopicCreate, "u1", "t1", "", "", "", nil)
	e.SubmitRequest(RequestSchemaChange, "u2", "t2", "", "", "", nil)
	e.SubmitRequest(RequestAccessRevoke, "u3", "t3", "", "", "", nil) // auto-approved

	pending := e.ListRequests(StatusPending, "")
	if len(pending) != 2 {
		t.Errorf("expected 2 pending, got %d", len(pending))
	}

	approved := e.ListRequests(StatusApproved, "")
	if len(approved) != 1 {
		t.Errorf("expected 1 approved, got %d", len(approved))
	}

	byType := e.ListRequests("", RequestTopicCreate)
	if len(byType) != 1 {
		t.Errorf("expected 1 topic_create, got %d", len(byType))
	}

	all := e.ListRequests("", "")
	if len(all) != 3 {
		t.Errorf("expected 3 total, got %d", len(all))
	}
}

func TestDashboard(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	e.SubmitRequest(RequestTopicCreate, "u1", "t1", "", "", "", nil)
	e.SubmitRequest(RequestSchemaChange, "u2", "t2", "", "", "", nil)

	dashboard := e.GetDashboard()
	if dashboard.TotalRequests != 2 {
		t.Errorf("expected 2 total, got %d", dashboard.TotalRequests)
	}
	if dashboard.PendingRequests != 2 {
		t.Errorf("expected 2 pending, got %d", dashboard.PendingRequests)
	}
	// All pending, no approvals → score 0
	if dashboard.ComplianceScore != 0 {
		t.Errorf("expected 0 compliance score for all-pending, got %d", dashboard.ComplianceScore)
	}
}

func TestDashboardComplianceScore(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	// Auto-approved
	e.SubmitRequest(RequestAccessRevoke, "u1", "t1", "", "", "", nil)
	e.SubmitRequest(RequestAccessRevoke, "u2", "t2", "", "", "", nil)

	dashboard := e.GetDashboard()
	if dashboard.ComplianceScore != 100 {
		t.Errorf("expected 100 score when all approved, got %d", dashboard.ComplianceScore)
	}
}

func TestDashboardEmpty(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	dashboard := e.GetDashboard()
	if dashboard.TotalRequests != 0 {
		t.Errorf("expected 0 total, got %d", dashboard.TotalRequests)
	}
	if dashboard.ComplianceScore != 100 {
		t.Errorf("expected 100 score for empty dashboard, got %d", dashboard.ComplianceScore)
	}
}

func TestSubmitRequestValidation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	if _, err := e.SubmitRequest(RequestTopicCreate, "", "title", "", "", "", nil); err == nil {
		t.Error("expected error for empty requester")
	}

	if _, err := e.SubmitRequest(RequestTopicCreate, "user", "", "", "", "", nil); err == nil {
		t.Error("expected error for empty title")
	}
}

func TestGetRequest(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	if _, err := e.GetRequest("nonexistent"); err == nil {
		t.Error("expected error for nonexistent request")
	}

	req, _ := e.SubmitRequest(RequestTopicCreate, "user", "title", "", "", "", nil)
	got, err := e.GetRequest(req.ID)
	if err != nil {
		t.Fatalf("GetRequest: %v", err)
	}
	if got.ID != req.ID {
		t.Errorf("expected %s, got %s", req.ID, got.ID)
	}
}

func TestGetWorkflows(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	workflows := e.GetWorkflows()
	if len(workflows) == 0 {
		t.Error("expected default workflows")
	}
	// Should have workflows for all request types
	if len(workflows) < 5 {
		t.Errorf("expected at least 5 default workflows, got %d", len(workflows))
	}
}
