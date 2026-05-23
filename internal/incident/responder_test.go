package incident

import (
	"log/slog"
	"testing"
	"time"
)

func newTestCorrelator() *Correlator {
	return NewCorrelator(DefaultCorrelatorConfig(), slog.Default())
}

func ingestTestEvents(c *Correlator) *IncidentCluster {
	now := time.Now()
	c.IngestEvent(AnomalyEvent{
		Cluster: "prod", Topic: "orders", Type: "consumer_lag",
		Severity: SeverityCritical, Value: 50000, Threshold: 10000,
		Timestamp: now,
	})
	c.IngestEvent(AnomalyEvent{
		Cluster: "prod", Group: "order-processor", Type: "throughput_drop",
		Severity: SeverityWarning, Value: 100, Threshold: 1000,
		Timestamp: now.Add(time.Second),
	})

	incidents := c.GetIncidents(1)
	if len(incidents) == 0 {
		return nil
	}
	return &incidents[0]
}

func TestGenerateHypotheses(t *testing.T) {
	t.Parallel()
	c := newTestCorrelator()
	incident := ingestTestEvents(c)
	if incident == nil {
		t.Skip("no incident created")
	}

	hypotheses, err := c.GenerateHypotheses(incident.ID)
	if err != nil {
		t.Fatalf("GenerateHypotheses: %v", err)
	}
	if len(hypotheses) == 0 {
		t.Error("expected at least one hypothesis")
	}
	if hypotheses[0].Confidence <= 0 {
		t.Error("expected positive confidence")
	}
	if hypotheses[0].Component == "" {
		t.Error("expected component to be set")
	}
	if len(hypotheses[0].TestSteps) == 0 {
		t.Error("expected test steps")
	}
}

func TestGenerateHypothesesNotFound(t *testing.T) {
	t.Parallel()
	c := newTestCorrelator()

	_, err := c.GenerateHypotheses("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent incident")
	}
}

func TestGenerateHypothesesBrokerDown(t *testing.T) {
	t.Parallel()
	c := newTestCorrelator()
	now := time.Now()

	c.IngestEvent(AnomalyEvent{
		Cluster: "prod", Type: "broker_down",
		Severity: SeverityCritical, Timestamp: now,
	})

	incidents := c.GetIncidents(10)
	if len(incidents) == 0 {
		t.Skip("no incident created")
	}

	hypotheses, err := c.GenerateHypotheses(incidents[len(incidents)-1].ID)
	if err != nil {
		t.Fatalf("GenerateHypotheses: %v", err)
	}

	hasBroker := false
	for _, h := range hypotheses {
		if h.Component == "broker" {
			hasBroker = true
			break
		}
	}
	if !hasBroker {
		t.Error("expected broker hypothesis for broker_down event")
	}
}

func TestGenerateRunbook(t *testing.T) {
	t.Parallel()
	c := newTestCorrelator()
	incident := ingestTestEvents(c)
	if incident == nil {
		t.Skip("no incident created")
	}

	runbook, err := c.GenerateRunbook(incident.ID)
	if err != nil {
		t.Fatalf("GenerateRunbook: %v", err)
	}
	if runbook.Status != "draft" {
		t.Errorf("expected draft status, got %s", runbook.Status)
	}
	if len(runbook.Actions) == 0 {
		t.Error("expected remediation actions")
	}
	if len(runbook.Hypotheses) == 0 {
		t.Error("expected hypotheses in runbook")
	}
	// Verify first action is always diagnostic
	if runbook.Actions[0].Action != "diagnose" {
		t.Errorf("expected first action to be diagnose, got %s", runbook.Actions[0].Action)
	}
	// Verify last action is always verify
	if runbook.Actions[len(runbook.Actions)-1].Action != "verify" {
		t.Errorf("expected last action to be verify, got %s", runbook.Actions[len(runbook.Actions)-1].Action)
	}
}

func TestGenerateRunbookNotFound(t *testing.T) {
	t.Parallel()
	c := newTestCorrelator()

	_, err := c.GenerateRunbook("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent incident")
	}
}

func TestApproveAndExecuteRunbook(t *testing.T) {
	t.Parallel()
	c := newTestCorrelator()
	incident := ingestTestEvents(c)
	if incident == nil {
		t.Skip("no incident created")
	}

	runbook, _ := c.GenerateRunbook(incident.ID)

	// Approve
	approved, err := c.ApproveRunbook(runbook.ID, "admin-user")
	if err != nil {
		t.Fatalf("ApproveRunbook: %v", err)
	}
	if approved.Status != "approved" {
		t.Errorf("expected approved, got %s", approved.Status)
	}
	if approved.ApprovedBy != "admin-user" {
		t.Errorf("expected admin-user, got %s", approved.ApprovedBy)
	}

	// Execute
	logs, err := c.ExecuteRunbook(runbook.ID)
	if err != nil {
		t.Fatalf("ExecuteRunbook: %v", err)
	}
	if len(logs) == 0 {
		t.Error("expected execution logs")
	}
	for _, log := range logs {
		if log.Status != "succeeded" {
			t.Errorf("expected succeeded, got %s for action %s", log.Status, log.ActionID)
		}
	}

	// Verify runbook is completed
	rb, _ := c.GetRunbook(runbook.ID)
	if rb.Status != "completed" {
		t.Errorf("expected completed after execution, got %s", rb.Status)
	}
}

func TestExecuteRunbookRequiresApproval(t *testing.T) {
	t.Parallel()
	c := newTestCorrelator()
	incident := ingestTestEvents(c)
	if incident == nil {
		t.Skip("no incident created")
	}

	runbook, _ := c.GenerateRunbook(incident.ID)

	// Execute without approval should fail
	if _, err := c.ExecuteRunbook(runbook.ID); err == nil {
		t.Error("expected error for unapproved execution")
	}
}

func TestApproveRunbookValidation(t *testing.T) {
	t.Parallel()
	c := newTestCorrelator()

	if _, err := c.ApproveRunbook("nonexistent", "admin"); err == nil {
		t.Error("expected error for nonexistent runbook")
	}

	incident := ingestTestEvents(c)
	if incident == nil {
		t.Skip("no incident created")
	}

	runbook, _ := c.GenerateRunbook(incident.ID)
	_, _ = c.ApproveRunbook(runbook.ID, "admin")

	// Double approve should fail
	if _, err := c.ApproveRunbook(runbook.ID, "admin"); err == nil {
		t.Error("expected error for already-approved runbook")
	}
}

func TestGetRunbookNotFound(t *testing.T) {
	t.Parallel()
	c := newTestCorrelator()

	if _, err := c.GetRunbook("nonexistent"); err == nil {
		t.Error("expected error for nonexistent runbook")
	}
}

func TestFullRespond(t *testing.T) {
	t.Parallel()
	c := newTestCorrelator()
	incident := ingestTestEvents(c)
	if incident == nil {
		t.Skip("no incident created")
	}

	result, err := c.Respond(incident.ID)
	if err != nil {
		t.Fatalf("Respond: %v", err)
	}
	if result.Summary == "" {
		t.Error("expected summary")
	}
	if result.Runbook == nil {
		t.Error("expected runbook")
	}
	if len(result.Hypotheses) == 0 {
		t.Error("expected hypotheses")
	}
	if result.Severity == "" {
		t.Error("expected severity")
	}
}
