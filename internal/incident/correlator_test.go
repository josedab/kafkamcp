package incident

import (
	"testing"
	"time"
)

func TestNewCorrelator(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	if c == nil {
		t.Fatal("NewCorrelator returned nil")
	}
	stats := c.Stats()
	if stats["total_events"].(int) != 0 {
		t.Error("expected 0 events")
	}
}

func TestIngestSingleEvent(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	result := c.IngestEvent(AnomalyEvent{
		Cluster:   "prod-1",
		Topic:     "orders",
		Type:      "consumer_lag",
		Severity:  SeverityWarning,
		Value:     10000,
		Timestamp: time.Now(),
	})
	// Single event shouldn't create incident
	if result != nil {
		t.Error("single event should not create incident")
	}
}

func TestIngestCorrelatedEvents(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()

	c.IngestEvent(AnomalyEvent{
		ID: "e1", Cluster: "prod-1", Topic: "orders",
		Type: "consumer_lag", Severity: SeverityWarning,
		Timestamp: now,
	})
	result := c.IngestEvent(AnomalyEvent{
		ID: "e2", Cluster: "prod-1", Topic: "payments",
		Type: "throughput_drop", Severity: SeverityCritical,
		Timestamp: now.Add(5 * time.Second),
	})

	if result == nil {
		t.Fatal("expected incident from correlated events")
	}
	if result.Status != "active" {
		t.Errorf("Status = %v, want active", result.Status)
	}
	if len(result.Clusters) == 0 {
		t.Error("expected affected clusters")
	}
	if result.RootCause == nil {
		t.Error("expected root cause")
	}
}

func TestCrossClusterCorrelation(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	c.SetDependencies([]TopicDependency{
		{Source: "orders", Target: "orders-replica", Type: "mirrors"},
	})

	now := time.Now()
	c.IngestEvent(AnomalyEvent{
		ID: "e1", Cluster: "prod-1", Topic: "orders",
		Type: "broker_down", Severity: SeverityCritical,
		Timestamp: now,
	})
	result := c.IngestEvent(AnomalyEvent{
		ID: "e2", Cluster: "prod-2", Topic: "orders-replica",
		Type: "throughput_drop", Severity: SeverityWarning,
		Timestamp: now.Add(10 * time.Second),
	})

	if result == nil {
		t.Fatal("expected cross-cluster incident")
	}
	if len(result.Clusters) < 2 {
		t.Errorf("expected 2 clusters, got %d", len(result.Clusters))
	}
}

func TestGenerateReport(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	now := time.Now()

	c.IngestEvent(AnomalyEvent{
		ID: "e1", Cluster: "prod-1", Topic: "orders",
		Type: "consumer_lag", Severity: SeverityWarning,
		Timestamp: now,
	})
	inc := c.IngestEvent(AnomalyEvent{
		ID: "e2", Cluster: "prod-1", Topic: "payments",
		Type: "error_spike", Severity: SeverityCritical,
		Timestamp: now.Add(5 * time.Second),
	})

	if inc == nil {
		t.Fatal("expected incident")
	}

	report, err := c.GenerateReport(inc.ID)
	if err != nil {
		t.Fatalf("GenerateReport error: %v", err)
	}
	if len(report.Timeline) == 0 {
		t.Error("expected timeline entries")
	}
	if len(report.Remediation) == 0 {
		t.Error("expected remediation steps")
	}
}

func TestGenerateReportNotFound(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	_, err := c.GenerateReport("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent incident")
	}
}

func TestGetIncidents(t *testing.T) {
	c := NewCorrelator(DefaultCorrelatorConfig(), nil)
	incidents := c.GetIncidents(10)
	if len(incidents) != 0 {
		t.Errorf("expected 0 incidents, got %d", len(incidents))
	}
}
