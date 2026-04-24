package observability

import (
	"strings"
	"testing"
	"time"
)

func TestCreateDashboard(t *testing.T) {
	m := NewManager()

	// Cluster dashboard (default type)
	d, err := m.CreateDashboard("Cluster Overview", "grafana", "cluster", "", "admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Name != "Cluster Overview" {
		t.Fatalf("expected name 'Cluster Overview', got %q", d.Name)
	}
	if d.Provider != "grafana" {
		t.Fatalf("expected provider 'grafana', got %q", d.Provider)
	}
	if d.Type != "cluster" {
		t.Fatalf("expected type 'cluster', got %q", d.Type)
	}
	if len(d.Panels) != 6 {
		t.Fatalf("expected 6 cluster panels, got %d", len(d.Panels))
	}

	// Topic dashboard
	d, err = m.CreateDashboard("Topic Dash", "datadog", "topic", "orders", "admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Type != "topic" {
		t.Fatalf("expected type 'topic', got %q", d.Type)
	}
	if len(d.Panels) != 5 {
		t.Fatalf("expected 5 topic panels, got %d", len(d.Panels))
	}
	if !strings.Contains(d.Panels[0].MetricExpr, "orders") {
		t.Fatalf("expected metric to contain target 'orders', got %q", d.Panels[0].MetricExpr)
	}

	// Consumer group dashboard
	d, err = m.CreateDashboard("CG Dash", "", "consumer_group", "my-group", "admin")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d.Provider != "grafana" {
		t.Fatalf("expected default provider 'grafana', got %q", d.Provider)
	}
	if len(d.Panels) != 4 {
		t.Fatalf("expected 4 consumer_group panels, got %d", len(d.Panels))
	}

	// Missing name
	_, err = m.CreateDashboard("", "grafana", "cluster", "", "admin")
	if err == nil {
		t.Fatal("expected error for missing name")
	}
}

func TestListDashboards(t *testing.T) {
	m := NewManager()

	// Empty list
	dashboards := m.ListDashboards()
	if len(dashboards) != 0 {
		t.Fatalf("expected 0 dashboards, got %d", len(dashboards))
	}

	// Multiple dashboards
	m.CreateDashboard("First", "grafana", "cluster", "", "admin")
	time.Sleep(5 * time.Millisecond)
	m.CreateDashboard("Second", "grafana", "topic", "t1", "admin")

	dashboards = m.ListDashboards()
	if len(dashboards) != 2 {
		t.Fatalf("expected 2 dashboards, got %d", len(dashboards))
	}
	names := map[string]bool{dashboards[0].Name: true, dashboards[1].Name: true}
	if !names["First"] || !names["Second"] {
		t.Fatalf("expected dashboards 'First' and 'Second', got %q and %q", dashboards[0].Name, dashboards[1].Name)
	}
}

func TestAlertRuleCRUD(t *testing.T) {
	m := NewManager()

	// Create
	rule, err := m.CreateAlertRule("High Lag", "kafka_consumer_group_lag", "gt", 1000, "5m", "critical", "admin", []string{"slack"}, "orders", "my-group")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rule.Name != "High Lag" {
		t.Fatalf("expected name 'High Lag', got %q", rule.Name)
	}
	if !rule.Enabled {
		t.Fatal("expected rule to be enabled")
	}

	// Create with defaults
	time.Sleep(5 * time.Millisecond)
	rule2, err := m.CreateAlertRule("Low Throughput", "kafka_messages_in", "", 0, "", "", "admin", nil, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rule2.Condition != "gt" {
		t.Fatalf("expected default condition 'gt', got %q", rule2.Condition)
	}
	if rule2.Severity != "warning" {
		t.Fatalf("expected default severity 'warning', got %q", rule2.Severity)
	}
	if rule2.Duration != "5m" {
		t.Fatalf("expected default duration '5m', got %q", rule2.Duration)
	}

	// List
	rules := m.ListAlertRules()
	if len(rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(rules))
	}

	// Delete
	err = m.DeleteAlertRule(rule.ID)
	if err != nil {
		t.Fatalf("unexpected error deleting rule: %v", err)
	}
	rules = m.ListAlertRules()
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule after delete, got %d", len(rules))
	}

	// Delete non-existent
	err = m.DeleteAlertRule("nonexistent")
	if err == nil {
		t.Fatal("expected error for deleting non-existent rule")
	}

	// Create with missing name
	_, err = m.CreateAlertRule("", "metric", "gt", 100, "", "", "", nil, "", "")
	if err == nil {
		t.Fatal("expected error for missing name")
	}

	// Create with missing metric
	_, err = m.CreateAlertRule("Test", "", "gt", 100, "", "", "", nil, "", "")
	if err == nil {
		t.Fatal("expected error for missing metric")
	}
}

func TestCreateIncident(t *testing.T) {
	m := NewManager()

	// Create and list
	inc1 := m.CreateIncident("High Consumer Lag", "critical", "anomaly_detector", map[string]any{"group": "my-group"})
	if inc1.Status != "open" {
		t.Fatalf("expected status 'open', got %q", inc1.Status)
	}
	if inc1.Severity != "critical" {
		t.Fatalf("expected severity 'critical', got %q", inc1.Severity)
	}

	time.Sleep(5 * time.Millisecond)
	m.CreateIncident("Broker Down", "warning", "alert_rule", nil)

	incidents := m.ListIncidents("")
	if len(incidents) != 2 {
		t.Fatalf("expected 2 incidents, got %d", len(incidents))
	}
	titles := map[string]bool{incidents[0].Title: true, incidents[1].Title: true}
	if !titles["High Consumer Lag"] || !titles["Broker Down"] {
		t.Fatalf("expected both incidents, got %q and %q", incidents[0].Title, incidents[1].Title)
	}

	// Filter by status
	openIncidents := m.ListIncidents("open")
	if len(openIncidents) != 2 {
		t.Fatalf("expected 2 open incidents, got %d", len(openIncidents))
	}
	resolvedIncidents := m.ListIncidents("resolved")
	if len(resolvedIncidents) != 0 {
		t.Fatalf("expected 0 resolved incidents, got %d", len(resolvedIncidents))
	}
}
