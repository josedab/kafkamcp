package replication

import (
	"context"
	"testing"
)

func TestRegisterAndGetStatus(t *testing.T) {
	m := NewMonitor(nil)
	m.RegisterLink("cluster-a", "cluster-b", "mirrormaker2")

	status := m.GetStatus(context.Background())
	if status.TotalLinks != 1 {
		t.Fatalf("expected 1 link, got %d", status.TotalLinks)
	}
	if status.Links[0].Source != "cluster-a" {
		t.Errorf("expected source cluster-a, got %s", status.Links[0].Source)
	}
}

func TestUpdateTopicLag(t *testing.T) {
	m := NewMonitor(nil)
	m.RegisterLink("source", "target", "mirrormaker2")

	m.UpdateTopicLag("source", "target", "orders", 0, 1000, 900)

	status := m.GetStatus(context.Background())
	if len(status.Links) != 1 {
		t.Fatal("expected 1 link")
	}
	link := status.Links[0]
	if len(link.Topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(link.Topics))
	}
	if link.Topics[0].TotalLag != 100 {
		t.Errorf("expected lag 100, got %d", link.Topics[0].TotalLag)
	}
}

func TestLagAlerts(t *testing.T) {
	m := NewMonitor(nil)
	m.SetThresholds(100, 500)
	m.RegisterLink("source", "target", "mirrormaker2")

	// Below warning threshold
	m.UpdateTopicLag("source", "target", "topic1", 0, 1000, 950)
	status := m.GetStatus(context.Background())
	if len(status.Alerts) != 0 {
		t.Errorf("expected 0 alerts, got %d", len(status.Alerts))
	}

	// Above warning threshold
	m.UpdateTopicLag("source", "target", "topic1", 1, 1000, 800)
	status = m.GetStatus(context.Background())
	if len(status.Alerts) != 1 {
		t.Fatalf("expected 1 alert, got %d", len(status.Alerts))
	}
	if status.Alerts[0].Severity != "warning" {
		t.Errorf("expected warning severity, got %s", status.Alerts[0].Severity)
	}

	// Above critical threshold
	m.UpdateTopicLag("source", "target", "topic1", 2, 10000, 9000)
	status = m.GetStatus(context.Background())
	if len(status.Alerts) < 2 {
		t.Fatalf("expected at least 2 alerts, got %d", len(status.Alerts))
	}
}
