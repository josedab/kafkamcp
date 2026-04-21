package replication

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// --- TopologyDetector tests ---

func TestTopologyDetector_DetectTopology_Single(t *testing.T) {
	mon := NewMonitor(testLogger())
	td := NewTopologyDetector(mon)

	topo := td.DetectTopology()
	if topo.Type != "single" {
		t.Errorf("expected 'single' with no links, got %q", topo.Type)
	}
	if len(topo.Clusters) != 0 {
		t.Errorf("expected 0 clusters, got %d", len(topo.Clusters))
	}
}

func TestTopologyDetector_DetectTopology_HubSpoke(t *testing.T) {
	mon := NewMonitor(testLogger())
	mon.RegisterLink("hub", "spoke1", "mirrormaker2")
	mon.RegisterLink("hub", "spoke2", "mirrormaker2")
	mon.RegisterLink("hub", "spoke3", "mirrormaker2")

	td := NewTopologyDetector(mon)
	topo := td.DetectTopology()

	if topo.Type != "hub-spoke" {
		t.Errorf("expected 'hub-spoke', got %q", topo.Type)
	}
	if len(topo.Links) != 3 {
		t.Errorf("expected 3 links, got %d", len(topo.Links))
	}
}

func TestTopologyDetector_DetectTopology_Mesh(t *testing.T) {
	mon := NewMonitor(testLogger())
	// A->B, B->A, A->C, C->A creates mesh (multiple bidirectional)
	mon.RegisterLink("A", "B", "cluster_linking")
	mon.RegisterLink("B", "A", "cluster_linking")
	mon.RegisterLink("A", "C", "cluster_linking")
	mon.RegisterLink("C", "A", "cluster_linking")

	td := NewTopologyDetector(mon)
	topo := td.DetectTopology()

	if topo.Type != "mesh" {
		t.Errorf("expected 'mesh', got %q", topo.Type)
	}
}

func TestTopologyDetector_DetectTopology_Chain(t *testing.T) {
	mon := NewMonitor(testLogger())
	// A->B, C->D = chain (multiple source-only, no bidirectional, single target each)
	mon.RegisterLink("A", "B", "mirrormaker2")
	mon.RegisterLink("C", "D", "mirrormaker2")

	td := NewTopologyDetector(mon)
	topo := td.DetectTopology()

	if topo.Type != "chain" {
		t.Errorf("expected 'chain', got %q", topo.Type)
	}
}

func TestTopologyDetector_DetectTopology_ClusterRoles(t *testing.T) {
	mon := NewMonitor(testLogger())
	mon.RegisterLink("source-only", "target-only", "mirrormaker2")

	td := NewTopologyDetector(mon)
	topo := td.DetectTopology()

	roleMap := make(map[string]string)
	for _, c := range topo.Clusters {
		roleMap[c.Name] = c.Role
	}

	if roleMap["source-only"] != "source" {
		t.Errorf("expected source role, got %q", roleMap["source-only"])
	}
	if roleMap["target-only"] != "target" {
		t.Errorf("expected target role, got %q", roleMap["target-only"])
	}
}

func TestTopologyDetector_DetectTopology_BidirectionalRole(t *testing.T) {
	mon := NewMonitor(testLogger())
	mon.RegisterLink("A", "B", "mirrormaker2")
	mon.RegisterLink("B", "A", "mirrormaker2")

	td := NewTopologyDetector(mon)
	topo := td.DetectTopology()

	for _, c := range topo.Clusters {
		if c.Role != "bidirectional" {
			t.Errorf("cluster %s: expected bidirectional, got %q", c.Name, c.Role)
		}
	}
}

func TestTopologyDetector_GetTopology_NilBeforeDetect(t *testing.T) {
	mon := NewMonitor(testLogger())
	td := NewTopologyDetector(mon)

	if got := td.GetTopology(); got != nil {
		t.Error("expected nil topology before detection")
	}
}

func TestTopologyDetector_GetTopology_CachedAfterDetect(t *testing.T) {
	mon := NewMonitor(testLogger())
	mon.RegisterLink("A", "B", "mirrormaker2")
	td := NewTopologyDetector(mon)

	td.DetectTopology()
	cached := td.GetTopology()
	if cached == nil {
		t.Fatal("expected cached topology")
	}
	if cached.Type == "" {
		t.Error("expected non-empty topology type")
	}
}

// --- AlertManager tests ---

func TestAlertManager_ProcessAlert(t *testing.T) {
	am, _ := NewAlertManager(NotificationConfig{CooldownMins: 5})

	alert := Alert{
		Source:    "cluster-a",
		Target:    "cluster-b",
		Topic:     "orders",
		Partition: 0,
		Lag:       5000,
		Threshold: 1000,
		Severity:  "warning",
	}

	notif := am.ProcessAlert(alert)
	if notif == nil {
		t.Fatal("expected notification")
	}
	if notif.Severity != "warning" || notif.CurrentLag != 5000 {
		t.Errorf("unexpected notification: %+v", notif)
	}
	if notif.ID != "repl-alert-1" {
		t.Errorf("expected ID repl-alert-1, got %s", notif.ID)
	}
}

func TestAlertManager_ProcessAlert_CooldownDedup(t *testing.T) {
	am, _ := NewAlertManager(NotificationConfig{CooldownMins: 60})

	alert := Alert{Source: "a", Target: "b", Topic: "t1", Partition: 0, Lag: 5000, Threshold: 1000, Severity: "warning"}

	notif1 := am.ProcessAlert(alert)
	if notif1 == nil {
		t.Fatal("expected first notification")
	}

	// Same alert within cooldown - should be suppressed
	notif2 := am.ProcessAlert(alert)
	if notif2 != nil {
		t.Error("expected suppressed notification within cooldown")
	}
}

func TestAlertManager_ProcessAlert_DifferentKey(t *testing.T) {
	am, _ := NewAlertManager(NotificationConfig{CooldownMins: 60})

	alert1 := Alert{Source: "a", Target: "b", Topic: "t1", Partition: 0, Lag: 5000, Threshold: 1000, Severity: "warning"}
	alert2 := Alert{Source: "a", Target: "b", Topic: "t2", Partition: 0, Lag: 5000, Threshold: 1000, Severity: "warning"}

	notif1 := am.ProcessAlert(alert1)
	notif2 := am.ProcessAlert(alert2)
	if notif1 == nil || notif2 == nil {
		t.Error("expected both notifications for different keys")
	}
}

func TestAlertManager_DefaultCooldown(t *testing.T) {
	am, _ := NewAlertManager(NotificationConfig{CooldownMins: 0})
	if am.config.CooldownMins != 5 {
		t.Errorf("expected default cooldown 5, got %d", am.config.CooldownMins)
	}
}

func TestAlertManager_GetRecentNotifications(t *testing.T) {
	am, _ := NewAlertManager(NotificationConfig{CooldownMins: 0})

	for i := 0; i < 5; i++ {
		am.ProcessAlert(Alert{
			Source: "a", Target: "b", Topic: "t1", Partition: int32(i),
			Lag: 5000, Threshold: 1000, Severity: "warning",
		})
		// Reset cooldown to allow processing all alerts
		am.mu.Lock()
		am.cooldowns = make(map[string]time.Time)
		am.mu.Unlock()
	}

	all := am.GetRecentNotifications(0)
	if len(all) != 5 {
		t.Errorf("expected 5 notifications, got %d", len(all))
	}

	last2 := am.GetRecentNotifications(2)
	if len(last2) != 2 {
		t.Errorf("expected 2 notifications, got %d", len(last2))
	}
}

func TestAlertManager_NotificationsCap1000(t *testing.T) {
	am, _ := NewAlertManager(NotificationConfig{CooldownMins: 0})

	for i := 0; i < 1005; i++ {
		am.mu.Lock()
		am.cooldowns = make(map[string]time.Time)
		am.mu.Unlock()
		am.ProcessAlert(Alert{
			Source: "a", Target: "b", Topic: "t1", Partition: int32(i % 100),
			Lag: 5000, Threshold: 1000, Severity: "warning",
		})
	}

	am.mu.RLock()
	count := len(am.notifications)
	am.mu.RUnlock()
	if count > 1000 {
		t.Errorf("expected max 1000 notifications, got %d", count)
	}
}

// --- Monitor resource methods tests ---

func TestMonitor_GetReplicationResource(t *testing.T) {
	mon := NewMonitor(testLogger())
	mon.RegisterLink("source", "target", "mirrormaker2")

	link, err := mon.GetReplicationResource("source", "target")
	if err != nil || link == nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if link.Source != "source" || link.Target != "target" {
		t.Errorf("unexpected link: %+v", link)
	}
}

func TestMonitor_GetReplicationResource_NotFound(t *testing.T) {
	mon := NewMonitor(testLogger())
	_, err := mon.GetReplicationResource("a", "b")
	if err == nil {
		t.Error("expected error for missing link")
	}
}

func TestMonitor_GetCrossClusterLinks(t *testing.T) {
	mon := NewMonitor(testLogger())
	mon.RegisterLink("a", "b", "mm2")
	mon.RegisterLink("b", "c", "mm2")

	links := mon.GetCrossClusterLinks()
	if len(links) != 2 {
		t.Errorf("expected 2 cross-cluster links, got %d", len(links))
	}
}

func TestMonitor_GetCrossClusterLinks_Empty(t *testing.T) {
	mon := NewMonitor(testLogger())
	links := mon.GetCrossClusterLinks()
	if len(links) != 0 {
		t.Errorf("expected 0 links, got %d", len(links))
	}
}
