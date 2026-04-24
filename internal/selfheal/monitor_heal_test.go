package selfheal

import (
	"testing"
)

func TestDiagnoseRootCause_NoMembers(t *testing.T) {
	m := NewMonitor()
	rc := m.DiagnoseRootCause("group-1", 5000, 0, 0, 0)
	if rc.Category != "no_members" {
		t.Fatalf("expected no_members, got %s", rc.Category)
	}
	if rc.Confidence != 0.95 {
		t.Fatalf("expected confidence 0.95, got %f", rc.Confidence)
	}
	if len(rc.Evidence) == 0 {
		t.Fatal("expected non-empty evidence")
	}
}

func TestDiagnoseRootCause_RebalanceStorm(t *testing.T) {
	m := NewMonitor()
	rc := m.DiagnoseRootCause("group-2", 1000, 5.0, 15, 3)
	if rc.Category != "rebalance_storm" {
		t.Fatalf("expected rebalance_storm, got %s", rc.Category)
	}
	if rc.Confidence != 0.9 {
		t.Fatalf("expected confidence 0.9, got %f", rc.Confidence)
	}
}

func TestDiagnoseRootCause_StuckPartition(t *testing.T) {
	m := NewMonitor()
	rc := m.DiagnoseRootCause("group-3", 50000, 0, 0, 2)
	if rc.Category != "stuck_partition" {
		t.Fatalf("expected stuck_partition, got %s", rc.Category)
	}
	if rc.Confidence != 0.85 {
		t.Fatalf("expected confidence 0.85, got %f", rc.Confidence)
	}
}

func TestDiagnoseRootCause_SlowConsumer(t *testing.T) {
	m := NewMonitor()
	rc := m.DiagnoseRootCause("group-4", 200000, 0.3, 0, 2)
	if rc.Category != "slow_consumer" {
		t.Fatalf("expected slow_consumer, got %s", rc.Category)
	}
	if rc.Confidence != 0.8 {
		t.Fatalf("expected confidence 0.8, got %f", rc.Confidence)
	}
}

func TestDiagnoseRootCause_GCPressure(t *testing.T) {
	m := NewMonitor()
	rc := m.DiagnoseRootCause("group-5", 60000, 2.0, 0, 2)
	if rc.Category != "gc_pressure" {
		t.Fatalf("expected gc_pressure, got %s", rc.Category)
	}
	if rc.Confidence != 0.6 {
		t.Fatalf("expected confidence 0.6, got %f", rc.Confidence)
	}
}

func TestDiagnoseRootCause_NetworkTimeout(t *testing.T) {
	m := NewMonitor()
	rc := m.DiagnoseRootCause("group-6", 1000, 5.0, 1, 3)
	if rc.Category != "network_timeout" {
		t.Fatalf("expected network_timeout, got %s", rc.Category)
	}
	if rc.Confidence != 0.5 {
		t.Fatalf("expected confidence 0.5, got %f", rc.Confidence)
	}
}

func TestDiagnoseRootCause_PriorityOrder(t *testing.T) {
	m := NewMonitor()
	// no_members should take priority over rebalance_storm
	rc := m.DiagnoseRootCause("group-prio", 100000, 0, 15, 0)
	if rc.Category != "no_members" {
		t.Fatalf("expected no_members to take priority, got %s", rc.Category)
	}
}

func TestGeneratePlaybook_NoMembers(t *testing.T) {
	m := NewMonitor()
	rc := &RootCause{Category: "no_members"}
	pb := m.GeneratePlaybook(rc, false)
	if len(pb) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(pb))
	}
	if pb[0].Action != "check_deployment" {
		t.Fatalf("expected check_deployment, got %s", pb[0].Action)
	}
	if pb[1].AutoExecute {
		t.Fatal("expected auto_execute=false when autoExecute=false")
	}
}

func TestGeneratePlaybook_NoMembers_AutoExecute(t *testing.T) {
	m := NewMonitor()
	rc := &RootCause{Category: "no_members"}
	pb := m.GeneratePlaybook(rc, true)
	if !pb[1].AutoExecute {
		t.Fatal("expected force_rebalance step to have auto_execute=true")
	}
	if pb[0].AutoExecute {
		t.Fatal("check_deployment should never be auto_execute")
	}
}

func TestGeneratePlaybook_RebalanceStorm(t *testing.T) {
	m := NewMonitor()
	rc := &RootCause{Category: "rebalance_storm"}
	pb := m.GeneratePlaybook(rc, false)
	if len(pb) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(pb))
	}
	if pb[0].Action != "increase_session_timeout" {
		t.Fatalf("expected increase_session_timeout, got %s", pb[0].Action)
	}
	for _, step := range pb {
		if step.Status != "pending" {
			t.Fatalf("expected pending status, got %s", step.Status)
		}
	}
}

func TestGeneratePlaybook_StuckPartition(t *testing.T) {
	m := NewMonitor()
	rc := &RootCause{Category: "stuck_partition"}
	pb := m.GeneratePlaybook(rc, true)
	if len(pb) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(pb))
	}
	if !pb[0].AutoExecute {
		t.Fatal("identify_stuck_partitions should always be auto_execute")
	}
	if !pb[1].AutoExecute {
		t.Fatal("restart_stuck_consumers should be auto_execute when autoExecute=true")
	}
	if pb[2].AutoExecute {
		t.Fatal("skip_poison_message should never be auto_execute")
	}
}

func TestGeneratePlaybook_SlowConsumer(t *testing.T) {
	m := NewMonitor()
	rc := &RootCause{Category: "slow_consumer"}
	pb := m.GeneratePlaybook(rc, false)
	if len(pb) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(pb))
	}
	if pb[0].Action != "analyze_processing_time" {
		t.Fatalf("expected analyze_processing_time, got %s", pb[0].Action)
	}
}

func TestGeneratePlaybook_GCPressure(t *testing.T) {
	m := NewMonitor()
	rc := &RootCause{Category: "gc_pressure"}
	pb := m.GeneratePlaybook(rc, true)
	if len(pb) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(pb))
	}
	// gc_pressure steps are all manual
	for _, step := range pb {
		if step.AutoExecute {
			t.Fatalf("gc_pressure steps should not be auto_execute, but %s is", step.Action)
		}
	}
}

func TestGeneratePlaybook_NetworkTimeout(t *testing.T) {
	m := NewMonitor()
	rc := &RootCause{Category: "network_timeout"}
	pb := m.GeneratePlaybook(rc, false)
	if len(pb) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(pb))
	}
	if pb[0].Action != "check_network" {
		t.Fatalf("expected check_network, got %s", pb[0].Action)
	}
	// check_broker_health should always be auto_execute
	if !pb[1].AutoExecute {
		t.Fatal("check_broker_health should be auto_execute")
	}
}

func TestGeneratePlaybook_StepNumbering(t *testing.T) {
	m := NewMonitor()
	rc := &RootCause{Category: "no_members"}
	pb := m.GeneratePlaybook(rc, false)
	for i, step := range pb {
		if step.Step != i+1 {
			t.Fatalf("expected step %d, got %d", i+1, step.Step)
		}
	}
}

func TestHeal_HealthyGroup(t *testing.T) {
	m := NewMonitor()
	result := m.Heal("healthy-group", 100, 3, 10.0, 0, false)
	if !result.Success {
		t.Fatalf("expected success for healthy group, got message: %s", result.Message)
	}
	if result.Health == nil {
		t.Fatal("expected non-nil health")
	}
	if result.Health.Status != "healthy" {
		t.Fatalf("expected healthy status, got %s", result.Health.Status)
	}
	if result.Diagnosis == nil {
		t.Fatal("expected non-nil diagnosis")
	}
	if len(result.Playbook) == 0 {
		t.Fatal("expected non-empty playbook")
	}
	if result.GroupID != "healthy-group" {
		t.Fatalf("expected healthy-group, got %s", result.GroupID)
	}
	if result.Action != "heal" {
		t.Fatalf("expected heal action, got %s", result.Action)
	}
	if result.Timestamp == "" {
		t.Fatal("expected non-empty timestamp")
	}
}

func TestHeal_DegradedGroup(t *testing.T) {
	m := NewMonitor()
	result := m.Heal("degraded-group", 50000, 2, 5.0, 6, false)
	if result.Success {
		t.Fatal("expected failure for degraded group")
	}
	if result.Health.Status != "degraded" {
		t.Fatalf("expected degraded status, got %s (score=%d)", result.Health.Status, result.Health.HealthScore)
	}
	if result.Diagnosis == nil {
		t.Fatal("expected non-nil diagnosis")
	}
	if result.Message == "" {
		t.Fatal("expected non-empty message")
	}
}

func TestHeal_DeadGroup(t *testing.T) {
	m := NewMonitor()
	result := m.Heal("dead-group", 2_000_000, 0, 0, 15, true)
	if result.Success {
		t.Fatal("expected failure for dead group")
	}
	if result.Health.Status != "dead" {
		t.Fatalf("expected dead status, got %s (score=%d)", result.Health.Status, result.Health.HealthScore)
	}
	if result.Diagnosis.Category != "no_members" {
		t.Fatalf("expected no_members diagnosis, got %s", result.Diagnosis.Category)
	}
	if len(result.Playbook) != 3 {
		t.Fatalf("expected 3 playbook steps, got %d", len(result.Playbook))
	}
}

func TestHeal_RecordsHistory(t *testing.T) {
	m := NewMonitor()
	m.Heal("hist-heal-group", 100, 3, 10.0, 0, false)
	history := m.GetHealthHistory("hist-heal-group")
	if len(history) != 1 {
		t.Fatalf("expected 1 history entry after Heal, got %d", len(history))
	}
}

func TestHeal_AutoExecutePropagates(t *testing.T) {
	m := NewMonitor()
	result := m.Heal("auto-group", 50000, 0, 0, 0, true)
	foundAutoExecute := false
	for _, step := range result.Playbook {
		if step.AutoExecute {
			foundAutoExecute = true
			break
		}
	}
	if !foundAutoExecute {
		t.Fatal("expected at least one auto_execute step when autoExecute=true")
	}
}
