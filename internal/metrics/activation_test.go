package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestActivationMetrics_DisabledIsNoop(t *testing.T) {
	reg := prometheus.NewRegistry()
	am := NewActivationMetrics(reg, false)
	if am.Enabled() {
		t.Fatal("expected disabled activation metrics")
	}
	// These must not panic even though nothing is registered.
	am.RecordSetup("success")
	am.RecordToolPack("core-read")
	am.RecordDeniedUnsafe("kafka_delete_topic")
	am.RecordApprovalOutcome("approve")
	am.RecordCoreWorkflow("approved_write")
	am.ObserveVerifiedAgent("hash")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	if len(mfs) != 0 {
		t.Fatalf("disabled activation metrics must register nothing, got %d families", len(mfs))
	}
}

func TestActivationMetrics_EnabledRecords(t *testing.T) {
	reg := prometheus.NewRegistry()
	am := NewActivationMetrics(reg, true)
	if !am.Enabled() {
		t.Fatal("expected enabled activation metrics")
	}
	am.RecordSetup("success")
	am.RecordToolPack("core-read")
	am.RecordDeniedUnsafe("kafka_delete_topic")
	am.RecordApprovalOutcome("approve")
	am.RecordCoreWorkflow("approved_write")
	// Distinct-agent gauge: two distinct hashes -> 2, repeat -> still 2.
	am.ObserveVerifiedAgent("a")
	am.ObserveVerifiedAgent("b")
	am.ObserveVerifiedAgent("a")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, want := range []string{
		"kafkamcp_activation_setup_total",
		"kafkamcp_activation_active_verified_agents",
		"kafkamcp_activation_tool_pack_use_total",
		"kafkamcp_activation_denied_unsafe_calls_total",
		"kafkamcp_activation_approval_outcomes_total",
		"kafkamcp_activation_core_workflow_total",
	} {
		if !names[want] {
			t.Errorf("expected metric %q to be registered", want)
		}
	}

	if got := gaugeValue(t, reg, "kafkamcp_activation_active_verified_agents"); got != 2 {
		t.Errorf("expected 2 distinct verified agents, got %v", got)
	}
}

// TestActivationMetrics_LowCardinalityLabels documents the privacy invariant:
// activation metrics must not use raw agent/resource identifiers as labels.
func TestActivationMetrics_LowCardinalityLabels(t *testing.T) {
	reg := prometheus.NewRegistry()
	am := NewActivationMetrics(reg, true)
	am.RecordApprovalOutcome("approve")
	mfs, _ := reg.Gather()
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if strings.Contains(strings.ToLower(l.GetName()), "agent_id") ||
					strings.Contains(strings.ToLower(l.GetName()), "resource") ||
					strings.Contains(strings.ToLower(l.GetName()), "topic") {
					t.Errorf("activation metric %q uses high-cardinality label %q", mf.GetName(), l.GetName())
				}
			}
		}
	}
}

func TestActivationMetrics_AgentWindowResetsAndBoundsMemory(t *testing.T) {
	reg := prometheus.NewRegistry()
	am := NewActivationMetrics(reg, true)
	now := time.Unix(1_700_000_000, 0)
	am.now = func() time.Time { return now }
	am.agentWindow = time.Hour
	am.maxAgentHashes = 2

	am.ObserveVerifiedAgent("a")
	am.ObserveVerifiedAgent("b")
	am.ObserveVerifiedAgent("c")
	if got := gaugeValue(t, reg, "kafkamcp_activation_active_verified_agents"); got != 2 {
		t.Fatalf("expected bounded gauge=2, got %v", got)
	}

	now = now.Add(2 * time.Hour)
	am.ObserveVerifiedAgent("d")
	if got := gaugeValue(t, reg, "kafkamcp_activation_active_verified_agents"); got != 1 {
		t.Fatalf("expected reset window gauge=1, got %v", got)
	}
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				return m.GetGauge().GetValue()
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}
