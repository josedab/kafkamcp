package metrics

import (
	"testing"
)

func TestSanitizeAgentLabel_PackageLevel(t *testing.T) {
	t.Parallel()
	m := New() // initializes defaultMetrics
	_ = m
	result := SanitizeAgentLabel("test-agent")
	if result != "test-agent" {
		t.Errorf("expected 'test-agent', got %q", result)
	}
}

func TestResetAgents(t *testing.T) {
	t.Parallel()
	m := New()
	m.SanitizeAgentLabel("agent-1")
	m.SanitizeAgentLabel("agent-2")

	m.ResetAgents()

	m.agentMu.RLock()
	count := len(m.knownAgents)
	m.agentMu.RUnlock()

	if count != 0 {
		t.Errorf("expected 0 agents after reset, got %d", count)
	}
}

func TestSanitizeAgentLabelOverflow(t *testing.T) {
	t.Parallel()
	m := New()
	// Fill up to maxUniqueAgents
	agents := make(map[string]struct{})
	for i := 0; i < maxUniqueAgents; i++ {
		id := "agent-" + string(rune('A'+i%26)) + string(rune('0'+i/26))
		agents[id] = struct{}{}
	}
	m.SetKnownAgents(agents)

	// The next agent should be bucketed
	result := m.SanitizeAgentLabel("overflow-agent")
	if result == "overflow-agent" {
		t.Error("expected overflow bucketing for agent exceeding max")
	}
	if len(result) < 10 {
		t.Errorf("expected 'overflow_XX' format, got %q", result)
	}
}
