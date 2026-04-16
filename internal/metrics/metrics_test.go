package metrics

import (
	"fmt"
	"sync"
	"testing"
)

func TestSanitizeAgentLabel(t *testing.T) {
	m := New()

	tests := []struct {
		name     string
		agentID  string
		want     string
		setup    func()
	}{
		{
			name:    "new agent within capacity",
			agentID: "agent-1",
			want:    "agent-1",
		},
		{
			name:    "known agent returns same ID",
			agentID: "agent-1",
			want:    "agent-1",
		},
		{
			name:    "another new agent within capacity",
			agentID: "agent-2",
			want:    "agent-2",
		},
		{
			name:    "overflow agent gets hash bucket",
			agentID: "overflow-agent",
			setup: func() {
				agents := make(map[string]struct{})
				for i := 0; i < maxUniqueAgents; i++ {
					agents[fmt.Sprintf("agent-%d", i)] = struct{}{}
				}
				m.SetKnownAgents(agents)
			},
			want: "", // will check prefix instead
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}
			got := m.SanitizeAgentLabel(tt.agentID)
			if tt.want != "" {
				if got != tt.want {
					t.Errorf("SanitizeAgentLabel(%q) = %q, want %q", tt.agentID, got, tt.want)
				}
			} else {
				// Overflow bucket: should match "overflow_XX" pattern
				if len(got) < 11 || got[:9] != "overflow_" {
					t.Errorf("SanitizeAgentLabel(%q) = %q, want overflow_XX format", tt.agentID, got)
				}
			}
		})
	}
}

func TestSanitizeAgentLabel_OverflowDeterministic(t *testing.T) {
	m := New()
	agents := make(map[string]struct{})
	for i := 0; i < maxUniqueAgents; i++ {
		agents[fmt.Sprintf("fill-%d", i)] = struct{}{}
	}
	m.SetKnownAgents(agents)

	// Same agent should always map to same bucket
	result1 := m.SanitizeAgentLabel("deterministic-test-agent")
	result2 := m.SanitizeAgentLabel("deterministic-test-agent")
	if result1 != result2 {
		t.Errorf("SanitizeAgentLabel not deterministic: got %q then %q", result1, result2)
	}
}

func TestSanitizeAgentLabel_Concurrent(t *testing.T) {
	m := New()

	var wg sync.WaitGroup
	const goroutines = 100

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			agentID := fmt.Sprintf("concurrent-agent-%d", id)
			result := m.SanitizeAgentLabel(agentID)
			if result == "" {
				t.Errorf("SanitizeAgentLabel(%q) returned empty string", agentID)
			}
		}(i)
	}
	wg.Wait()
}
