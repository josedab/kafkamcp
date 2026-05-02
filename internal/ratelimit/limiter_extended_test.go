package ratelimit

import (
	"testing"
)

func TestStatus(t *testing.T) {
	t.Parallel()
	l := New(0)
	l.SetAgentLimit("agent-1", 60)
	l.SetAgentLimit("agent-2", 120)

	statuses := l.Status()
	if len(statuses) != 2 {
		t.Fatalf("expected 2 statuses, got %d", len(statuses))
	}

	found := map[string]bool{}
	for _, s := range statuses {
		found[s.AgentID] = true
		if s.MaxTokens <= 0 {
			t.Errorf("expected positive max tokens for %s", s.AgentID)
		}
		if s.RefillRatePerSec <= 0 {
			t.Errorf("expected positive refill rate for %s", s.AgentID)
		}
		if s.TokensRemaining < 0 {
			t.Errorf("expected non-negative tokens for %s", s.AgentID)
		}
	}
	if !found["agent-1"] || !found["agent-2"] {
		t.Error("expected both agents in status")
	}
}

func TestStatusEmpty(t *testing.T) {
	t.Parallel()
	l := New(0)
	statuses := l.Status()
	if len(statuses) != 0 {
		t.Fatalf("expected 0 statuses, got %d", len(statuses))
	}
}

func TestStatusAfterExhaustion(t *testing.T) {
	t.Parallel()
	l := New(0)
	l.SetAgentLimit("agent-1", 2)

	// Exhaust tokens
	l.Allow("agent-1")
	l.Allow("agent-1")

	statuses := l.Status()
	if len(statuses) != 1 {
		t.Fatalf("expected 1 status, got %d", len(statuses))
	}
	// Tokens should be near zero (could have tiny refill from elapsed time)
	if statuses[0].TokensRemaining > 1 {
		t.Errorf("expected near-zero tokens, got %f", statuses[0].TokensRemaining)
	}
}

func TestAllowNoLimits(t *testing.T) {
	t.Parallel()
	l := New(0)
	// No global, no per-agent limits => allow all
	if !l.Allow("any-agent") {
		t.Fatal("expected allow when no limits configured")
	}
}

func TestAllowGlobalDenyBeforeAgent(t *testing.T) {
	t.Parallel()
	l := New(1) // 1 request per minute global
	l.SetAgentLimit("agent-1", 100)

	if !l.Allow("agent-1") {
		t.Fatal("expected first request to be allowed")
	}
	if l.Allow("agent-1") {
		t.Fatal("expected global deny after 1 request")
	}
}

func TestSetAgentLimitZero(t *testing.T) {
	t.Parallel()
	l := New(0)
	l.SetAgentLimit("agent-1", 0) // should not create a bucket
	l.SetAgentLimit("agent-2", 10)

	// agent-1 has no bucket (limit=0), should be allowed (default-allow)
	if !l.Allow("agent-1") {
		t.Fatal("expected allow for agent without configured limit (default-allow)")
	}
}
