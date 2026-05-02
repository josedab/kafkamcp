package collaboration

import (
	"log/slog"
	"testing"
)

func newTestCoordinator() *Coordinator {
	return NewCoordinator(slog.Default())
}

func TestAcquireLock(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	result := c.AcquireLock("agent-1", "topic:orders", 30, nil)
	if !result.Acquired {
		t.Fatal("expected lock to be acquired")
	}
	if result.Lock.Owner != "agent-1" {
		t.Fatalf("expected owner agent-1, got %s", result.Lock.Owner)
	}

	// Same agent can re-acquire
	result2 := c.AcquireLock("agent-1", "topic:orders", 30, nil)
	if !result2.Acquired {
		t.Fatal("same agent should be able to re-acquire")
	}

	// Different agent should be denied
	result3 := c.AcquireLock("agent-2", "topic:orders", 30, nil)
	if result3.Acquired {
		t.Fatal("different agent should not acquire held lock")
	}
}

func TestReleaseLock(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireLock("agent-1", "topic:orders", 30, nil)

	// Wrong owner can't release
	err := c.ReleaseLock("agent-2", "topic:orders")
	if err == nil {
		t.Fatal("wrong agent should not be able to release")
	}

	// Correct owner can release
	err = c.ReleaseLock("agent-1", "topic:orders")
	if err != nil {
		t.Fatalf("expected release to succeed: %v", err)
	}

	// Now agent-2 can acquire
	result := c.AcquireLock("agent-2", "topic:orders", 30, nil)
	if !result.Acquired {
		t.Fatal("expected lock to be acquired after release")
	}
}

func TestVectorClock(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	vc := c.IncrementClock("agent-1", "topic:orders")
	if vc.Counters["agent-1"] != 1 {
		t.Fatalf("expected counter 1, got %d", vc.Counters["agent-1"])
	}

	c.IncrementClock("agent-1", "topic:orders")
	vc = c.GetClock("topic:orders")
	if vc.Counters["agent-1"] != 2 {
		t.Fatalf("expected counter 2, got %d", vc.Counters["agent-1"])
	}

	c.IncrementClock("agent-2", "topic:orders")
	vc = c.GetClock("topic:orders")
	if vc.Counters["agent-2"] != 1 {
		t.Fatalf("expected counter 1 for agent-2, got %d", vc.Counters["agent-2"])
	}
}

func TestAgentMessaging(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	msg, err := c.SendMessage("agent-1", "agent-2", "request", "help needed", "can you handle topic X?")
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}
	if msg.From != "agent-1" {
		t.Fatalf("expected from agent-1, got %s", msg.From)
	}

	messages := c.GetMessages("agent-2", false)
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	// agent-1 should not see agent-2's messages
	messages = c.GetMessages("agent-1", false)
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages for agent-1, got %d", len(messages))
	}
}

func TestAgentRegistration(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	state := c.RegisterAgent("agent-1", "active", []string{"monitor", "alert"}, nil)
	if state.Status != "active" {
		t.Fatalf("expected active, got %s", state.Status)
	}

	agents := c.ListAgents()
	if len(agents) != 1 {
		t.Fatalf("expected 1 agent, got %d", len(agents))
	}
}
