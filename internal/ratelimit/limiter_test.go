package ratelimit

import (
	"testing"
	"time"
)

func TestBucketAllow(t *testing.T) {
	t.Parallel()
	b := newBucket(60) // 60/min = 1/sec
	for i := 0; i < 60; i++ {
		if !b.allow() {
			t.Fatalf("expected allow at request %d", i)
		}
	}
	if b.allow() {
		t.Fatal("expected deny after exhausting tokens")
	}
}

func TestBucketRefill(t *testing.T) {
	t.Parallel()
	b := newBucket(60)
	for i := 0; i < 60; i++ {
		b.allow()
	}
	b.lastRefill = time.Now().Add(-2 * time.Second)
	if !b.allow() {
		t.Fatal("expected allow after refill")
	}
}

func TestLimiterPerAgent(t *testing.T) {
	t.Parallel()
	l := New(0)
	l.SetAgentLimit("agent-1", 2)

	if !l.Allow("agent-1") {
		t.Fatal("expected allow")
	}
	if !l.Allow("agent-1") {
		t.Fatal("expected allow")
	}
	if l.Allow("agent-1") {
		t.Fatal("expected deny")
	}
	if !l.Allow("agent-2") {
		t.Fatal("expected allow for unknown agent (default-allow)")
	}
}

func TestLimiterGlobal(t *testing.T) {
	t.Parallel()
	l := New(2)
	l.SetAgentLimit("any", 100) // configure agent so it's not default-denied
	if !l.Allow("any") {
		t.Fatal("expected allow")
	}
	if !l.Allow("any") {
		t.Fatal("expected allow")
	}
	if l.Allow("any") {
		t.Fatal("expected global deny")
	}
}
