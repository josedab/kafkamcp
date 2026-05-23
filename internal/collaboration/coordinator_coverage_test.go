package collaboration

import (
	"testing"
	"time"
)

// --- ListLocks ---

func TestListLocksEmpty(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	locks := c.ListLocks()
	if len(locks) != 0 {
		t.Errorf("expected 0 locks, got %d", len(locks))
	}
}

func TestListLocksActiveListed(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	c.AcquireLock("agent-1", "res-1", 60, nil)
	c.AcquireLock("agent-2", "res-2", 60, nil)

	locks := c.ListLocks()
	if len(locks) != 2 {
		t.Errorf("expected 2 active locks, got %d", len(locks))
	}
}

func TestListLocksExpiredFiltered(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	c.AcquireLock("agent-1", "res-1", 1, nil)
	// Force lock expiry by backdating ExpiresAt instead of sleeping
	c.mu.Lock()
	c.locks["res-1"].ExpiresAt = time.Now().Add(-time.Second)
	c.mu.Unlock()

	locks := c.ListLocks()
	if len(locks) != 0 {
		t.Errorf("expected 0 locks (expired), got %d", len(locks))
	}
}

// --- MergeClocks ---

func TestMergeClocksDisjointAgents(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	c.IncrementClock("agent-1", "res")

	vc := c.MergeClocks("res", map[string]int64{"agent-2": 5})
	if vc.Counters["agent-1"] != 1 {
		t.Errorf("expected agent-1=1, got %d", vc.Counters["agent-1"])
	}
	if vc.Counters["agent-2"] != 5 {
		t.Errorf("expected agent-2=5, got %d", vc.Counters["agent-2"])
	}
}

func TestMergeClocksOverlappingTakeMax(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	for i := 0; i < 3; i++ {
		c.IncrementClock("agent-1", "res")
	}

	vc := c.MergeClocks("res", map[string]int64{"agent-1": 5})
	if vc.Counters["agent-1"] != 5 {
		t.Errorf("expected max=5, got %d", vc.Counters["agent-1"])
	}
}

func TestMergeClocksLocalHigher(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	for i := 0; i < 10; i++ {
		c.IncrementClock("agent-1", "res")
	}

	vc := c.MergeClocks("res", map[string]int64{"agent-1": 3})
	if vc.Counters["agent-1"] != 10 {
		t.Errorf("expected local max=10, got %d", vc.Counters["agent-1"])
	}
}

func TestMergeClocksEmptyMerge(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	c.IncrementClock("agent-1", "res")

	vc := c.MergeClocks("res", map[string]int64{})
	if vc.Counters["agent-1"] != 1 {
		t.Errorf("expected 1 after empty merge, got %d", vc.Counters["agent-1"])
	}
}

func TestMergeClocksNewResource(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	vc := c.MergeClocks("new-res", map[string]int64{"agent-1": 7})
	if vc.Counters["agent-1"] != 7 {
		t.Errorf("expected 7, got %d", vc.Counters["agent-1"])
	}
}

// --- MarkRead ---

func TestMarkReadSingleMessage(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	msg, _ := c.SendMessage("a1", "a2", "notification", "test", "body")

	err := c.MarkRead("a2", msg.ID)
	if err != nil {
		t.Fatalf("MarkRead failed: %v", err)
	}

	messages := c.GetMessages("a2", true)
	if len(messages) != 0 {
		t.Errorf("expected 0 unread, got %d", len(messages))
	}
}

func TestMarkReadNonExistent(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	err := c.MarkRead("agent", "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent message")
	}
}

func TestMarkReadIdempotent(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	msg, _ := c.SendMessage("a1", "a2", "notification", "test", "body")

	_ = c.MarkRead("a2", msg.ID)
	err := c.MarkRead("a2", msg.ID)
	if err != nil {
		t.Error("expected idempotent MarkRead to succeed")
	}
}

func TestMarkReadWrongAgent(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	msg, _ := c.SendMessage("a1", "a2", "notification", "test", "body")

	err := c.MarkRead("a3", msg.ID)
	if err == nil {
		t.Error("expected error for wrong agent")
	}
}

// --- CleanExpiredLocks ---

func TestCleanExpiredLocksRemovesExpired(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	c.AcquireLock("agent-1", "res-1", 1, nil)
	// Force lock expiry by backdating ExpiresAt instead of sleeping
	c.mu.Lock()
	c.locks["res-1"].ExpiresAt = time.Now().Add(-time.Second)
	c.mu.Unlock()

	cleaned := c.CleanExpiredLocks()
	if cleaned != 1 {
		t.Errorf("expected 1 cleaned, got %d", cleaned)
	}
}

func TestCleanExpiredLocksRetainsActive(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	c.AcquireLock("agent-1", "res-1", 3600, nil)

	cleaned := c.CleanExpiredLocks()
	if cleaned != 0 {
		t.Errorf("expected 0 cleaned, got %d", cleaned)
	}
}

func TestCleanExpiredLocksEmpty(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	cleaned := c.CleanExpiredLocks()
	if cleaned != 0 {
		t.Errorf("expected 0 cleaned, got %d", cleaned)
	}
}

// --- AcquireLock edge cases ---

func TestAcquireLockDefaultTTL(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	result := c.AcquireLock("agent-1", "res", 0, nil)
	if !result.Acquired {
		t.Error("expected lock acquired with default TTL")
	}
	if result.Lock.TTLSec != 30 {
		t.Errorf("expected default TTL 30, got %d", result.Lock.TTLSec)
	}
}

func TestAcquireLockNegativeTTL(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	result := c.AcquireLock("agent-1", "res", -5, nil)
	if !result.Acquired {
		t.Error("expected lock acquired")
	}
	if result.Lock.TTLSec != 30 {
		t.Errorf("expected default TTL 30 for negative, got %d", result.Lock.TTLSec)
	}
}

func TestAcquireLockClampedTTL(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	result := c.AcquireLock("agent-1", "res", 9999, nil)
	if !result.Acquired {
		t.Error("expected lock acquired")
	}
	if result.Lock.TTLSec != 3600 {
		t.Errorf("expected clamped TTL 3600, got %d", result.Lock.TTLSec)
	}
}

func TestAcquireLockOverrideBySameAgent(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	c.AcquireLock("agent-1", "res", 30, nil)
	result := c.AcquireLock("agent-1", "res", 60, nil)
	if !result.Acquired {
		t.Error("expected same agent to override lock")
	}
	if result.Lock.TTLSec != 60 {
		t.Errorf("expected TTL 60 after override, got %d", result.Lock.TTLSec)
	}
}

// --- SendMessage edge cases ---

func TestSendMessageBroadcast(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	msg, err := c.SendMessage("a1", "*", "broadcast", "alert", "system alert")
	if err != nil {
		t.Fatalf("SendMessage failed: %v", err)
	}
	if msg.To != "*" {
		t.Errorf("expected broadcast to=*, got %s", msg.To)
	}

	// Both agents should receive broadcast
	msgs := c.GetMessages("a2", false)
	if len(msgs) != 1 {
		t.Errorf("expected 1 broadcast msg for a2, got %d", len(msgs))
	}
	msgs = c.GetMessages("a3", false)
	if len(msgs) != 1 {
		t.Errorf("expected 1 broadcast msg for a3, got %d", len(msgs))
	}
}

func TestSendMessageEmptyFrom(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	_, err := c.SendMessage("", "a2", "request", "", "")
	if err == nil {
		t.Error("expected error for empty from")
	}
}

func TestSendMessageEmptyTo(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	_, err := c.SendMessage("a1", "", "request", "", "")
	if err == nil {
		t.Error("expected error for empty to")
	}
}

func TestSendMessageEmptyBody(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	msg, err := c.SendMessage("a1", "a2", "request", "test", "")
	if err != nil {
		t.Fatalf("SendMessage failed: %v", err)
	}
	if msg.Body != "" {
		t.Error("expected empty body")
	}
}

// --- GetMessages ---

func TestGetMessagesUnreadOnly(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	msg, _ := c.SendMessage("a1", "a2", "request", "test", "body")
	_, _ = c.SendMessage("a1", "a2", "request", "test2", "body2")
	_ = c.MarkRead("a2", msg.ID)

	unread := c.GetMessages("a2", true)
	if len(unread) != 1 {
		t.Errorf("expected 1 unread, got %d", len(unread))
	}
}

// --- ReleaseLock edge cases ---

func TestReleaseLockNotExists(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	err := c.ReleaseLock("agent", "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent lock")
	}
}

// --- GetClock ---

func TestGetClockNonExistent(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()
	vc := c.GetClock("nonexistent")
	if vc == nil {
		t.Fatal("expected non-nil vector clock")
	}
	if len(vc.Counters) != 0 {
		t.Errorf("expected empty counters, got %v", vc.Counters)
	}
}
