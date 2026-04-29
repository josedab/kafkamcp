package sandbox

import (
	"testing"
	"time"
)

func TestCleanExpired_NoSandboxes(t *testing.T) {
	e := NewEngine(testLogger())
	cleaned := e.CleanExpired()
	if cleaned != 0 {
		t.Errorf("expected 0 cleaned, got %d", cleaned)
	}
}

func TestCleanExpired_AllActiveNonExpired(t *testing.T) {
	e := NewEngine(testLogger())
	e.Create("sb1", "", "agent-1", 3600, nil)
	e.Create("sb2", "", "agent-2", 3600, nil)

	cleaned := e.CleanExpired()
	if cleaned != 0 {
		t.Errorf("expected 0 cleaned (all active), got %d", cleaned)
	}

	// Verify statuses unchanged
	list := e.List("")
	for _, sb := range list {
		if sb.Status != "active" {
			t.Errorf("sandbox %s should be active, got %s", sb.ID, sb.Status)
		}
	}
}

func TestCleanExpired_OneExpiredActive(t *testing.T) {
	e := NewEngine(testLogger())
	sb, _ := e.Create("expired-sb", "", "agent-1", 1, nil)

	// Manually set expiry to the past
	e.mu.Lock()
	e.sandboxes[sb.ID].ExpiresAt = time.Now().Add(-1 * time.Minute)
	e.mu.Unlock()

	cleaned := e.CleanExpired()
	if cleaned != 1 {
		t.Errorf("expected 1 cleaned, got %d", cleaned)
	}

	got, _ := e.Get(sb.ID)
	if got.Status != "expired" {
		t.Errorf("expected expired status, got %s", got.Status)
	}
}

func TestCleanExpired_MixedStatuses(t *testing.T) {
	e := NewEngine(testLogger())

	// Active, not expired
	e.Create("active-sb", "", "agent-1", 3600, nil)

	// Active, expired
	expiredSb, _ := e.Create("will-expire", "", "agent-2", 1, nil)
	e.mu.Lock()
	e.sandboxes[expiredSb.ID].ExpiresAt = time.Now().Add(-1 * time.Minute)
	e.mu.Unlock()

	// Destroyed (not active, should not be cleaned)
	destroyedSb, _ := e.Create("destroyed-sb", "", "agent-3", 1, nil)
	e.Destroy(destroyedSb.ID)
	e.mu.Lock()
	e.sandboxes[destroyedSb.ID].ExpiresAt = time.Now().Add(-1 * time.Minute)
	e.mu.Unlock()

	cleaned := e.CleanExpired()
	if cleaned != 1 {
		t.Errorf("expected 1 cleaned (only active+expired), got %d", cleaned)
	}
}

func TestCleanExpired_AlreadyExpiredNotDoubleCounted(t *testing.T) {
	e := NewEngine(testLogger())
	sb, _ := e.Create("expired-sb", "", "agent-1", 1, nil)

	e.mu.Lock()
	e.sandboxes[sb.ID].ExpiresAt = time.Now().Add(-1 * time.Minute)
	e.mu.Unlock()

	// First clean
	cleaned1 := e.CleanExpired()
	if cleaned1 != 1 {
		t.Errorf("first clean: expected 1, got %d", cleaned1)
	}

	// Second clean should not re-count
	cleaned2 := e.CleanExpired()
	if cleaned2 != 0 {
		t.Errorf("second clean: expected 0 (already expired), got %d", cleaned2)
	}
}
