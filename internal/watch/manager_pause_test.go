package watch

import (
	"testing"
	"time"
)

// --- Pause/Resume tests ---

func TestPauseSubscription(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	sub, _ := mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)

	err := mgr.PauseSubscription(sub.ID, "agent-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := mgr.GetSubscription(sub.ID)
	if !got.Paused {
		t.Error("expected subscription to be paused")
	}
}

func TestPauseSubscription_Idempotent(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	sub, _ := mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)

	mgr.PauseSubscription(sub.ID, "agent-1")
	err := mgr.PauseSubscription(sub.ID, "agent-1")
	if err != nil {
		t.Error("expected pause to be idempotent")
	}

	got, _ := mgr.GetSubscription(sub.ID)
	if !got.Paused {
		t.Error("expected subscription to remain paused")
	}
}

func TestPauseSubscription_NotFound(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	err := mgr.PauseSubscription("nonexistent", "agent-1")
	if err == nil {
		t.Error("expected error for nonexistent subscription")
	}
}

func TestPauseSubscription_WrongAgent(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	sub, _ := mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)

	err := mgr.PauseSubscription(sub.ID, "agent-2")
	if err == nil {
		t.Error("expected error for wrong agent")
	}
}

func TestResumeSubscription(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	sub, _ := mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)
	mgr.PauseSubscription(sub.ID, "agent-1")

	err := mgr.ResumeSubscription(sub.ID, "agent-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := mgr.GetSubscription(sub.ID)
	if got.Paused {
		t.Error("expected subscription to be resumed")
	}
}

func TestResumeSubscription_Idempotent(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	sub, _ := mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)

	// Resume when not paused
	err := mgr.ResumeSubscription(sub.ID, "agent-1")
	if err != nil {
		t.Error("expected resume to be idempotent")
	}
}

func TestResumeSubscription_NotFound(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	err := mgr.ResumeSubscription("nonexistent", "agent-1")
	if err == nil {
		t.Error("expected error for nonexistent subscription")
	}
}

func TestResumeSubscription_WrongAgent(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	sub, _ := mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)
	mgr.PauseSubscription(sub.ID, "agent-1")

	err := mgr.ResumeSubscription(sub.ID, "agent-2")
	if err == nil {
		t.Error("expected error for wrong agent")
	}
}

// --- ExpireStale tests ---

func TestExpireStale_NoExpired(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)

	expired := mgr.ExpireStale()
	if expired != 0 {
		t.Errorf("expected 0 expired, got %d", expired)
	}
}

func TestExpireStale_ExpiredSubscription(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	sub, _ := mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)

	// Set expiration in the past
	past := time.Now().Add(-1 * time.Hour)
	mgr.mu.Lock()
	mgr.subscriptions[sub.ID].ExpiresAt = &past
	mgr.mu.Unlock()

	expired := mgr.ExpireStale()
	if expired != 1 {
		t.Errorf("expected 1 expired, got %d", expired)
	}

	// Verify it's gone
	if _, err := mgr.GetSubscription(sub.ID); err == nil {
		t.Error("expected expired subscription to be removed")
	}
}

func TestExpireStale_FutureExpiration(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	sub, _ := mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)

	future := time.Now().Add(1 * time.Hour)
	mgr.mu.Lock()
	mgr.subscriptions[sub.ID].ExpiresAt = &future
	mgr.mu.Unlock()

	expired := mgr.ExpireStale()
	if expired != 0 {
		t.Errorf("expected 0 expired for future expiration, got %d", expired)
	}
}

func TestExpireStale_MixedExpirations(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	sub1, _ := mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)
	sub2, _ := mgr.Subscribe("agent-1", "payments.*", "", "", "", nil)
	mgr.Subscribe("agent-1", "users.*", "", "", "", nil) // no expiration

	past := time.Now().Add(-1 * time.Hour)
	future := time.Now().Add(1 * time.Hour)
	mgr.mu.Lock()
	mgr.subscriptions[sub1.ID].ExpiresAt = &past
	mgr.subscriptions[sub2.ID].ExpiresAt = &future
	mgr.mu.Unlock()

	expired := mgr.ExpireStale()
	if expired != 1 {
		t.Errorf("expected 1 expired, got %d", expired)
	}
}

// --- Paused subscription doesn't count as active ---

func TestActiveSubscriptionCount_ExcludesPaused(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	sub1, _ := mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)
	mgr.Subscribe("agent-1", "payments.*", "", "", "", nil)

	if mgr.ActiveSubscriptionCount() != 2 {
		t.Errorf("expected 2 active, got %d", mgr.ActiveSubscriptionCount())
	}

	mgr.PauseSubscription(sub1.ID, "agent-1")

	if mgr.ActiveSubscriptionCount() != 1 {
		t.Errorf("expected 1 active after pause, got %d", mgr.ActiveSubscriptionCount())
	}

	mgr.ResumeSubscription(sub1.ID, "agent-1")

	if mgr.ActiveSubscriptionCount() != 2 {
		t.Errorf("expected 2 active after resume, got %d", mgr.ActiveSubscriptionCount())
	}
}
