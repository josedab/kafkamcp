package collaboration

import (
	"fmt"
	"sync"
	"testing"
)

func TestAcquireTopicLease_Exclusive(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	res := c.AcquireTopicLease("agent-1", "orders", "exclusive", 30, 5, []string{"read"}, nil)
	if !res.Acquired {
		t.Fatal("expected lease to be acquired")
	}
	if res.Lease.Owner != "agent-1" {
		t.Fatalf("expected owner agent-1, got %s", res.Lease.Owner)
	}
	if res.Lease.LeaseType != "exclusive" {
		t.Fatalf("expected exclusive, got %s", res.Lease.LeaseType)
	}
	if res.Lease.MaxRenewals != 5 {
		t.Fatalf("expected maxRenewals 5, got %d", res.Lease.MaxRenewals)
	}
}

func TestAcquireTopicLease_ExclusiveDenied(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "exclusive", 30, 5, nil, nil)

	res := c.AcquireTopicLease("agent-2", "orders", "exclusive", 30, 5, nil, nil)
	if res.Acquired {
		t.Fatal("expected lease to be denied for agent-2")
	}
	if res.Error == "" {
		t.Fatal("expected error message")
	}
}

func TestAcquireTopicLease_SameAgentReacquire(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "exclusive", 30, 5, nil, nil)

	res := c.AcquireTopicLease("agent-1", "orders", "exclusive", 60, 5, nil, nil)
	if !res.Acquired {
		t.Fatal("same agent should be able to re-acquire exclusive lease")
	}
}

func TestAcquireTopicLease_Shared(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	res1 := c.AcquireTopicLease("agent-1", "orders", "shared", 30, 5, []string{"read"}, nil)
	if !res1.Acquired {
		t.Fatal("expected shared lease for agent-1")
	}

	res2 := c.AcquireTopicLease("agent-2", "orders", "shared", 30, 5, []string{"read"}, nil)
	if !res2.Acquired {
		t.Fatal("expected shared lease for agent-2")
	}
}

func TestAcquireTopicLease_ExclusiveDeniedByShared(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "shared", 30, 5, nil, nil)

	res := c.AcquireTopicLease("agent-2", "orders", "exclusive", 30, 5, nil, nil)
	if res.Acquired {
		t.Fatal("exclusive should be denied when shared lease exists from another agent")
	}
	if res.Error == "" {
		t.Fatal("expected error message")
	}
}

func TestAcquireTopicLease_Defaults(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	res := c.AcquireTopicLease("agent-1", "orders", "", 0, 0, nil, nil)
	if !res.Acquired {
		t.Fatal("expected lease to be acquired")
	}
	if res.Lease.LeaseType != "exclusive" {
		t.Fatalf("expected default lease type exclusive, got %s", res.Lease.LeaseType)
	}
	if res.Lease.MaxRenewals != 10 {
		t.Fatalf("expected default maxRenewals 10, got %d", res.Lease.MaxRenewals)
	}
	if len(res.Lease.Permissions) != 2 || res.Lease.Permissions[0] != "read" || res.Lease.Permissions[1] != "write" {
		t.Fatalf("expected default permissions [read write], got %v", res.Lease.Permissions)
	}
}

func TestReleaseTopicLease_Exclusive(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "exclusive", 30, 5, nil, nil)

	err := c.ReleaseTopicLease("agent-1", "orders")
	if err != nil {
		t.Fatalf("expected release to succeed: %v", err)
	}

	// Another agent can now acquire
	res := c.AcquireTopicLease("agent-2", "orders", "exclusive", 30, 5, nil, nil)
	if !res.Acquired {
		t.Fatal("expected agent-2 to acquire after release")
	}
}

func TestReleaseTopicLease_Shared(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "shared", 30, 5, nil, nil)

	err := c.ReleaseTopicLease("agent-1", "orders")
	if err != nil {
		t.Fatalf("expected shared release to succeed: %v", err)
	}
}

func TestReleaseTopicLease_NoLease(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	err := c.ReleaseTopicLease("agent-1", "orders")
	if err == nil {
		t.Fatal("expected error when releasing non-existent lease")
	}
}

func TestReleaseTopicLease_WrongAgent(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "exclusive", 30, 5, nil, nil)

	err := c.ReleaseTopicLease("agent-2", "orders")
	if err == nil {
		t.Fatal("expected error when wrong agent releases lease")
	}
}

func TestRenewTopicLease_Exclusive(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "exclusive", 30, 5, nil, nil)

	lease, err := c.RenewTopicLease("agent-1", "orders", 60)
	if err != nil {
		t.Fatalf("expected renew to succeed: %v", err)
	}
	if lease.RenewCount != 1 {
		t.Fatalf("expected renewCount 1, got %d", lease.RenewCount)
	}
	if lease.Owner != "agent-1" {
		t.Fatalf("expected owner agent-1, got %s", lease.Owner)
	}
}

func TestRenewTopicLease_Shared(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "shared", 30, 5, nil, nil)

	lease, err := c.RenewTopicLease("agent-1", "orders", 60)
	if err != nil {
		t.Fatalf("expected renew to succeed: %v", err)
	}
	if lease.RenewCount != 1 {
		t.Fatalf("expected renewCount 1, got %d", lease.RenewCount)
	}
}

func TestRenewTopicLease_NoLease(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	_, err := c.RenewTopicLease("agent-1", "orders", 60)
	if err == nil {
		t.Fatal("expected error when renewing non-existent lease")
	}
}

func TestRenewTopicLease_MaxRenewals(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "exclusive", 30, 2, nil, nil)

	// Renew twice to hit max
	if _, err := c.RenewTopicLease("agent-1", "orders", 30); err != nil {
		t.Fatalf("first renew should succeed: %v", err)
	}
	if _, err := c.RenewTopicLease("agent-1", "orders", 30); err != nil {
		t.Fatalf("second renew should succeed: %v", err)
	}

	// Third renewal should fail
	_, err := c.RenewTopicLease("agent-1", "orders", 30)
	if err == nil {
		t.Fatal("expected error when max renewals reached")
	}
}

func TestRenewTopicLease_DefaultTTL(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "exclusive", 30, 5, nil, nil)

	lease, err := c.RenewTopicLease("agent-1", "orders", 0)
	if err != nil {
		t.Fatalf("expected renew with default ttl to succeed: %v", err)
	}
	if lease.RenewCount != 1 {
		t.Fatalf("expected renewCount 1, got %d", lease.RenewCount)
	}
}

func TestListTopicLeases_Empty(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	leases := c.ListTopicLeases()
	if len(leases) != 0 {
		t.Fatalf("expected 0 leases, got %d", len(leases))
	}
}

func TestListTopicLeases_Multiple(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	c.AcquireTopicLease("agent-1", "orders", "exclusive", 30, 5, nil, nil)
	c.AcquireTopicLease("agent-2", "events", "exclusive", 30, 5, nil, nil)
	c.AcquireTopicLease("agent-3", "logs", "shared", 30, 5, nil, nil)

	leases := c.ListTopicLeases()
	if len(leases) != 3 {
		t.Fatalf("expected 3 leases, got %d", len(leases))
	}
}

func TestJoinStrings(t *testing.T) {
	t.Parallel()

	if got := joinStrings(nil); got != "" {
		t.Fatalf("expected empty string, got %q", got)
	}
	if got := joinStrings([]string{"a"}); got != "a" {
		t.Fatalf("expected \"a\", got %q", got)
	}
	if got := joinStrings([]string{"a", "b", "c"}); got != "a,b,c" {
		t.Fatalf("expected \"a,b,c\", got %q", got)
	}
}

func TestSplitPermissions(t *testing.T) {
	t.Parallel()

	if got := splitPermissions(""); got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
	if got := splitPermissions("a"); len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a], got %v", got)
	}
	if got := splitPermissions("a,b,c"); len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("expected [a b c], got %v", got)
	}
}

func TestAcquireTopicLease_Concurrent(t *testing.T) {
	t.Parallel()
	c := newTestCoordinator()

	const numAgents = 20
	var wg sync.WaitGroup
	results := make([]LeaseResult, numAgents)

	wg.Add(numAgents)
	for i := 0; i < numAgents; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx] = c.AcquireTopicLease(fmt.Sprintf("agent-%d", idx), "contested-topic", "exclusive", 30, 5, nil, nil)
		}(i)
	}
	wg.Wait()

	acquired := 0
	for _, r := range results {
		if r.Acquired {
			acquired++
		}
	}
	if acquired != 1 {
		t.Fatalf("expected exactly 1 agent to acquire exclusive lease, got %d", acquired)
	}
}
