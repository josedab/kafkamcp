package approval

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/josedab/kafkamcp/internal/engine/backend"
)

func testScope() Scope {
	return Scope{
		AgentID:        "agent-1",
		IdentityMethod: "oidc",
		Tool:           "kafka_delete_topic",
		Cluster:        "prod",
		Resource:       "orders",
		ArgsDigest:     DigestArgs(map[string]any{"topic": "orders"}),
	}
}

func newManager(t *testing.T) (*Manager, *fakeClock) {
	t.Helper()
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	m, err := NewManager(context.Background(), backend.NewInMemory(), DefaultConfig())
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	m.SetClock(clk.Now)
	return m, clk
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func approveGrant(t *testing.T, m *Manager, scope Scope) (string, string) {
	t.Helper()
	ctx := context.Background()
	g, err := m.Request(ctx, RequestSpec{Scope: scope, Preview: "DELETE topic orders", Requester: "agent-1"})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if g.Status != StatusRequested {
		t.Fatalf("expected requested, got %s", g.Status)
	}
	if g.TokenHash != "" {
		t.Fatal("redacted grant must not carry token hash")
	}
	_, token, err := m.Approve(ctx, g.ID, "human-approver", 0)
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	if token == "" {
		t.Fatal("expected a token on approval")
	}
	return g.ID, token
}

func TestConsume_HappyPath(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	_, token := approveGrant(t, m, scope)
	g, err := m.Consume(context.Background(), token, scope)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if g.Status != StatusUsed {
		t.Fatalf("expected used after single-use consume, got %s", g.Status)
	}
}

func TestConsume_ReplayDenied(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	_, token := approveGrant(t, m, scope)
	if _, err := m.Consume(context.Background(), token, scope); err != nil {
		t.Fatalf("first consume: %v", err)
	}
	if _, err := m.Consume(context.Background(), token, scope); err != ErrAlreadyUsed && err != ErrInvalidToken {
		t.Fatalf("replay must be denied, got %v", err)
	}
}

func TestConsume_Expired(t *testing.T) {
	m, clk := newManager(t)
	scope := testScope()
	_, token := approveGrant(t, m, scope)
	clk.Advance(DefaultTTL + time.Second)
	if _, err := m.Consume(context.Background(), token, scope); err != ErrExpired {
		t.Fatalf("expected ErrExpired, got %v", err)
	}
}

func TestConsume_WrongAgent(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	_, token := approveGrant(t, m, scope)
	bad := scope
	bad.AgentID = "attacker"
	if _, err := m.Consume(context.Background(), token, bad); err != ErrScopeMismatch {
		t.Fatalf("expected scope mismatch for wrong agent, got %v", err)
	}
}

func TestConsume_WrongTool(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	_, token := approveGrant(t, m, scope)
	bad := scope
	bad.Tool = "kafka_create_topic"
	if _, err := m.Consume(context.Background(), token, bad); err != ErrScopeMismatch {
		t.Fatalf("expected scope mismatch for wrong tool, got %v", err)
	}
}

func TestConsume_WrongResource(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	_, token := approveGrant(t, m, scope)
	bad := scope
	bad.Resource = "payments"
	if _, err := m.Consume(context.Background(), token, bad); err != ErrScopeMismatch {
		t.Fatalf("expected scope mismatch for wrong resource, got %v", err)
	}
}

func TestConsume_WrongArgs(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	_, token := approveGrant(t, m, scope)
	bad := scope
	bad.ArgsDigest = DigestArgs(map[string]any{"topic": "orders", "partitions": 99})
	if _, err := m.Consume(context.Background(), token, bad); err != ErrScopeMismatch {
		t.Fatalf("expected scope mismatch for wrong args, got %v", err)
	}
}

func TestConsume_InvalidToken(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	approveGrant(t, m, scope)
	if _, err := m.Consume(context.Background(), "not-a-real-token", scope); err != ErrInvalidToken {
		t.Fatalf("expected ErrInvalidToken, got %v", err)
	}
}

func TestConsume_Revoked(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	id, token := approveGrant(t, m, scope)
	if _, err := m.Revoke(context.Background(), id, "operator", "operator changed mind"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := m.Consume(context.Background(), token, scope); err != ErrRevoked && err != ErrInvalidToken {
		t.Fatalf("expected revoked/invalid after revoke, got %v", err)
	}
}

func TestConsume_NotApproved(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	// Request but do not approve.
	g, err := m.Request(context.Background(), RequestSpec{Scope: scope, Requester: "agent-1"})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	// No token exists; a random token must be invalid.
	if _, err := m.Consume(context.Background(), "x", scope); err != ErrInvalidToken {
		t.Fatalf("expected invalid token for unapproved grant, got %v", err)
	}
	if g.Status != StatusRequested {
		t.Fatalf("expected requested status, got %s", g.Status)
	}
}

func TestApprove_TTLCap(t *testing.T) {
	m, clk := newManager(t)
	scope := testScope()
	ctx := context.Background()
	g, _ := m.Request(ctx, RequestSpec{Scope: scope, Requester: "agent-1"})
	// Request a TTL far above the cap; it must be capped at MaxTTLCap.
	approved, token, err := m.Approve(ctx, g.ID, "human", 10*time.Hour)
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	if approved.ExpiresAt == nil {
		t.Fatal("expected expiry")
	}
	// Just before cap: valid. Just after cap: expired.
	clk.Advance(MaxTTLCap - time.Minute)
	if _, err := m.Consume(ctx, token, scope); err != nil {
		t.Fatalf("should still be valid within cap: %v", err)
	}
}

func TestRevoke_WrongState(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	id, token := approveGrant(t, m, scope)
	// Consume (single-use) so the grant becomes "used".
	if _, err := m.Consume(context.Background(), token, scope); err != nil {
		t.Fatalf("consume: %v", err)
	}
	// Revoking an already-used grant must fail with wrong-state.
	if _, err := m.Revoke(context.Background(), id, "operator", "late"); err == nil {
		t.Fatal("expected wrong-state error revoking used grant")
	}
}

func TestPersistenceAcrossManagers(t *testing.T) {
	be := backend.NewInMemory()
	scope := testScope()
	ctx := context.Background()

	m1, err := NewManager(ctx, be, DefaultConfig())
	if err != nil {
		t.Fatalf("new manager 1: %v", err)
	}
	g, err := m1.Request(ctx, RequestSpec{Scope: scope, Requester: "agent-1"})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	_, token, err := m1.Approve(ctx, g.ID, "human", 0)
	if err != nil {
		t.Fatalf("approve: %v", err)
	}

	// A brand-new manager over the same backend must honor the grant.
	m2, err := NewManager(ctx, be, DefaultConfig())
	if err != nil {
		t.Fatalf("new manager 2: %v", err)
	}
	if _, err := m2.Consume(ctx, token, scope); err != nil {
		t.Fatalf("grant must persist across managers: %v", err)
	}
	// And replay must be denied on the second manager too.
	if _, err := m2.Consume(ctx, token, scope); err == nil {
		t.Fatal("replay must be denied after persisted consume")
	}
}

func TestList_FiltersByAgent(t *testing.T) {
	m, _ := newManager(t)
	ctx := context.Background()
	_, _ = m.Request(ctx, RequestSpec{Scope: Scope{AgentID: "a", Tool: "kafka_produce"}, Requester: "a"})
	_, _ = m.Request(ctx, RequestSpec{Scope: Scope{AgentID: "b", Tool: "kafka_produce"}, Requester: "b"})
	got, err := m.List(ctx, "a")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 1 || got[0].Scope.AgentID != "a" {
		t.Fatalf("expected only agent a's grants, got %+v", got)
	}
}

func TestSweep_ExpiresApproved(t *testing.T) {
	m, clk := newManager(t)
	scope := testScope()
	approveGrant(t, m, scope)
	clk.Advance(DefaultTTL + time.Minute)
	n, err := m.Sweep(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 grant expired, got %d", n)
	}
}

func TestDigestArgs_OrderIndependent(t *testing.T) {
	a := DigestArgs(map[string]any{"topic": "x", "partitions": 3})
	b := DigestArgs(map[string]any{"partitions": 3, "topic": "x"})
	if a != b {
		t.Fatal("digest must be order-independent")
	}
	c := DigestArgs(map[string]any{"topic": "y", "partitions": 3})
	if a == c {
		t.Fatal("different args must produce different digests")
	}
}

func TestConcurrentSingleUse(t *testing.T) {
	m, _ := newManager(t)
	scope := testScope()
	_, token := approveGrant(t, m, scope)

	const goroutines = 40
	var wg sync.WaitGroup
	var successes int64
	var mu sync.Mutex
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if _, err := m.Consume(context.Background(), token, scope); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("single-use grant must be consumable exactly once under concurrency, got %d", successes)
	}
}

func TestConcurrentSingleUseAcrossManagers(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		open func(t *testing.T) (backend.Backend, backend.Backend)
	}{
		{
			name: "in-memory",
			open: func(t *testing.T) (backend.Backend, backend.Backend) {
				be := backend.NewInMemory()
				t.Cleanup(func() { _ = be.Close() })
				return be, be
			},
		},
		{
			name: "sqlite-connections",
			open: func(t *testing.T) (backend.Backend, backend.Backend) {
				path := filepath.Join(t.TempDir(), "approval-cas.db")
				first, err := backend.OpenSQLite(backend.SQLiteOptions{Path: path})
				if err != nil {
					t.Fatal(err)
				}
				second, err := backend.OpenSQLite(backend.SQLiteOptions{Path: path})
				if err != nil {
					_ = first.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_ = first.Close()
					_ = second.Close()
				})
				return first, second
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			firstBackend, secondBackend := tc.open(t)
			first, err := NewManager(ctx, firstBackend, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}
			scope := testScope()
			grant, err := first.Request(ctx, RequestSpec{
				Scope:     scope,
				Requester: "agent-1",
			})
			if err != nil {
				t.Fatal(err)
			}
			_, token, err := first.Approve(ctx, grant.ID, "human", 0)
			if err != nil {
				t.Fatal(err)
			}
			second, err := NewManager(ctx, secondBackend, DefaultConfig())
			if err != nil {
				t.Fatal(err)
			}

			managers := []*Manager{first, second}
			start := make(chan struct{})
			results := make(chan error, len(managers))
			var wg sync.WaitGroup
			for _, manager := range managers {
				wg.Add(1)
				go func(manager *Manager) {
					defer wg.Done()
					<-start
					_, err := manager.Consume(ctx, token, scope)
					results <- err
				}(manager)
			}
			close(start)
			wg.Wait()
			close(results)

			successes := 0
			for err := range results {
				if err == nil {
					successes++
					continue
				}
				if !errors.Is(err, ErrAlreadyUsed) && !errors.Is(err, ErrInvalidToken) {
					t.Fatalf("unexpected consume error: %v", err)
				}
			}
			if successes != 1 {
				t.Fatalf("successful consumes = %d, want 1", successes)
			}
		})
	}
}

func TestConcurrentRequests(t *testing.T) {
	m, _ := newManager(t)
	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _ = m.Request(context.Background(), RequestSpec{
				Scope:     Scope{AgentID: "a", Tool: "kafka_produce"},
				Requester: "a",
			})
		}()
	}
	wg.Wait()
	got, err := m.List(context.Background(), "a")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != n {
		t.Fatalf("expected %d grants, got %d (ID collision or race?)", n, len(got))
	}
}

func TestRequest_MissingRequired(t *testing.T) {
	m, _ := newManager(t)
	if _, err := m.Request(context.Background(), RequestSpec{Scope: Scope{Tool: "kafka_produce"}}); err != ErrMissingRequired {
		t.Fatalf("expected ErrMissingRequired for missing agent, got %v", err)
	}
}

func TestApprove_SelfApprovalDeniedByDefault(t *testing.T) {
	m, _ := newManager(t)
	g, err := m.Request(context.Background(), RequestSpec{
		Scope:     testScope(),
		Requester: "agent-1",
	})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if _, _, err := m.Approve(context.Background(), g.ID, "agent-1", 0); err != ErrSelfApproval {
		t.Fatalf("expected ErrSelfApproval, got %v", err)
	}
}

func TestSignedDecisionChainDetectsTampering(t *testing.T) {
	ctx := context.Background()
	be := backend.NewInMemory()
	cfg := DefaultConfig()
	cfg.DecisionSigningKey = []byte("0123456789abcdef0123456789abcdef")
	cfg.RequireSignedDecisions = true
	m, err := NewManager(ctx, be, cfg)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	g, err := m.Request(ctx, RequestSpec{
		Scope:     testScope(),
		Preview:   "delete orders",
		Requester: "agent-1",
	})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if len(g.Decisions) != 1 || g.Decisions[0].MAC == "" {
		t.Fatalf("expected signed request decision, got %+v", g.Decisions)
	}

	stored, ok, err := m.grants.Get(ctx, g.ID)
	if err != nil || !ok {
		t.Fatalf("load stored grant: ok=%v err=%v", ok, err)
	}
	stored.Decisions[0].Actor = "attacker"
	if err := m.grants.Set(ctx, g.ID, stored); err != nil {
		t.Fatalf("tamper grant: %v", err)
	}
	if _, err := NewManager(ctx, be, cfg); !errors.Is(err, ErrDecisionTampered) {
		t.Fatalf("expected tamper detection, got %v", err)
	}
}

func TestSignedGrantStateDetectsScopeTampering(t *testing.T) {
	ctx := context.Background()
	be := backend.NewInMemory()
	cfg := DefaultConfig()
	cfg.DecisionSigningKey = []byte("0123456789abcdef0123456789abcdef")
	cfg.RequireSignedDecisions = true
	manager, err := NewManager(ctx, be, cfg)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	grant, err := manager.Request(ctx, RequestSpec{
		Scope:     testScope(),
		Preview:   "delete orders",
		Requester: "agent-1",
	})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	stored, ok, err := manager.grants.Get(ctx, grant.ID)
	if err != nil || !ok {
		t.Fatalf("load grant: ok=%v err=%v", ok, err)
	}
	stored.Scope.Resource = "payments"
	if err := manager.grants.Set(ctx, grant.ID, stored); err != nil {
		t.Fatalf("tamper grant: %v", err)
	}
	if _, err := NewManager(ctx, be, cfg); !errors.Is(err, ErrDecisionTampered) {
		t.Fatalf("expected state tamper detection, got %v", err)
	}
}

func TestSignedGrantPersistsAcrossSQLiteRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "approvals.db")
	key := []byte("0123456789abcdef0123456789abcdef")
	cfg := DefaultConfig()
	cfg.DecisionSigningKey = key
	cfg.RequireSignedDecisions = true

	firstBackend, err := backend.OpenSQLite(backend.SQLiteOptions{Path: path})
	if err != nil {
		t.Fatalf("open first backend: %v", err)
	}
	first, err := NewManager(ctx, firstBackend, cfg)
	if err != nil {
		t.Fatalf("new first manager: %v", err)
	}
	grant, err := first.Request(ctx, RequestSpec{
		Scope:     testScope(),
		Preview:   "delete orders",
		Requester: "agent-1",
	})
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	_, token, err := first.Approve(ctx, grant.ID, "operator-1", 0)
	if err != nil {
		t.Fatalf("approve: %v", err)
	}
	if err := firstBackend.Close(); err != nil {
		t.Fatalf("close first backend: %v", err)
	}

	secondBackend, err := backend.OpenSQLite(backend.SQLiteOptions{Path: path})
	if err != nil {
		t.Fatalf("reopen backend: %v", err)
	}
	defer func() {
		if err := secondBackend.Close(); err != nil {
			t.Errorf("close second backend: %v", err)
		}
	}()
	second, err := NewManager(ctx, secondBackend, cfg)
	if err != nil {
		t.Fatalf("new second manager: %v", err)
	}
	if err := second.Verify(ctx, grant.ID); err != nil {
		t.Fatalf("verify restored decisions: %v", err)
	}
	if _, err := second.Consume(ctx, token, testScope()); err != nil {
		t.Fatalf("consume restored grant: %v", err)
	}
}
