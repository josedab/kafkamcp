package approval

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/josedab/kafkamcp/internal/engine/backend"
)

func TestPreviousSigningKeyAllowsHistoricalVerification(t *testing.T) {
	ctx := context.Background()
	be := backend.NewInMemory()
	oldKey := []byte("old-key-0123456789abcdef01234567")
	newKey := []byte("new-key-0123456789abcdef01234567")

	oldCfg := DefaultConfig()
	oldCfg.DecisionSigningKey = oldKey
	oldCfg.RequireSignedDecisions = true
	oldManager, err := NewManager(ctx, be, oldCfg)
	if err != nil {
		t.Fatalf("old manager: %v", err)
	}
	grant, err := oldManager.Request(ctx, RequestSpec{
		Scope: testScope(), Preview: "delete orders", Requester: "agent-1",
	})
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	newCfg := DefaultConfig()
	newCfg.DecisionSigningKey = newKey
	newCfg.VerificationKeys = [][]byte{oldKey}
	newCfg.RequireSignedDecisions = true
	newManager, err := NewManager(ctx, be, newCfg)
	if err != nil {
		t.Fatalf("rotated manager: %v", err)
	}
	if _, err := newManager.Get(ctx, grant.ID); err != nil {
		t.Fatalf("historical grant should remain readable: %v", err)
	}
	if _, _, err := newManager.Approve(ctx, grant.ID, "operator", 0); err == nil ||
		!strings.Contains(err.Error(), "request a new approval") {
		t.Fatalf("in-flight old-key grant must not cross signing keys, got %v", err)
	}
}

func TestPruneRemovesOldTerminalGrants(t *testing.T) {
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.TerminalRetention = time.Hour
	manager, clock := newManagerWithConfig(t, cfg)
	grantID, token := approveGrant(t, manager, testScope())
	if _, err := manager.Consume(ctx, token, testScope()); err != nil {
		t.Fatalf("consume: %v", err)
	}
	clock.Advance(2 * time.Hour)
	pruned, err := manager.Prune(ctx)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected one pruned grant, got %d", pruned)
	}
	if _, err := manager.Get(ctx, grantID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected pruned grant to be absent, got %v", err)
	}
}

func newManagerWithConfig(t *testing.T, cfg Config) (*Manager, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	manager, err := NewManager(context.Background(), backend.NewInMemory(), cfg)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	manager.SetClock(clock.Now)
	manager.lastPrune = clock.Now()
	return manager, clock
}
