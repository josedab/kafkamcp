package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/josedab/kafkamcp/internal/engine"
	"github.com/josedab/kafkamcp/internal/engine/backend"
)

const namespace = "idempotency_receipts"

var (
	// ErrKeyConflict indicates an idempotency key was reused for a different
	// operation scope.
	ErrKeyConflict = errors.New("idempotency: key was already used for a different operation")
	// ErrInProgress indicates the matching operation has not completed yet.
	ErrInProgress = errors.New("idempotency: matching operation is already in progress")
	// ErrReservationLost indicates the caller no longer owns the pending
	// reservation, usually because its pending lease expired and another
	// process safely replaced it.
	ErrReservationLost = errors.New("idempotency: reservation is no longer current")
	// ErrConcurrentUpdate indicates repeated compare-and-swap conflicts.
	ErrConcurrentUpdate = errors.New("idempotency: concurrent state update")
)

const maxCASAttempts = 8

// Receipt records a safely retryable mutation result without storing the raw
// idempotency key.
type Receipt struct {
	KeyHash     string          `json:"key_hash"`
	ScopeDigest string          `json:"scope_digest"`
	Status      string          `json:"status"`
	Result      json.RawMessage `json:"result,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
	ExpiresAt   time.Time       `json:"expires_at"`
}

// Manager coordinates idempotent mutation reservations and completed receipts.
type Manager struct {
	mu         sync.Mutex
	store      *engine.Store[Receipt]
	now        func() time.Time
	ttl        time.Duration
	pendingTTL time.Duration
}

// NewManager builds a persistence-aware receipt manager.
func NewManager(be backend.Backend, ttl time.Duration) *Manager {
	if be == nil {
		be = backend.NewInMemory()
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &Manager{
		store:      engine.NewStore[Receipt](be, namespace),
		now:        time.Now,
		ttl:        ttl,
		pendingTTL: 5 * time.Minute,
	}
}

// Begin reserves key for scope. A completed matching receipt is returned with
// replay=true; callers must return its result without executing the mutation.
func (m *Manager) Begin(ctx context.Context, key, scope string) (receipt Receipt, replay bool, err error) {
	if key == "" {
		return Receipt{}, false, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	keyHash := digest("idempotency-key\x00" + key)
	for range maxCASAttempts {
		now := m.now()
		existing, ok, err := m.store.GetVersioned(ctx, keyHash)
		if err != nil {
			return Receipt{}, false, err
		}
		if ok && now.Before(existing.Value.ExpiresAt) {
			if existing.Value.ScopeDigest != scope {
				return Receipt{}, false, ErrKeyConflict
			}
			if existing.Value.Status == "completed" {
				return existing.Value, true, nil
			}
			if now.Sub(existing.Value.CreatedAt) < m.pendingTTL {
				return Receipt{}, false, ErrInProgress
			}
		}

		receipt = Receipt{
			KeyHash:     keyHash,
			ScopeDigest: scope,
			Status:      "pending",
			CreatedAt:   now,
			ExpiresAt:   now.Add(m.ttl),
		}
		var swapped bool
		if ok {
			swapped, err = m.store.CompareAndSwap(ctx, keyHash, existing.Raw, receipt)
		} else {
			swapped, err = m.store.Create(ctx, keyHash, receipt)
		}
		if err != nil {
			return Receipt{}, false, err
		}
		if swapped {
			return receipt, false, nil
		}
	}
	return Receipt{}, false, ErrConcurrentUpdate
}

// Complete stores the successful result for a reservation.
func (m *Manager) Complete(ctx context.Context, receipt Receipt, result any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	raw, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("idempotency: marshal result: %w", err)
	}
	for range maxCASAttempts {
		current, ok, err := m.store.GetVersioned(ctx, receipt.KeyHash)
		if err != nil {
			return err
		}
		if !ok || !sameReservation(current.Value, receipt) {
			return ErrReservationLost
		}
		if current.Value.Status == "completed" {
			return nil
		}
		if current.Value.Status != "pending" {
			return ErrReservationLost
		}
		now := m.now()
		completed := current.Value
		completed.Status = "completed"
		completed.Result = raw
		completed.CompletedAt = &now
		swapped, err := m.store.CompareAndSwap(ctx, receipt.KeyHash, current.Raw, completed)
		if err != nil {
			return err
		}
		if swapped {
			return nil
		}
	}
	return ErrConcurrentUpdate
}

// Abort removes a pending reservation after a denied or failed operation.
func (m *Manager) Abort(ctx context.Context, receipt Receipt) error {
	if receipt.KeyHash == "" {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for range maxCASAttempts {
		current, ok, err := m.store.GetVersioned(ctx, receipt.KeyHash)
		if err != nil || !ok {
			return err
		}
		if current.Value.Status != "pending" || !sameReservation(current.Value, receipt) {
			return nil
		}
		deleted, err := m.store.DeleteIfUnchanged(ctx, receipt.KeyHash, current.Raw)
		if err != nil {
			return err
		}
		if deleted {
			return nil
		}
	}
	return ErrConcurrentUpdate
}

func sameReservation(current, expected Receipt) bool {
	return current.KeyHash == expected.KeyHash &&
		current.ScopeDigest == expected.ScopeDigest &&
		current.CreatedAt.Equal(expected.CreatedAt)
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
