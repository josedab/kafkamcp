package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/josedab/kafkamcp/internal/engine/backend"
)

func TestManagerCompletedReplay(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(backend.NewInMemory(), 0)
	receipt, replay, err := manager.Begin(ctx, "request-1", "scope-a")
	if err != nil || replay {
		t.Fatalf("begin: replay=%v err=%v", replay, err)
	}
	if err := manager.Complete(ctx, receipt, map[string]any{"status": "created"}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	got, replay, err := manager.Begin(ctx, "request-1", "scope-a")
	if err != nil || !replay {
		t.Fatalf("replay: replay=%v err=%v", replay, err)
	}
	var result map[string]any
	if err := json.Unmarshal(got.Result, &result); err != nil {
		t.Fatalf("result: %v", err)
	}
	if result["status"] != "created" {
		t.Fatalf("unexpected cached result: %#v", result)
	}
}

func TestConcurrentManagersReserveOnce(t *testing.T) {
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
				path := filepath.Join(t.TempDir(), "idempotency.db")
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
			managers := []*Manager{
				NewManager(firstBackend, 0),
				NewManager(secondBackend, 0),
			}
			start := make(chan struct{})
			var wg sync.WaitGroup
			errs := make(chan error, len(managers))
			for _, manager := range managers {
				wg.Add(1)
				go func(manager *Manager) {
					defer wg.Done()
					<-start
					_, _, err := manager.Begin(ctx, "shared-key", "scope-a")
					errs <- err
				}(manager)
			}
			close(start)
			wg.Wait()
			close(errs)

			successes := 0
			inProgress := 0
			for err := range errs {
				switch {
				case err == nil:
					successes++
				case errors.Is(err, ErrInProgress):
					inProgress++
				default:
					t.Fatalf("unexpected Begin error: %v", err)
				}
			}
			if successes != 1 || inProgress != 1 {
				t.Fatalf("successes=%d in_progress=%d, want 1 each", successes, inProgress)
			}
		})
	}
}

func TestManagerRejectsKeyReuseAcrossScopes(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(backend.NewInMemory(), 0)
	if _, _, err := manager.Begin(ctx, "request-1", "scope-a"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, _, err := manager.Begin(ctx, "request-1", "scope-b"); !errors.Is(err, ErrKeyConflict) {
		t.Fatalf("expected key conflict, got %v", err)
	}
}

func TestManagerAbortAllowsRetry(t *testing.T) {
	ctx := context.Background()
	manager := NewManager(backend.NewInMemory(), 0)
	receipt, _, err := manager.Begin(ctx, "request-1", "scope-a")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := manager.Abort(ctx, receipt); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, replay, err := manager.Begin(ctx, "request-1", "scope-a"); err != nil || replay {
		t.Fatalf("retry: replay=%v err=%v", replay, err)
	}
}

func TestCompletedReceiptPersistsAcrossManagers(t *testing.T) {
	ctx := context.Background()
	be := backend.NewInMemory()
	first := NewManager(be, 0)
	receipt, _, err := first.Begin(ctx, "request-1", "scope-a")
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := first.Complete(ctx, receipt, map[string]any{"status": "done"}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	second := NewManager(be, 0)
	if _, replay, err := second.Begin(ctx, "request-1", "scope-a"); err != nil || !replay {
		t.Fatalf("restored replay: replay=%v err=%v", replay, err)
	}
}
