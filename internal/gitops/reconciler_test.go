package gitops

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestNewReconciler(t *testing.T) {
	t.Parallel()
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)
	if r == nil {
		t.Fatal("NewReconciler returned nil")
	}
}

func TestSetDesiredState(t *testing.T) {
	t.Parallel()
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)

	err := r.SetDesiredState(DesiredState{
		Version: "1",
		Topics: []TopicSpec{
			{Name: "orders", Partitions: 3, ReplicationFactor: 3},
		},
	})
	if err != nil {
		t.Fatalf("SetDesiredState error: %v", err)
	}
}

func TestSetDesiredStateInvalid(t *testing.T) {
	t.Parallel()
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)

	err := r.SetDesiredState(DesiredState{})
	if err == nil {
		t.Error("expected error for invalid state")
	}
}

func TestReconcileDryRun(t *testing.T) {
	t.Parallel()
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)
	_ = r.SetDesiredState(DesiredState{
		Version: "1",
		Topics: []TopicSpec{
			{Name: "orders", Partitions: 3, ReplicationFactor: 3},
		},
	})

	result, err := r.Reconcile(context.Background(), LiveState{Topics: make(map[string]TopicSpec)}, true)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if result.Status != ReconcileStatusDryRun {
		t.Errorf("Status = %v, want dry_run", result.Status)
	}
	if result.Diff.TotalChanges == 0 {
		t.Error("expected changes in diff")
	}
}

func TestReconcileApply(t *testing.T) {
	t.Parallel()
	mgr := NewManager(nil)
	cfg := DefaultReconcilerConfig()
	cfg.AutoApprove = true
	r := NewReconciler(cfg, mgr, nil)
	_ = r.SetDesiredState(DesiredState{
		Version: "1",
		Topics: []TopicSpec{
			{Name: "orders", Partitions: 3, ReplicationFactor: 3},
		},
	})

	result, err := r.Reconcile(context.Background(), LiveState{Topics: make(map[string]TopicSpec)}, false)
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}
	if result.Status != ReconcileStatusSuccess {
		t.Errorf("Status = %v, want success", result.Status)
	}
	if result.StepsApplied == 0 {
		t.Error("expected steps to be applied")
	}
}

func TestReconcileNoDesiredState(t *testing.T) {
	t.Parallel()
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)

	_, err := r.Reconcile(context.Background(), LiveState{}, false)
	if err == nil {
		t.Error("expected error when no desired state configured")
	}
}

func TestGetResults(t *testing.T) {
	t.Parallel()
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)
	results := r.GetResults(10)
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestHandleWebhook_PushEventTriggersReconcile(t *testing.T) {
	t.Parallel()
	cfg := DefaultReconcilerConfig()
	cfg.GitBranch = "main"
	mgr := NewManager(nil)
	r := NewReconciler(cfg, mgr, nil)

	_ = r.SetDesiredState(DesiredState{
		Version: "1",
		Topics:  []TopicSpec{{Name: "t1", Partitions: 1, ReplicationFactor: 1}},
	})

	event := WebhookEvent{Type: "push", Ref: "refs/heads/main", Repository: "test/repo"}
	result, err := r.HandleWebhook(event)
	if err != nil {
		t.Fatalf("HandleWebhook push: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result for push event on tracked branch")
	}
}

func TestHandleWebhook_PullRequestDryRun(t *testing.T) {
	t.Parallel()
	cfg := DefaultReconcilerConfig()
	mgr := NewManager(nil)
	r := NewReconciler(cfg, mgr, nil)

	_ = r.SetDesiredState(DesiredState{
		Version: "1",
		Topics:  []TopicSpec{{Name: "t1", Partitions: 1, ReplicationFactor: 1}},
	})

	event := WebhookEvent{Type: "pull_request", Action: "opened", Repository: "test/repo"}
	result, err := r.HandleWebhook(event)
	if err != nil {
		t.Fatalf("HandleWebhook PR: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result for opened PR")
	}
	if !result.DryRun {
		t.Error("PR webhook should trigger dry run")
	}
}

func TestHandleWebhook_PullRequestClosedSkipped(t *testing.T) {
	t.Parallel()
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)

	event := WebhookEvent{Type: "pull_request", Action: "closed", Repository: "test/repo"}
	result, err := r.HandleWebhook(event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("closed PR should return nil result")
	}
}

func TestHandleWebhook_UnknownEventReturnsNil(t *testing.T) {
	t.Parallel()
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)

	event := WebhookEvent{Type: "deployment", Repository: "test/repo"}
	result, err := r.HandleWebhook(event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("unknown event type should return nil")
	}
}

func TestHandleWebhook_PushWrongBranch(t *testing.T) {
	t.Parallel()
	cfg := DefaultReconcilerConfig()
	cfg.GitBranch = "main"
	mgr := NewManager(nil)
	r := NewReconciler(cfg, mgr, nil)

	event := WebhookEvent{Type: "push", Ref: "refs/heads/develop", Repository: "test/repo"}
	result, err := r.HandleWebhook(event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Error("push to non-tracked branch should return nil")
	}
}

func TestStart_LaunchesGoroutine(t *testing.T) {
	t.Parallel()
	cfg := DefaultReconcilerConfig()
	cfg.Interval = 50 * time.Millisecond
	mgr := NewManager(nil)
	r := NewReconciler(cfg, mgr, nil)

	r.Start(context.Background())
	defer r.Stop()

	r.mu.RLock()
	running := r.running
	r.mu.RUnlock()
	if !running {
		t.Error("expected running=true after Start")
	}
}

func TestStart_IdempotentDoubleStart(t *testing.T) {
	t.Parallel()
	cfg := DefaultReconcilerConfig()
	cfg.Interval = 50 * time.Millisecond
	mgr := NewManager(nil)
	r := NewReconciler(cfg, mgr, nil)

	ctx := context.Background()
	r.Start(ctx)
	r.Start(ctx) // second call should be no-op
	defer r.Stop()

	r.mu.RLock()
	running := r.running
	r.mu.RUnlock()
	if !running {
		t.Error("expected running=true after double Start")
	}
}

func TestStop_CancelsContextAndLoop(t *testing.T) {
	t.Parallel()
	cfg := DefaultReconcilerConfig()
	cfg.Interval = 50 * time.Millisecond
	mgr := NewManager(nil)
	r := NewReconciler(cfg, mgr, nil)

	r.Start(context.Background())
	r.Stop()

	r.mu.RLock()
	running := r.running
	r.mu.RUnlock()
	if running {
		t.Error("expected running=false after Stop")
	}
}

func TestLoop_PeriodicReconcile(t *testing.T) {
	t.Parallel()
	cfg := DefaultReconcilerConfig()
	cfg.Interval = 50 * time.Millisecond
	cfg.DryRunDefault = true
	mgr := NewManager(nil)
	r := NewReconciler(cfg, mgr, nil)

	_ = r.SetDesiredState(DesiredState{
		Version: "1",
		Topics:  []TopicSpec{{Name: "loop-topic", Partitions: 1, ReplicationFactor: 1}},
	})

	r.Start(context.Background())
	time.Sleep(200 * time.Millisecond)
	r.Stop()

	results := r.GetResults(0)
	if len(results) == 0 {
		t.Error("expected at least one reconciliation result from loop")
	}
}

func TestLoop_NilDesiredStateSkips(t *testing.T) {
	t.Parallel()
	cfg := DefaultReconcilerConfig()
	cfg.Interval = 50 * time.Millisecond
	mgr := NewManager(nil)
	r := NewReconciler(cfg, mgr, nil)

	r.Start(context.Background())
	time.Sleep(150 * time.Millisecond)
	r.Stop()

	results := r.GetResults(0)
	if len(results) != 0 {
		t.Errorf("expected 0 results with nil desired state, got %d", len(results))
	}
}

func TestConcurrent_StartStop(t *testing.T) {
	t.Parallel()
	cfg := DefaultReconcilerConfig()
	cfg.Interval = 10 * time.Millisecond
	mgr := NewManager(nil)
	r := NewReconciler(cfg, mgr, nil)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Start(context.Background())
			time.Sleep(5 * time.Millisecond)
			r.Stop()
		}()
	}
	wg.Wait()
}

func TestRecordResult_MaxHistoryEnforced(t *testing.T) {
	t.Parallel()
	cfg := DefaultReconcilerConfig()
	cfg.MaxHistory = 3
	mgr := NewManager(nil)
	r := NewReconciler(cfg, mgr, nil)

	for i := 0; i < 5; i++ {
		r.recordResult(ReconcileResult{ID: fmt.Sprintf("r-%d", i)})
	}

	results := r.GetResults(0)
	if len(results) != 3 {
		t.Errorf("expected 3 results (max history), got %d", len(results))
	}
}

func TestGetResults_LimitBehavior(t *testing.T) {
	t.Parallel()
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)

	_ = r.SetDesiredState(DesiredState{
		Version: "1",
		Topics:  []TopicSpec{{Name: "t1", Partitions: 1, ReplicationFactor: 1}},
	})

	for i := 0; i < 5; i++ {
		r.Reconcile(context.Background(), LiveState{Topics: make(map[string]TopicSpec)}, true)
	}

	all := r.GetResults(0)
	if len(all) != 5 {
		t.Errorf("expected 5 results, got %d", len(all))
	}
	limited := r.GetResults(2)
	if len(limited) != 2 {
		t.Errorf("expected 2 results with limit=2, got %d", len(limited))
	}
}
