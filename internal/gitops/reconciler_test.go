package gitops

import (
	"context"
	"testing"
)

func TestNewReconciler(t *testing.T) {
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)
	if r == nil {
		t.Fatal("NewReconciler returned nil")
	}
}

func TestSetDesiredState(t *testing.T) {
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
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)

	err := r.SetDesiredState(DesiredState{})
	if err == nil {
		t.Error("expected error for invalid state")
	}
}

func TestReconcileDryRun(t *testing.T) {
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
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)

	_, err := r.Reconcile(context.Background(), LiveState{}, false)
	if err == nil {
		t.Error("expected error when no desired state configured")
	}
}

func TestGetResults(t *testing.T) {
	mgr := NewManager(nil)
	r := NewReconciler(DefaultReconcilerConfig(), mgr, nil)
	results := r.GetResults(10)
	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}
