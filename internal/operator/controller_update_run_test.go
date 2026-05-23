package operator

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestUpdateInstance_ExistingInstance(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())
	_, _ = c.CreateInstance("test", "default", KafkaMCPSpec{Replicas: 1})

	newSpec := KafkaMCPSpec{Replicas: 5, Image: "custom/image", Version: "v2"}
	inst, err := c.UpdateInstance("test", "default", newSpec)
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	if inst.Spec.Replicas != 5 {
		t.Errorf("expected 5 replicas, got %d", inst.Spec.Replicas)
	}
	if inst.Spec.Image != "custom/image" {
		t.Errorf("expected custom/image, got %s", inst.Spec.Image)
	}
}

func TestUpdateInstance_SetsUpdatingThenRunning(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())
	_, _ = c.CreateInstance("test", "ns", KafkaMCPSpec{Replicas: 1})

	// After UpdateInstance, reconcile is called which sets phase to Running
	inst, err := c.UpdateInstance("test", "ns", KafkaMCPSpec{Replicas: 3})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	// reconcile is called synchronously so phase should be Running
	if inst.Status.Phase != "Running" {
		t.Errorf("expected Running after reconcile, got %s", inst.Status.Phase)
	}
}

func TestUpdateInstance_NonExistent(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())

	_, err := c.UpdateInstance("nonexistent", "default", KafkaMCPSpec{})
	if err == nil {
		t.Fatal("expected error for non-existent instance")
	}
}

func TestUpdateInstance_PreservesNameNamespace(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())
	_, _ = c.CreateInstance("myapp", "production", KafkaMCPSpec{Replicas: 1})

	inst, err := c.UpdateInstance("myapp", "production", KafkaMCPSpec{Replicas: 10})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}

	if inst.Name != "myapp" {
		t.Errorf("expected name 'myapp', got %q", inst.Name)
	}
	if inst.Namespace != "production" {
		t.Errorf("expected namespace 'production', got %q", inst.Namespace)
	}
}

func TestRun_ShutdownOnContextCancel(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx)
	}()

	// Cancel quickly
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected nil error on clean shutdown, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestRun_ReconcilesTicker(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())
	_, _ = c.CreateInstance("test", "default", KafkaMCPSpec{Replicas: 2})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx)
	}()

	// Let it run briefly and then cancel
	time.Sleep(100 * time.Millisecond)

	// Verify instance still exists and is running
	inst, err := c.GetInstance("test", "default")
	if err != nil {
		t.Fatalf("instance should still exist: %v", err)
	}
	if inst.Status.Phase != "Running" {
		t.Errorf("expected Running phase, got %s", inst.Status.Phase)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestRun_NewInstanceReconciled(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.Run(ctx)
	}()

	// Add instance while running
	_, err := c.CreateInstance("dynamic", "default", KafkaMCPSpec{Replicas: 3})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	inst, err := c.GetInstance("dynamic", "default")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if inst.Status.Phase != "Running" {
		t.Errorf("expected Running, got %s", inst.Status.Phase)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop")
	}
}
