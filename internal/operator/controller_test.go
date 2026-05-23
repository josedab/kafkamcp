package operator

import (
	"log/slog"
	"testing"
)

func TestCreateInstance(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())

	inst, err := c.CreateInstance("test-kafkamcp", "default", KafkaMCPSpec{
		Replicas: 3,
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if inst.Spec.Replicas != 3 {
		t.Fatalf("expected 3 replicas, got %d", inst.Spec.Replicas)
	}
	if inst.Status.Phase != "Running" {
		t.Fatalf("expected Running, got %s", inst.Status.Phase)
	}

	// Duplicate should fail
	_, err = c.CreateInstance("test-kafkamcp", "default", KafkaMCPSpec{})
	if err == nil {
		t.Fatal("expected error for duplicate instance")
	}
}

func TestListInstances(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())

	_, _ = c.CreateInstance("inst1", "ns1", KafkaMCPSpec{})
	_, _ = c.CreateInstance("inst2", "ns2", KafkaMCPSpec{})

	all := c.ListInstances("")
	if len(all) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(all))
	}

	ns1 := c.ListInstances("ns1")
	if len(ns1) != 1 {
		t.Fatalf("expected 1 instance in ns1, got %d", len(ns1))
	}
}

func TestScaleInstance(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())

	_, _ = c.CreateInstance("test", "default", KafkaMCPSpec{Replicas: 1})

	inst, err := c.ScaleInstance("test", "default", 5)
	if err != nil {
		t.Fatalf("scale failed: %v", err)
	}
	if inst.Spec.Replicas != 5 {
		t.Fatalf("expected 5 replicas, got %d", inst.Spec.Replicas)
	}
}

func TestDeleteInstance(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())

	_, _ = c.CreateInstance("test", "default", KafkaMCPSpec{})

	err := c.DeleteInstance("test", "default")
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, err = c.GetInstance("test", "default")
	if err == nil {
		t.Fatal("expected error getting deleted instance")
	}
}

func TestGenerateCRD(t *testing.T) {
	t.Parallel()
	crd := GenerateCRD()
	if crd == "" {
		t.Fatal("expected non-empty CRD")
	}
	if !contains(crd, "kafkamcps.kafkamcp.io") {
		t.Fatal("expected CRD to contain resource name")
	}
}

func TestGenerateManifest(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())
	inst, _ := c.CreateInstance("test", "default", KafkaMCPSpec{Replicas: 2})

	manifest := c.GenerateDeploymentManifest(inst)
	if !contains(manifest, "replicas: 2") {
		t.Fatal("expected manifest to contain replicas")
	}
}

func TestDefaultValues(t *testing.T) {
	t.Parallel()
	c := NewController(slog.Default())

	inst, _ := c.CreateInstance("test", "default", KafkaMCPSpec{})

	if inst.Spec.Image != "kafkamcp/kafkamcp" {
		t.Fatalf("expected default image, got %s", inst.Spec.Image)
	}
	if inst.Spec.Port != 3100 {
		t.Fatalf("expected default port 3100, got %d", inst.Spec.Port)
	}
	if inst.Spec.Resources.CPURequest != "100m" {
		t.Fatalf("expected default CPU request, got %s", inst.Spec.Resources.CPURequest)
	}
}

func contains(s, sub string) bool {
	return len(s) > 0 && len(sub) > 0 && len(s) >= len(sub) && containsSubstring(s, sub)
}

func containsSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
