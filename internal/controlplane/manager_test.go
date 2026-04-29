package controlplane

import (
	"log/slog"
	"testing"
	"time"
)

func TestOrgLifecycle(t *testing.T) {
	m := NewManager(slog.Default())

	org, err := m.CreateOrganization("Acme Corp", "pro")
	if err != nil {
		t.Fatalf("CreateOrganization failed: %v", err)
	}
	if org.Name != "Acme Corp" {
		t.Errorf("expected Acme Corp, got %s", org.Name)
	}

	got, err := m.GetOrganization(org.ID)
	if err != nil {
		t.Fatalf("GetOrganization failed: %v", err)
	}
	if got.ID != org.ID {
		t.Errorf("ID mismatch")
	}

	orgs := m.ListOrganizations()
	if len(orgs) != 1 {
		t.Fatalf("expected 1 org, got %d", len(orgs))
	}
}

func TestAPIKeyFlow(t *testing.T) {
	m := NewManager(slog.Default())
	org, _ := m.CreateOrganization("Test", "starter")

	key, err := m.CreateAPIKey(org.ID, "test-key", []string{"read", "write"})
	if err != nil {
		t.Fatalf("CreateAPIKey failed: %v", err)
	}
	if key.Key == "" {
		t.Fatal("API key should not be empty")
	}

	orgID, err := m.ValidateAPIKey(key.Key)
	if err != nil {
		t.Fatalf("ValidateAPIKey failed: %v", err)
	}
	if orgID != org.ID {
		t.Errorf("expected org %s, got %s", org.ID, orgID)
	}

	_, err = m.ValidateAPIKey("invalid-key")
	if err == nil {
		t.Fatal("expected error for invalid key")
	}
}

func TestInstanceLifecycle(t *testing.T) {
	m := NewManager(slog.Default())
	org, _ := m.CreateOrganization("Test", "pro")

	inst, err := m.ProvisionInstance(org.ID, "prod-instance", "standard", "us-east-1", InstanceConfig{
		BootstrapServers: "kafka.internal:9092",
	})
	if err != nil {
		t.Fatalf("ProvisionInstance failed: %v", err)
	}
	if inst.Status != StatusProvisioning {
		t.Errorf("expected provisioning, got %s", inst.Status)
	}

	// Wait for async provisioning
	time.Sleep(200 * time.Millisecond)

	got, _ := m.GetInstance(inst.ID)
	if got.Status != StatusRunning {
		t.Errorf("expected running, got %s", got.Status)
	}

	// Stop
	if err := m.StopInstance(inst.ID); err != nil {
		t.Fatalf("StopInstance failed: %v", err)
	}

	// Start
	if err := m.StartInstance(inst.ID); err != nil {
		t.Fatalf("StartInstance failed: %v", err)
	}

	// Terminate
	if err := m.TerminateInstance(inst.ID); err != nil {
		t.Fatalf("TerminateInstance failed: %v", err)
	}
}

func TestUsageTracking(t *testing.T) {
	m := NewManager(slog.Default())
	org, _ := m.CreateOrganization("Test", "pro")

	m.RecordUsage(org.ID, "inst_1", "tool_calls", 100)
	m.RecordUsage(org.ID, "inst_1", "messages_read", 1000)

	summary := m.GetUsageSummary(org.ID, "current")
	if summary.ToolCalls != 100 {
		t.Errorf("expected 100 tool calls, got %d", summary.ToolCalls)
	}
	if summary.EstimatedCost <= 0 {
		t.Error("expected positive cost estimate")
	}
}
