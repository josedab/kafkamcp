package tenant

import (
	"testing"
)

func TestCreateTenant(t *testing.T) {
	m := NewManager()
	tenant, apiKey, err := m.CreateTenant("Test Corp", "admin@test.com", "pro")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant.Name != "Test Corp" {
		t.Errorf("expected name 'Test Corp', got %q", tenant.Name)
	}
	if tenant.Plan != "pro" {
		t.Errorf("expected plan 'pro', got %q", tenant.Plan)
	}
	if apiKey == "" {
		t.Error("expected non-empty API key")
	}
	if len(tenant.APIKeys) != 1 {
		t.Errorf("expected 1 API key, got %d", len(tenant.APIKeys))
	}
}

func TestCreateTenantDuplicateEmail(t *testing.T) {
	m := NewManager()
	_, _, _ = m.CreateTenant("Corp A", "same@email.com", "free")
	_, _, err := m.CreateTenant("Corp B", "same@email.com", "free")
	if err == nil {
		t.Fatal("expected error for duplicate email")
	}
}

func TestCreateTenantValidation(t *testing.T) {
	m := NewManager()
	_, _, err := m.CreateTenant("", "test@test.com", "free")
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestGetTenant(t *testing.T) {
	m := NewManager()
	tenant, _, _ := m.CreateTenant("Test", "t@t.com", "free")
	found, err := m.GetTenant(tenant.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found.ID != tenant.ID {
		t.Error("tenant ID mismatch")
	}
}

func TestPlanLimits(t *testing.T) {
	free := PlanLimits("free")
	if free.MaxClusters != 1 {
		t.Errorf("expected free plan max_clusters=1, got %d", free.MaxClusters)
	}
	pro := PlanLimits("pro")
	if pro.MaxClusters != 5 {
		t.Errorf("expected pro plan max_clusters=5, got %d", pro.MaxClusters)
	}
	enterprise := PlanLimits("enterprise")
	if enterprise.MaxClusters != -1 {
		t.Errorf("expected enterprise plan unlimited clusters, got %d", enterprise.MaxClusters)
	}
}

func TestRecordUsageAndCheckLimits(t *testing.T) {
	m := NewManager()
	tenant, _, _ := m.CreateTenant("Test", "t@t.com", "free")

	err := m.RecordUsage(tenant.ID, 5000, 100, 50, 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = m.CheckLimits(tenant.ID)
	if err != nil {
		t.Fatalf("should be within limits: %v", err)
	}

	// Exceed limit
	err = m.RecordUsage(tenant.ID, 5000, 0, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = m.CheckLimits(tenant.ID)
	if err == nil {
		t.Fatal("expected error for exceeded limits")
	}
}

func TestAddCluster(t *testing.T) {
	m := NewManager()
	tenant, _, _ := m.CreateTenant("Test", "t@t.com", "free")

	err := m.AddCluster(tenant.ID, TenantCluster{
		Name:             "prod",
		BootstrapServers: "kafka:9092",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Free plan allows only 1 cluster
	err = m.AddCluster(tenant.ID, TenantCluster{
		Name:             "staging",
		BootstrapServers: "kafka-staging:9092",
	})
	if err == nil {
		t.Fatal("expected error for cluster limit exceeded")
	}
}
