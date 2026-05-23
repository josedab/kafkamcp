package tenant

import (
	"testing"
)

func TestAuthenticateByKey(t *testing.T) {
	t.Parallel()
	m := NewManager()
	tenant, _, _ := m.CreateTenant("Auth Test", "auth@test.com", "pro")

	// Authenticate with valid key prefix
	prefix := tenant.APIKeys[0].Prefix
	found, err := m.AuthenticateByKey(prefix)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if found.ID != tenant.ID {
		t.Error("tenant ID mismatch after authentication")
	}

	// Authenticate with invalid key prefix
	_, err = m.AuthenticateByKey("invalid_prefix")
	if err == nil {
		t.Fatal("expected error for invalid API key")
	}
}

func TestAuthenticateByKeySuspendedTenant(t *testing.T) {
	t.Parallel()
	m := NewManager()
	tenant, _, _ := m.CreateTenant("Suspended", "suspended@test.com", "free")
	prefix := tenant.APIKeys[0].Prefix

	// Suspend the tenant
	m.mu.Lock()
	m.tenants[tenant.ID].Status = "suspended"
	m.mu.Unlock()

	_, err := m.AuthenticateByKey(prefix)
	if err == nil {
		t.Fatal("expected error for suspended tenant")
	}
}

func TestListTenants(t *testing.T) {
	t.Parallel()
	m := NewManager()

	// Empty list
	tenants := m.ListTenants()
	if len(tenants) != 0 {
		t.Fatalf("expected 0 tenants, got %d", len(tenants))
	}

	// Add tenants
	_, _, _ = m.CreateTenant("Tenant A", "a@test.com", "free")
	_, _, _ = m.CreateTenant("Tenant B", "b@test.com", "pro")

	tenants = m.ListTenants()
	if len(tenants) != 2 {
		t.Fatalf("expected 2 tenants, got %d", len(tenants))
	}
}

func TestPlanLimitsDefault(t *testing.T) {
	t.Parallel()
	// Unknown plan should default to free
	limits := PlanLimits("unknown_plan")
	freeLimits := PlanLimits("free")
	if limits.MaxClusters != freeLimits.MaxClusters {
		t.Errorf("expected unknown plan to default to free limits")
	}
	if limits.MaxToolCallsPerMonth != freeLimits.MaxToolCallsPerMonth {
		t.Errorf("expected same tool call limit as free")
	}
}

func TestCreateTenantEmptyEmail(t *testing.T) {
	t.Parallel()
	m := NewManager()
	_, _, err := m.CreateTenant("Name", "", "free")
	if err == nil {
		t.Fatal("expected error for empty email")
	}
}

func TestCreateTenantDefaultPlan(t *testing.T) {
	t.Parallel()
	m := NewManager()
	tenant, _, err := m.CreateTenant("Default Plan", "default@test.com", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tenant.Plan != "free" {
		t.Errorf("expected default plan 'free', got %q", tenant.Plan)
	}
}

func TestGetTenantNotFound(t *testing.T) {
	t.Parallel()
	m := NewManager()
	_, err := m.GetTenant("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent tenant")
	}
}

func TestRecordUsageNotFound(t *testing.T) {
	t.Parallel()
	m := NewManager()
	err := m.RecordUsage("nonexistent", 1, 0, 0, 0)
	if err == nil {
		t.Fatal("expected error for nonexistent tenant")
	}
}

func TestCheckLimitsNotFound(t *testing.T) {
	t.Parallel()
	m := NewManager()
	err := m.CheckLimits("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent tenant")
	}
}

func TestCheckLimitsEnterprise(t *testing.T) {
	t.Parallel()
	m := NewManager()
	tenant, _, _ := m.CreateTenant("Enterprise", "ent@test.com", "enterprise")

	// Record massive usage
	_ = m.RecordUsage(tenant.ID, 999999999, 0, 0, 0)

	// Enterprise has unlimited (-1), so should never exceed
	err := m.CheckLimits(tenant.ID)
	if err != nil {
		t.Fatalf("enterprise should not have limits: %v", err)
	}
}

func TestAddClusterNotFound(t *testing.T) {
	t.Parallel()
	m := NewManager()
	err := m.AddCluster("nonexistent", TenantCluster{Name: "prod"})
	if err == nil {
		t.Fatal("expected error for nonexistent tenant")
	}
}

func TestAddClusterEnterprise(t *testing.T) {
	t.Parallel()
	m := NewManager()
	tenant, _, _ := m.CreateTenant("Enterprise", "ent@test.com", "enterprise")

	// Enterprise has unlimited clusters (-1)
	for i := 0; i < 10; i++ {
		err := m.AddCluster(tenant.ID, TenantCluster{Name: "cluster"})
		if err != nil {
			t.Fatalf("enterprise should allow unlimited clusters: %v", err)
		}
	}
}
