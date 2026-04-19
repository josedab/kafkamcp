package tenant

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// helper to create a ControlPlane with a tenant already registered
func setupControlPlane(t *testing.T) (*ControlPlane, string) {
	t.Helper()
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	tenant, _, err := mgr.CreateTenant("Test Corp", "test@example.com", "pro")
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}
	return cp, tenant.ID
}

// --- ControlPlane Region tests ---

func TestControlPlane_AddAndListRegions(t *testing.T) {
	mgr := NewManager()
	cp := NewControlPlane(mgr)

	if regions := cp.ListRegions(); len(regions) != 0 {
		t.Errorf("expected 0 regions, got %d", len(regions))
	}

	cp.AddRegion(RegionConfig{Name: "us-east-1", Provider: "aws", IsActive: true})
	cp.AddRegion(RegionConfig{Name: "eu-west-1", Provider: "aws", IsActive: true})

	regions := cp.ListRegions()
	if len(regions) != 2 {
		t.Fatalf("expected 2 regions, got %d", len(regions))
	}
	if regions[0].Name != "us-east-1" {
		t.Errorf("expected us-east-1, got %s", regions[0].Name)
	}
}

// --- Billing tests ---

func TestControlPlane_SetupAndGetBilling(t *testing.T) {
	cp, tenantID := setupControlPlane(t)

	billing := BillingInfo{
		PlanID:       "pro",
		BillingEmail: "billing@test.com",
		Status:       "active",
	}
	if err := cp.SetupBilling(tenantID, billing); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	b, err := cp.GetBilling(tenantID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if b.PlanID != "pro" || b.Status != "active" {
		t.Errorf("unexpected billing: %+v", b)
	}
}

func TestControlPlane_SetupBilling_InvalidTenant(t *testing.T) {
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	err := cp.SetupBilling("nonexistent", BillingInfo{})
	if err == nil {
		t.Error("expected error for nonexistent tenant")
	}
}

func TestControlPlane_GetBilling_NoBilling(t *testing.T) {
	cp, _ := setupControlPlane(t)
	_, err := cp.GetBilling("nonexistent")
	if err == nil {
		t.Error("expected error for missing billing")
	}
}

// --- ResourceQuota tests ---

func TestControlPlane_SetAndGetResourceQuota(t *testing.T) {
	cp, tenantID := setupControlPlane(t)

	quota := ResourceQuota{MaxCPUMillicores: 4000, MaxMemoryMB: 2048, MaxConcurrentAgents: 50}
	if err := cp.SetResourceQuota(tenantID, quota); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	q := cp.GetResourceQuota(tenantID)
	if q.MaxCPUMillicores != 4000 {
		t.Errorf("expected 4000 CPU, got %d", q.MaxCPUMillicores)
	}
}

func TestControlPlane_SetResourceQuota_InvalidTenant(t *testing.T) {
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	err := cp.SetResourceQuota("nonexistent", ResourceQuota{})
	if err == nil {
		t.Error("expected error for nonexistent tenant")
	}
}

func TestControlPlane_GetResourceQuota_DefaultsByPlan(t *testing.T) {
	mgr := NewManager()
	cp := NewControlPlane(mgr)

	tests := []struct {
		plan    string
		maxCPU  int
	}{
		{"free", 500},
		{"pro", 2000},
		{"enterprise", 8000},
		{"unknown", 500},
	}

	for _, tt := range tests {
		tenant, _, _ := mgr.CreateTenant("Tenant-"+tt.plan, tt.plan+"@test.com", tt.plan)
		q := cp.GetResourceQuota(tenant.ID)
		if q.MaxCPUMillicores != tt.maxCPU {
			t.Errorf("plan %s: expected %d CPU, got %d", tt.plan, tt.maxCPU, q.MaxCPUMillicores)
		}
	}
}

func TestControlPlane_GetResourceQuota_UnknownTenant(t *testing.T) {
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	q := cp.GetResourceQuota("missing")
	if q.MaxCPUMillicores != 500 {
		t.Errorf("expected fallback 500 CPU, got %d", q.MaxCPUMillicores)
	}
}

// --- SSO tests ---

func TestControlPlane_ConfigureAndGetSSO(t *testing.T) {
	cp, tenantID := setupControlPlane(t)

	sso := SSOConfig{Provider: "oidc", ClientID: "client123", IssuerURL: "https://auth.example.com"}
	if err := cp.ConfigureSSO(tenantID, sso); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, err := cp.GetSSO(tenantID)
	if err != nil || got.Provider != "oidc" || got.ClientID != "client123" {
		t.Errorf("unexpected SSO: %+v, err=%v", got, err)
	}
}

func TestControlPlane_ConfigureSSO_InvalidTenant(t *testing.T) {
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	err := cp.ConfigureSSO("nonexistent", SSOConfig{})
	if err == nil {
		t.Error("expected error for nonexistent tenant")
	}
}

func TestControlPlane_GetSSO_NotConfigured(t *testing.T) {
	cp, tenantID := setupControlPlane(t)
	_, err := cp.GetSSO(tenantID)
	if err == nil {
		t.Error("expected error for unconfigured SSO")
	}
}

// --- NetworkIsolation tests ---

func TestControlPlane_ConfigureAndGetNetworkIsolation(t *testing.T) {
	cp, tenantID := setupControlPlane(t)

	iso := NetworkIsolation{VPCEnabled: true, TLSRequired: true, PrivateLink: true}
	if err := cp.ConfigureNetworkIsolation(tenantID, iso); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := cp.GetNetworkIsolation(tenantID)
	if !got.VPCEnabled || !got.PrivateLink {
		t.Errorf("unexpected isolation: %+v", got)
	}
}

func TestControlPlane_ConfigureNetworkIsolation_InvalidTenant(t *testing.T) {
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	err := cp.ConfigureNetworkIsolation("nonexistent", NetworkIsolation{})
	if err == nil {
		t.Error("expected error for nonexistent tenant")
	}
}

func TestControlPlane_GetNetworkIsolation_Default(t *testing.T) {
	cp, tenantID := setupControlPlane(t)
	got := cp.GetNetworkIsolation(tenantID)
	if !got.TLSRequired {
		t.Error("expected default TLSRequired=true")
	}
}

// --- Usage/Billing cost tests ---

func TestControlPlane_RecordUsagePeriod(t *testing.T) {
	cp, tenantID := setupControlPlane(t)

	record := UsageBillingRecord{
		TenantID:         tenantID,
		ToolCalls:        1000,
		MessagesConsumed: 5000,
		MessagesProduced: 2000,
		BytesTransferred: 1024 * 1024 * 1024, // 1GB
	}
	cp.RecordUsagePeriod(record)

	records := cp.GetUsageRecords(tenantID)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].ComputedCostUSD <= 0 {
		t.Error("expected computed cost > 0")
	}
}

func TestControlPlane_ComputeCost(t *testing.T) {
	mgr := NewManager()
	cp := NewControlPlane(mgr)

	record := UsageBillingRecord{
		ToolCalls:        100,
		MessagesConsumed: 1000,
		MessagesProduced: 0,
		BytesTransferred: 0,
	}
	cost := cp.computeCost(record)
	// 100 * 0.0001 + 1000 * 0.00001 = 0.01 + 0.01 = 0.02
	if cost < 0.019 || cost > 0.021 {
		t.Errorf("expected cost ~0.02, got %f", cost)
	}
}

func TestControlPlane_GetUsageRecords_FilterByTenant(t *testing.T) {
	cp, tenantID := setupControlPlane(t)

	cp.RecordUsagePeriod(UsageBillingRecord{TenantID: tenantID, ToolCalls: 10})
	cp.RecordUsagePeriod(UsageBillingRecord{TenantID: "other-tenant", ToolCalls: 20})

	records := cp.GetUsageRecords(tenantID)
	if len(records) != 1 {
		t.Errorf("expected 1 record for tenant, got %d", len(records))
	}
}

func TestControlPlane_RecordUsagePeriod_Cap10000(t *testing.T) {
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	for i := 0; i < 10005; i++ {
		cp.RecordUsagePeriod(UsageBillingRecord{TenantID: "t1", ToolCalls: int64(i)})
	}
	cp.mu.RLock()
	count := len(cp.usageRecords)
	cp.mu.RUnlock()
	if count > 10000 {
		t.Errorf("expected max 10000 records, got %d", count)
	}
}

// --- GenerateAdminAPIKey tests ---

func TestGenerateAdminAPIKey(t *testing.T) {
	key, err := GenerateAdminAPIKey()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(key, "kmcp_admin_") {
		t.Errorf("expected prefix kmcp_admin_, got %s", key)
	}
	if len(key) < 20 {
		t.Error("expected key longer than 20 chars")
	}
}

// --- InstanceManager tests ---

func TestInstanceManager_RegisterAndGet(t *testing.T) {
	im := NewInstanceManager()
	im.RegisterInstance(TenantInstance{
		TenantID:   "t1",
		InstanceID: "i1",
		Region:     "us-east-1",
		Status:     "running",
		StartedAt:  time.Now(),
		CPU:        50.0,
	})

	instances := im.GetInstances("t1")
	if len(instances) != 1 || instances[0].InstanceID != "i1" {
		t.Errorf("unexpected instances: %+v", instances)
	}
	if empty := im.GetInstances("missing"); len(empty) != 0 {
		t.Error("expected empty list for unknown tenant")
	}
}

func TestInstanceManager_SetAutoScaleConfig(t *testing.T) {
	im := NewInstanceManager()
	im.SetAutoScaleConfig("t1", AutoScaleConfig{
		Enabled: true, MinInstances: 2, MaxInstances: 10, TargetCPUPercent: 70,
	})
	im.mu.RLock()
	cfg := im.configs["t1"]
	im.mu.RUnlock()
	if cfg == nil || !cfg.Enabled || cfg.MinInstances != 2 {
		t.Error("expected auto-scale config to be set")
	}
}

func TestInstanceManager_EvaluateAutoScale_NoConfig(t *testing.T) {
	im := NewInstanceManager()
	action, count := im.EvaluateAutoScale("t1")
	if action != "none" || count != 0 {
		t.Errorf("expected none/0, got %s/%d", action, count)
	}
}

func TestInstanceManager_EvaluateAutoScale_Disabled(t *testing.T) {
	im := NewInstanceManager()
	im.SetAutoScaleConfig("t1", AutoScaleConfig{Enabled: false})
	action, _ := im.EvaluateAutoScale("t1")
	if action != "none" {
		t.Errorf("expected 'none' when disabled, got %s", action)
	}
}

func TestInstanceManager_EvaluateAutoScale_NoInstances(t *testing.T) {
	im := NewInstanceManager()
	im.SetAutoScaleConfig("t1", AutoScaleConfig{Enabled: true, MinInstances: 3, MaxInstances: 10, TargetCPUPercent: 70})

	action, count := im.EvaluateAutoScale("t1")
	if action != "scale_up" || count != 3 {
		t.Errorf("expected scale_up/3, got %s/%d", action, count)
	}
}

func TestInstanceManager_EvaluateAutoScale_ScaleUp(t *testing.T) {
	im := NewInstanceManager()
	im.SetAutoScaleConfig("t1", AutoScaleConfig{Enabled: true, MinInstances: 1, MaxInstances: 5, TargetCPUPercent: 50})
	im.RegisterInstance(TenantInstance{TenantID: "t1", InstanceID: "i1", CPU: 80.0})

	action, count := im.EvaluateAutoScale("t1")
	if action != "scale_up" || count != 1 {
		t.Errorf("expected scale_up/1, got %s/%d", action, count)
	}
}

func TestInstanceManager_EvaluateAutoScale_ScaleDown(t *testing.T) {
	im := NewInstanceManager()
	im.SetAutoScaleConfig("t1", AutoScaleConfig{Enabled: true, MinInstances: 1, MaxInstances: 5, TargetCPUPercent: 80})
	im.RegisterInstance(TenantInstance{TenantID: "t1", InstanceID: "i1", CPU: 10.0})
	im.RegisterInstance(TenantInstance{TenantID: "t1", InstanceID: "i2", CPU: 20.0})

	action, count := im.EvaluateAutoScale("t1")
	if action != "scale_down" || count != 1 {
		t.Errorf("expected scale_down/1, got %s/%d", action, count)
	}
}

func TestInstanceManager_EvaluateAutoScale_None(t *testing.T) {
	im := NewInstanceManager()
	im.SetAutoScaleConfig("t1", AutoScaleConfig{Enabled: true, MinInstances: 1, MaxInstances: 5, TargetCPUPercent: 70})
	im.RegisterInstance(TenantInstance{TenantID: "t1", InstanceID: "i1", CPU: 50.0})

	action, _ := im.EvaluateAutoScale("t1")
	if action != "none" {
		t.Errorf("expected 'none', got %s", action)
	}
}

func TestInstanceManager_EvaluateAutoScale_AtMaxInstances(t *testing.T) {
	im := NewInstanceManager()
	im.SetAutoScaleConfig("t1", AutoScaleConfig{Enabled: true, MinInstances: 1, MaxInstances: 1, TargetCPUPercent: 50})
	im.RegisterInstance(TenantInstance{TenantID: "t1", InstanceID: "i1", CPU: 90.0})

	action, _ := im.EvaluateAutoScale("t1")
	if action != "none" {
		t.Errorf("expected 'none' at max, got %s", action)
	}
}

// --- HTTP API tests ---

func TestControlPlaneAPI_AuthMiddleware(t *testing.T) {
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "secret-key")
	handler := api.Handler()

	// No auth header
	req := httptest.NewRequest(http.MethodGet, "/api/v1/regions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}

	// Wrong key
	req = httptest.NewRequest(http.MethodGet, "/api/v1/regions", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}

	// Correct key
	req = httptest.NewRequest(http.MethodGet, "/api/v1/regions", nil)
	req.Header.Set("Authorization", "Bearer secret-key")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestControlPlaneAPI_NoAuthRequired(t *testing.T) {
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/regions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 without auth, got %d", w.Code)
	}
}

func TestControlPlaneAPI_HandleTenants_GET(t *testing.T) {
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]any
	json.Unmarshal(w.Body.Bytes(), &body)
	if body["total"] == nil {
		t.Error("expected 'total' field in response")
	}
}

func TestControlPlaneAPI_HandleTenants_POST(t *testing.T) {
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	body := `{"name":"New Corp","email":"new@example.com","plan":"pro"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants", strings.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
}

func TestControlPlaneAPI_HandleTenants_MethodNotAllowed(t *testing.T) {
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/tenants", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestControlPlaneAPI_HandleTenantByID(t *testing.T) {
	cp, tenantID := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/"+tenantID, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestControlPlaneAPI_HandleTenantByID_NotFound(t *testing.T) {
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestControlPlaneAPI_HandleRegions_MethodNotAllowed(t *testing.T) {
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/regions", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestControlPlaneAPI_HandleBilling_GET(t *testing.T) {
	cp, tenantID := setupControlPlane(t)
	cp.SetupBilling(tenantID, BillingInfo{PlanID: "pro", Status: "active"})
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/billing/"+tenantID, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestControlPlaneAPI_HandleBilling_POST(t *testing.T) {
	cp, tenantID := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	billing, _ := json.Marshal(BillingInfo{PlanID: "enterprise", Status: "active"})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/billing/"+tenantID, bytes.NewReader(billing))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestControlPlaneAPI_HandleBilling_MethodNotAllowed(t *testing.T) {
	cp, tenantID := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/billing/"+tenantID, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestControlPlaneAPI_HandleQuotas_GET(t *testing.T) {
	cp, tenantID := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/quotas/"+tenantID, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestControlPlaneAPI_HandleQuotas_PUT(t *testing.T) {
	cp, tenantID := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	quota, _ := json.Marshal(ResourceQuota{MaxCPUMillicores: 8000})
	req := httptest.NewRequest(http.MethodPut, "/api/v1/quotas/"+tenantID, bytes.NewReader(quota))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestControlPlaneAPI_HandleQuotas_MethodNotAllowed(t *testing.T) {
	cp, tenantID := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/quotas/"+tenantID, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestControlPlaneAPI_HandleBilling_EmptyTenantID(t *testing.T) {
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/billing/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestControlPlaneAPI_HandleQuotas_EmptyTenantID(t *testing.T) {
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/quotas/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
