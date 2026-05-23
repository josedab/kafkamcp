package tenant

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --- handleBilling error paths ---

func TestHandleBilling_MalformedJSON(t *testing.T) {
	t.Parallel()
	cp, tenantID := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/billing/"+tenantID, strings.NewReader("{invalid json"))
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for malformed JSON, got %d", w.Code)
	}
}

func TestHandleBilling_EmptyBody(t *testing.T) {
	t.Parallel()
	cp, tenantID := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/billing/"+tenantID, strings.NewReader(""))
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Empty body causes JSON decode error
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty body, got %d", w.Code)
	}
}

func TestHandleBilling_GetNonExistent(t *testing.T) {
	t.Parallel()
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/billing/nonexistent", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

// --- handleQuotas error paths ---

func TestHandleQuotas_PutInvalidJSON(t *testing.T) {
	t.Parallel()
	cp, tenantID := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodPut, "/api/v1/quotas/"+tenantID, strings.NewReader("not json"))
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid JSON, got %d", w.Code)
	}
}

func TestHandleQuotas_GetNonExistentTenant(t *testing.T) {
	t.Parallel()
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/quotas/unknown-tenant", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should return defaults even for unknown tenant
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 (with defaults), got %d", w.Code)
	}
}

// --- handleTenants error paths ---

func TestHandleTenants_PostInvalidJSON(t *testing.T) {
	t.Parallel()
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants", strings.NewReader("{bad"))
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid JSON, got %d", w.Code)
	}
}

func TestHandleTenants_PostDuplicateName(t *testing.T) {
	t.Parallel()
	mgr := NewManager()
	cp := NewControlPlane(mgr)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	body := `{"name":"Test Corp","email":"test@test.com","plan":"pro"}`

	// First create
	req := httptest.NewRequest(http.MethodPost, "/api/v1/tenants", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("first create: expected 201, got %d", w.Code)
	}

	// Second create with same email should fail (email uniqueness)
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tenants", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-key")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("second create with same email: expected 400, got %d", w.Code)
	}
}

// --- writeJSON ---

func TestWriteJSON_ValidPayload(t *testing.T) {
	t.Parallel()
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "test-key")

	w := httptest.NewRecorder()
	api.writeJSON(w, map[string]string{"key": "value"})

	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected application/json, got %s", w.Header().Get("Content-Type"))
	}

	var result map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &result)
	if result["key"] != "value" {
		t.Errorf("expected value, got %s", result["key"])
	}
}

func TestWriteJSON_Nil(t *testing.T) {
	t.Parallel()
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "test-key")

	w := httptest.NewRecorder()
	api.writeJSON(w, nil)

	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected application/json, got %s", w.Header().Get("Content-Type"))
	}
}

// --- Invalid tenant ID format ---

func TestHandleBilling_InvalidTenantIDFormat(t *testing.T) {
	t.Parallel()
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	// Use a tenant ID with special chars that the regex rejects
	req := httptest.NewRequest(http.MethodGet, "/api/v1/billing/inv@lid!", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid tenant ID, got %d", w.Code)
	}
}

func TestHandleQuotas_InvalidTenantIDFormat(t *testing.T) {
	t.Parallel()
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/quotas/inv@lid!id", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid tenant ID, got %d", w.Code)
	}
}

// --- Security headers ---

func TestSecurityHeaders(t *testing.T) {
	t.Parallel()
	cp, _ := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/regions", nil)
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	expected := map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Cache-Control":          "no-store",
	}
	for k, v := range expected {
		if w.Header().Get(k) != v {
			t.Errorf("expected %s=%s, got %s", k, v, w.Header().Get(k))
		}
	}
}

// --- handleQuotas with valid data ---

func TestHandleQuotas_PutValid(t *testing.T) {
	t.Parallel()
	cp, tenantID := setupControlPlane(t)
	api := NewControlPlaneAPI(cp, "test-key")
	handler := api.Handler()

	quota := ResourceQuota{MaxCPUMillicores: 4000, MaxMemoryMB: 2048}
	body, _ := json.Marshal(quota)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/quotas/"+tenantID, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}
