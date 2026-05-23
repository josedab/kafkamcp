package tenant

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
)

var validTenantIDRegex = regexp.MustCompile(`^[a-zA-Z0-9_\-]{1,128}$`)

// ControlPlane manages the multi-tenant SaaS control plane.
type ControlPlane struct {
	mu           sync.RWMutex
	tenantMgr    *Manager
	regions      []RegionConfig
	billing      map[string]*BillingInfo
	quotas       map[string]*ResourceQuota
	ssoConfigs   map[string]*SSOConfig
	isolation    map[string]*NetworkIsolation
	usageRecords []UsageBillingRecord
}

// NewControlPlane creates a new control plane for the SaaS offering.
func NewControlPlane(tenantMgr *Manager) *ControlPlane {
	return &ControlPlane{
		tenantMgr:  tenantMgr,
		billing:    make(map[string]*BillingInfo),
		quotas:     make(map[string]*ResourceQuota),
		ssoConfigs: make(map[string]*SSOConfig),
		isolation:  make(map[string]*NetworkIsolation),
	}
}

// ControlPlaneAPI provides HTTP handlers for the control plane REST API.
type ControlPlaneAPI struct {
	cp     *ControlPlane
	apiKey string // Admin API key for control plane operations
}

// NewControlPlaneAPI creates a new control plane API handler.
func NewControlPlaneAPI(cp *ControlPlane, adminAPIKey string) *ControlPlaneAPI {
	if adminAPIKey == "" {
		slog.Warn("SECURITY WARNING: control plane API key is empty — all admin endpoints are unauthenticated")
	}
	return &ControlPlaneAPI{cp: cp, apiKey: adminAPIKey}
}

// Handler returns the HTTP handler for the control plane API.
func (api *ControlPlaneAPI) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/tenants", api.authMiddleware(api.handleTenants))
	mux.HandleFunc("/api/v1/tenants/", api.authMiddleware(api.handleTenantByID))
	mux.HandleFunc("/api/v1/regions", api.authMiddleware(api.handleRegions))
	mux.HandleFunc("/api/v1/billing/", api.authMiddleware(api.handleBilling))
	mux.HandleFunc("/api/v1/quotas/", api.authMiddleware(api.handleQuotas))

	// Wrap with security headers and body size limit (10 MB)
	return controlPlaneSecurityHeaders(http.MaxBytesHandler(mux, 10<<20))
}

// controlPlaneSecurityHeaders adds standard security headers to control plane HTTP responses.
func controlPlaneSecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Security-Policy", "default-src 'none'")
		w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		next.ServeHTTP(w, r)
	})
}

func (api *ControlPlaneAPI) authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if api.apiKey == "" {
			http.Error(w, "control plane not configured", http.StatusServiceUnavailable)
			return
		}
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		provided := strings.TrimPrefix(authHeader, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(provided), []byte(api.apiKey)) != 1 {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next(w, r)
	}
}

func (api *ControlPlaneAPI) handleTenants(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		tenants := api.cp.tenantMgr.ListTenants()
		api.writeJSON(w, map[string]any{"tenants": tenants, "total": len(tenants)})

	case http.MethodPost:
		var req struct {
			Name  string `json:"name"`
			Email string `json:"email"`
			Plan  string `json:"plan"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		tenant, apiKey, err := api.cp.tenantMgr.CreateTenant(req.Name, req.Email, req.Plan)
		if err != nil {
			slog.Error("failed to create tenant", "error", err)
			http.Error(w, "failed to create tenant", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusCreated)
		api.writeJSON(w, map[string]any{"tenant": tenant, "api_key": apiKey})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *ControlPlaneAPI) handleTenantByID(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/tenants/")
	if tenantID == "" {
		http.Error(w, "tenant ID required", http.StatusBadRequest)
		return
	}
	if !validTenantIDRegex.MatchString(tenantID) {
		http.Error(w, "invalid tenant ID format", http.StatusBadRequest)
		return
	}

	tenant, err := api.cp.tenantMgr.GetTenant(tenantID)
	if err != nil {
		slog.Error("tenant not found", "tenant_id", tenantID, "error", err)
		http.Error(w, "tenant not found", http.StatusNotFound)
		return
	}

	api.writeJSON(w, map[string]any{
		"tenant":  tenant,
		"quota":   api.cp.GetResourceQuota(tenantID),
		"network": api.cp.GetNetworkIsolation(tenantID),
	})
}

func (api *ControlPlaneAPI) handleRegions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	regions := api.cp.ListRegions()
	api.writeJSON(w, map[string]any{"regions": regions, "total": len(regions)})
}

func (api *ControlPlaneAPI) handleBilling(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/billing/")
	if tenantID == "" {
		http.Error(w, "tenant ID required", http.StatusBadRequest)
		return
	}
	if !validTenantIDRegex.MatchString(tenantID) {
		http.Error(w, "invalid tenant ID format", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		billing, err := api.cp.GetBilling(tenantID)
		if err != nil {
			slog.Error("billing not found", "tenant_id", tenantID, "error", err)
			http.Error(w, "billing information not found", http.StatusNotFound)
			return
		}
		usage := api.cp.GetUsageRecords(tenantID)
		api.writeJSON(w, map[string]any{"billing": billing, "usage_records": usage})

	case http.MethodPost:
		var billing BillingInfo
		if err := json.NewDecoder(r.Body).Decode(&billing); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if err := api.cp.SetupBilling(tenantID, billing); err != nil {
			slog.Error("failed to setup billing", "tenant_id", tenantID, "error", err)
			http.Error(w, "failed to configure billing", http.StatusBadRequest)
			return
		}
		api.writeJSON(w, map[string]any{"status": "billing configured"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *ControlPlaneAPI) handleQuotas(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimPrefix(r.URL.Path, "/api/v1/quotas/")
	if tenantID == "" {
		http.Error(w, "tenant ID required", http.StatusBadRequest)
		return
	}
	if !validTenantIDRegex.MatchString(tenantID) {
		http.Error(w, "invalid tenant ID format", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		quota := api.cp.GetResourceQuota(tenantID)
		api.writeJSON(w, map[string]any{"quota": quota})

	case http.MethodPut:
		var quota ResourceQuota
		if err := json.NewDecoder(r.Body).Decode(&quota); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if err := api.cp.SetResourceQuota(tenantID, quota); err != nil {
			slog.Error("failed to set quota", "tenant_id", tenantID, "error", err)
			http.Error(w, "failed to update quota", http.StatusBadRequest)
			return
		}
		api.writeJSON(w, map[string]any{"status": "quota updated"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (api *ControlPlaneAPI) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

// GenerateAdminAPIKey generates a new admin API key.
func GenerateAdminAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("kmcp_admin_%s", hex.EncodeToString(b)), nil
}
