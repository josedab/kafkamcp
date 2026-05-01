package cloud

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// APIHandler provides HTTP REST API endpoints for the managed cloud service.
type APIHandler struct {
	service *CloudService
}

const maxCloudRequestSize = 1 << 20 // 1 MB

// NewAPIHandler creates a new cloud service API handler.
func NewAPIHandler(service *CloudService) *APIHandler {
	return &APIHandler{service: service}
}

// RegisterRoutes adds cloud service API routes to the given mux.
func (h *APIHandler) RegisterRoutes(mux *http.ServeMux, authMiddleware func(http.HandlerFunc) http.HandlerFunc) {
	wrap := authMiddleware
	if wrap == nil {
		wrap = func(h http.HandlerFunc) http.HandlerFunc { return h }
	}

	mux.HandleFunc("/api/cloud/v1/workspaces", wrap(h.handleWorkspaces))
	mux.HandleFunc("/api/cloud/v1/workspaces/", wrap(h.handleWorkspaceByID))
	mux.HandleFunc("/api/cloud/v1/auth", wrap(h.handleAuth))
	mux.HandleFunc("/api/cloud/v1/billing", wrap(h.handleBilling))
	mux.HandleFunc("/api/cloud/v1/usage", wrap(h.handleUsage))
	mux.HandleFunc("/api/cloud/v1/plans", wrap(h.handlePlans))
	mux.HandleFunc("/api/cloud/v1/provision", wrap(h.handleProvision))
}

func (h *APIHandler) handleWorkspaces(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		workspaces := h.service.ListWorkspaces()
		writeCloudJSON(w, http.StatusOK, map[string]any{
			"workspaces": workspaces,
			"total":      len(workspaces),
		})
	case http.MethodPost:
		var req struct {
			Name       string `json:"name"`
			OwnerEmail string `json:"owner_email"`
			Plan       string `json:"plan"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCloudRequestSize)).Decode(&req); err != nil {
			writeCloudError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		ws, apiKey, err := h.service.CreateWorkspace(req.Name, req.OwnerEmail, req.Plan)
		if err != nil {
			writeCloudError(w, http.StatusBadRequest, "failed to create workspace")
			return
		}
		writeCloudJSON(w, http.StatusCreated, map[string]any{
			"workspace": ws,
			"api_key":   apiKey,
			"message":   "Store the API key securely. It will not be shown again.",
		})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *APIHandler) handleWorkspaceByID(w http.ResponseWriter, r *http.Request) {
	wsID := strings.TrimPrefix(r.URL.Path, "/api/cloud/v1/workspaces/")
	if wsID == "" {
		writeCloudError(w, http.StatusBadRequest, "workspace ID required")
		return
	}

	// Strip sub-paths for nested operations
	parts := strings.SplitN(wsID, "/", 2)
	wsID = parts[0]
	subPath := ""
	if len(parts) > 1 {
		subPath = parts[1]
	}

	switch {
	case subPath == "clusters" && r.Method == http.MethodPost:
		var req struct {
			Name             string `json:"name"`
			BootstrapServers string `json:"bootstrap_servers"`
			SecurityProtocol string `json:"security_protocol"`
			Region           string `json:"region"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCloudRequestSize)).Decode(&req); err != nil {
			writeCloudError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		err := h.service.AddCluster(wsID, ClusterConnection{
			Name:             req.Name,
			BootstrapServers: req.BootstrapServers,
			SecurityProtocol: req.SecurityProtocol,
			Region:           req.Region,
		})
		if err != nil {
			writeCloudError(w, http.StatusBadRequest, "failed to add cluster")
			return
		}
		writeCloudJSON(w, http.StatusCreated, map[string]string{"status": "cluster added"})

	case subPath == "members" && r.Method == http.MethodPost:
		var req struct {
			Email string `json:"email"`
			Role  string `json:"role"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCloudRequestSize)).Decode(&req); err != nil {
			writeCloudError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		err := h.service.AddMember(wsID, req.Email, req.Role)
		if err != nil {
			writeCloudError(w, http.StatusBadRequest, "failed to add member")
			return
		}
		writeCloudJSON(w, http.StatusCreated, map[string]string{"status": "member added"})

	case subPath == "dashboard" && r.Method == http.MethodGet:
		data, err := h.service.GetDashboardData(wsID)
		if err != nil {
			writeCloudError(w, http.StatusNotFound, "workspace not found")
			return
		}
		writeCloudJSON(w, http.StatusOK, data)

	case subPath == "" && r.Method == http.MethodGet:
		ws, err := h.service.GetWorkspace(wsID)
		if err != nil {
			writeCloudError(w, http.StatusNotFound, "workspace not found")
			return
		}
		writeCloudJSON(w, http.StatusOK, ws)

	case subPath == "" && r.Method == http.MethodDelete:
		err := h.service.DeleteWorkspace(wsID)
		if err != nil {
			writeCloudError(w, http.StatusNotFound, "workspace not found")
			return
		}
		writeCloudJSON(w, http.StatusOK, map[string]string{"status": "deleted"})

	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (h *APIHandler) handleAuth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	apiKey := extractAPIKey(r)
	if apiKey == "" {
		writeCloudError(w, http.StatusUnauthorized, "API key required")
		return
	}

	wsID, err := h.service.AuthenticateAPIKey(apiKey)
	if err != nil {
		writeCloudError(w, http.StatusUnauthorized, "invalid API key")
		return
	}

	ws, err := h.service.GetWorkspace(wsID)
	if err != nil {
		writeCloudError(w, http.StatusInternalServerError, "workspace not found")
		return
	}

	writeCloudJSON(w, http.StatusOK, map[string]any{
		"workspace_id": ws.ID,
		"workspace":    ws.Name,
		"plan":         ws.Plan,
		"status":       ws.Status,
	})
}

func (h *APIHandler) handleBilling(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	wsID := r.URL.Query().Get("workspace_id")
	if wsID == "" {
		writeCloudError(w, http.StatusBadRequest, "workspace_id required")
		return
	}

	ws, err := h.service.GetWorkspace(wsID)
	if err != nil {
		writeCloudError(w, http.StatusNotFound, "workspace not found")
		return
	}

	limits := GetPlanLimits(ws.Plan)
	billing := BillingInfo{
		Plan:          ws.Plan,
		BillingCycle:  "monthly",
		NextBillingAt: time.Now().AddDate(0, 1, 0),
	}

	switch ws.Plan {
	case "pro":
		billing.AmountCents = 9900 // $99/mo
	case "enterprise":
		billing.AmountCents = 49900 // $499/mo
	}

	writeCloudJSON(w, http.StatusOK, map[string]any{
		"billing":     billing,
		"plan_limits": limits,
		"usage":       ws.Usage,
	})
}

func (h *APIHandler) handleUsage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	wsID := r.URL.Query().Get("workspace_id")
	if wsID == "" {
		writeCloudError(w, http.StatusBadRequest, "workspace_id required")
		return
	}

	data, err := h.service.GetDashboardData(wsID)
	if err != nil {
		writeCloudError(w, http.StatusNotFound, "workspace not found")
		return
	}
	writeCloudJSON(w, http.StatusOK, data)
}

func (h *APIHandler) handlePlans(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	plans := map[string]any{
		"plans": []map[string]any{
			{
				"id":     "free",
				"name":   "Free",
				"price":  "$0/month",
				"limits": GetPlanLimits("free"),
			},
			{
				"id":     "pro",
				"name":   "Pro",
				"price":  "$99/month",
				"limits": GetPlanLimits("pro"),
			},
			{
				"id":     "enterprise",
				"name":   "Enterprise",
				"price":  "$499/month",
				"limits": GetPlanLimits("enterprise"),
			},
		},
	}
	writeCloudJSON(w, http.StatusOK, plans)
}

func (h *APIHandler) handleProvision(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		WorkspaceID string `json:"workspace_id"`
		Region      string `json:"region"`
		Size        string `json:"size"` // small, medium, large
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCloudRequestSize)).Decode(&req); err != nil {
		writeCloudError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	ws, err := h.service.GetWorkspace(req.WorkspaceID)
	if err != nil {
		writeCloudError(w, http.StatusNotFound, "workspace not found")
		return
	}

	if ws.Plan == "free" {
		writeCloudError(w, http.StatusForbidden, "provisioning requires Pro or Enterprise plan")
		return
	}

	region := req.Region
	if region == "" {
		region = "us-east-1"
	}
	size := req.Size
	if size == "" {
		size = "small"
	}

	provisionID, err := generateID("prov")
	if err != nil {
		writeCloudJSON(w, http.StatusInternalServerError, map[string]any{"error": "failed to generate provision ID"})
		return
	}
	writeCloudJSON(w, http.StatusAccepted, map[string]any{
		"provision_id":  provisionID,
		"workspace_id":  req.WorkspaceID,
		"region":        region,
		"size":          size,
		"status":        "provisioning",
		"estimated_eta": "2-5 minutes",
	})
}

func extractAPIKey(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}
	return r.Header.Get("X-API-Key")
}

func writeCloudJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("failed to encode cloud JSON response", "error", err)
	}
}

func writeCloudError(w http.ResponseWriter, status int, msg string) {
	writeCloudJSON(w, status, map[string]string{"error": msg})
}
