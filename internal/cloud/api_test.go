package cloud

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func apiTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func setupAPI() (*APIHandler, *CloudService) {
	svc := NewCloudService(apiTestLogger())
	h := NewAPIHandler(svc)
	return h, svc
}

// --- handleWorkspaces ---

func TestHandleWorkspacesGET(t *testing.T) {
	h, svc := setupAPI()
	svc.CreateWorkspace("ws1", "a@b.com", "free")

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/workspaces", nil)
	w := httptest.NewRecorder()
	h.handleWorkspaces(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["total"].(float64) != 1 {
		t.Errorf("expected total 1, got %v", body["total"])
	}
}

func TestHandleWorkspacesPOST(t *testing.T) {
	h, _ := setupAPI()

	payload := `{"name":"test-ws","owner_email":"u@x.com","plan":"pro"}`
	req := httptest.NewRequest(http.MethodPost, "/api/cloud/v1/workspaces", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	h.handleWorkspaces(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["api_key"] == nil {
		t.Error("expected api_key in response")
	}
}

func TestHandleWorkspacesPOSTInvalidJSON(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodPost, "/api/cloud/v1/workspaces", bytes.NewBufferString("{invalid"))
	w := httptest.NewRecorder()
	h.handleWorkspaces(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleWorkspacesMethodNotAllowed(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodDelete, "/api/cloud/v1/workspaces", nil)
	w := httptest.NewRecorder()
	h.handleWorkspaces(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

// --- handleWorkspaceByID ---

func TestHandleWorkspaceByIDGet(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "free")

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/workspaces/"+ws.ID, nil)
	w := httptest.NewRecorder()
	h.handleWorkspaceByID(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleWorkspaceByIDNotFound(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/workspaces/nonexistent", nil)
	w := httptest.NewRecorder()
	h.handleWorkspaceByID(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandleWorkspaceByIDEmptyID(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/workspaces/", nil)
	w := httptest.NewRecorder()
	h.handleWorkspaceByID(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleWorkspaceByIDClusters(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "free")

	payload := `{"name":"cl1","bootstrap_servers":"localhost:9092","security_protocol":"PLAINTEXT","region":"us-east-1"}`
	req := httptest.NewRequest(http.MethodPost, "/api/cloud/v1/workspaces/"+ws.ID+"/clusters", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	h.handleWorkspaceByID(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
}

func TestHandleWorkspaceByIDMembers(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "free")

	payload := `{"email":"b@c.com","role":"admin"}`
	req := httptest.NewRequest(http.MethodPost, "/api/cloud/v1/workspaces/"+ws.ID+"/members", bytes.NewBufferString(payload))
	w := httptest.NewRecorder()
	h.handleWorkspaceByID(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", w.Code)
	}
}

func TestHandleWorkspaceByIDDashboard(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "free")

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/workspaces/"+ws.ID+"/dashboard", nil)
	w := httptest.NewRecorder()
	h.handleWorkspaceByID(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleWorkspaceByIDDelete(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "free")

	req := httptest.NewRequest(http.MethodDelete, "/api/cloud/v1/workspaces/"+ws.ID, nil)
	w := httptest.NewRecorder()
	h.handleWorkspaceByID(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleWorkspaceByIDUnknownSubpath(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "free")

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/workspaces/"+ws.ID+"/unknown", nil)
	w := httptest.NewRecorder()
	h.handleWorkspaceByID(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

// --- handleAuth ---

func TestHandleAuthValid(t *testing.T) {
	h, svc := setupAPI()
	_, apiKey, _ := svc.CreateWorkspace("ws1", "a@b.com", "pro")

	req := httptest.NewRequest(http.MethodPost, "/api/cloud/v1/auth", nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	w := httptest.NewRecorder()
	h.handleAuth(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleAuthMissingKey(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodPost, "/api/cloud/v1/auth", nil)
	w := httptest.NewRecorder()
	h.handleAuth(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestHandleAuthInvalidKey(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodPost, "/api/cloud/v1/auth", nil)
	req.Header.Set("Authorization", "Bearer invalid-key-12345")
	w := httptest.NewRecorder()
	h.handleAuth(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestHandleAuthMethodNotAllowed(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/auth", nil)
	w := httptest.NewRecorder()
	h.handleAuth(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

// --- handleBilling ---

func TestHandleBillingValid(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "pro")

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/billing?workspace_id="+ws.ID, nil)
	w := httptest.NewRecorder()
	h.handleBilling(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleBillingMissingWorkspace(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/billing", nil)
	w := httptest.NewRecorder()
	h.handleBilling(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleBillingNotFound(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/billing?workspace_id=nonexistent", nil)
	w := httptest.NewRecorder()
	h.handleBilling(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

// --- handleProvision ---

func TestHandleProvisionValid(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "pro")

	payload, _ := json.Marshal(map[string]string{"workspace_id": ws.ID, "region": "eu-west-1", "size": "medium"})
	req := httptest.NewRequest(http.MethodPost, "/api/cloud/v1/provision", bytes.NewBuffer(payload))
	w := httptest.NewRecorder()
	h.handleProvision(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["region"] != "eu-west-1" {
		t.Errorf("expected region eu-west-1, got %v", body["region"])
	}
}

func TestHandleProvisionDefaultRegion(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "enterprise")

	payload, _ := json.Marshal(map[string]string{"workspace_id": ws.ID})
	req := httptest.NewRequest(http.MethodPost, "/api/cloud/v1/provision", bytes.NewBuffer(payload))
	w := httptest.NewRecorder()
	h.handleProvision(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["region"] != "us-east-1" {
		t.Errorf("expected default region us-east-1, got %v", body["region"])
	}
}

func TestHandleProvisionFreePlanForbidden(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "free")

	payload, _ := json.Marshal(map[string]string{"workspace_id": ws.ID})
	req := httptest.NewRequest(http.MethodPost, "/api/cloud/v1/provision", bytes.NewBuffer(payload))
	w := httptest.NewRecorder()
	h.handleProvision(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
}

// --- extractAPIKey ---

func TestExtractAPIKeyFromBearer(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer mytoken123")
	key := extractAPIKey(req)
	if key != "mytoken123" {
		t.Errorf("expected mytoken123, got %s", key)
	}
}

func TestExtractAPIKeyFromXAPIKeyHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-API-Key", "headerkey")
	key := extractAPIKey(req)
	if key != "headerkey" {
		t.Errorf("expected headerkey, got %s", key)
	}
}

func TestExtractAPIKeyEmpty(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	key := extractAPIKey(req)
	if key != "" {
		t.Errorf("expected empty, got %s", key)
	}
}

// --- writeCloudJSON / writeCloudError ---

func TestWriteCloudJSON(t *testing.T) {
	w := httptest.NewRecorder()
	writeCloudJSON(w, http.StatusOK, map[string]string{"key": "value"})

	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected application/json content-type, got %s", w.Header().Get("Content-Type"))
	}
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestWriteCloudError(t *testing.T) {
	w := httptest.NewRecorder()
	writeCloudError(w, http.StatusBadRequest, "bad request")

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["error"] != "bad request" {
		t.Errorf("expected error 'bad request', got %s", body["error"])
	}
}

// --- handleUsage ---

func TestHandleUsageValid(t *testing.T) {
	h, svc := setupAPI()
	ws, _, _ := svc.CreateWorkspace("ws1", "a@b.com", "free")

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/usage?workspace_id="+ws.ID, nil)
	w := httptest.NewRecorder()
	h.handleUsage(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleUsageMissingID(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/usage", nil)
	w := httptest.NewRecorder()
	h.handleUsage(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- handlePlans ---

func TestHandlePlans(t *testing.T) {
	h, _ := setupAPI()

	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/plans", nil)
	w := httptest.NewRecorder()
	h.handlePlans(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	plans := body["plans"].([]any)
	if len(plans) != 3 {
		t.Errorf("expected 3 plans, got %d", len(plans))
	}
}

// --- RegisterRoutes ---

func TestRegisterRoutes(t *testing.T) {
	h, _ := setupAPI()
	mux := http.NewServeMux()
	h.RegisterRoutes(mux, nil)

	// Verify routes are registered by making a request
	req := httptest.NewRequest(http.MethodGet, "/api/cloud/v1/plans", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}
