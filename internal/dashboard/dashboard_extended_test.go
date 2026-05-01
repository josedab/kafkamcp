package dashboard

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/ratelimit"
)

// newTestDashboard creates a dashboard with a real audit logger for testing.
func newTestDashboard(t *testing.T) *Dashboard {
	t.Helper()
	rl := ratelimit.New(0)
	al, err := audit.NewLogger(context.Background(), true, 100, "", slog.Default())
	if err != nil {
		t.Fatalf("failed to create audit logger: %v", err)
	}
	d := New(nil, al, rl, slog.Default())
	d.SetAllowNoAuth(true)
	return d
}

// --- handleActivity tests ---

func TestHandleActivityGET(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/activity", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if _, ok := result["activity"]; !ok {
		t.Error("expected 'activity' key in response")
	}
	if _, ok := result["total"]; !ok {
		t.Error("expected 'total' key in response")
	}
}

func TestHandleActivityMethodNotAllowed(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/api/v1/activity", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("activity %s: expected 405, got %d", method, w.Code)
		}
	}
}

// --- handleAuditTrail tests ---

func TestHandleAuditTrailGET(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/audit", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if _, ok := result["entries"]; !ok {
		t.Error("expected 'entries' key in response")
	}
	if _, ok := result["total"]; !ok {
		t.Error("expected 'total' key in response")
	}
}

func TestHandleAuditTrailMethodNotAllowed(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/api/v1/audit", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("audit %s: expected 405, got %d", method, w.Code)
		}
	}
}

// --- handleTopics / handleTopology / handleHealth method-not-allowed tests ---

func TestHandleTopicsMethodNotAllowed(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/api/v1/topics", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("topics %s: expected 405, got %d", method, w.Code)
		}
	}
}

func TestHandleTopologyMethodNotAllowed(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req := httptest.NewRequest(method, "/api/v1/topology", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("topology %s: expected 405, got %d", method, w.Code)
		}
	}
}

// --- handleSchemas method-not-allowed (extended methods) ---

func TestHandleSchemasMethodNotAllowed(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/api/v1/schemas", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("schemas %s: expected 405, got %d", method, w.Code)
		}
	}
}

// --- handleRateLimits method-not-allowed (extended methods) ---

func TestHandleRateLimitsMethodNotAllowed(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/api/v1/ratelimits", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("ratelimits %s: expected 405, got %d", method, w.Code)
		}
	}
}

// --- handleSSE tests ---

// nonFlusherWriter implements http.ResponseWriter but NOT http.Flusher.
type nonFlusherWriter struct {
	code    int
	headers http.Header
	body    []byte
}

func (w *nonFlusherWriter) Header() http.Header         { return w.headers }
func (w *nonFlusherWriter) Write(b []byte) (int, error)  { w.body = append(w.body, b...); return len(b), nil }
func (w *nonFlusherWriter) WriteHeader(code int)         { w.code = code }

func TestHandleSSENoFlusher(t *testing.T) {
	d := newTestDashboard(t)

	w := &nonFlusherWriter{headers: make(http.Header)}
	req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	d.handleSSE(w, req)

	if w.code != http.StatusInternalServerError {
		t.Errorf("expected 500 for non-flusher writer, got %d", w.code)
	}
}

// --- Handler() routing tests ---

func TestHandlerRoutesRespondToRequests(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	endpoints := []string{
		"/api/v1/schemas",
		"/api/v1/ratelimits",
		"/api/v1/activity",
		"/api/v1/audit",
	}

	for _, ep := range endpoints {
		req := httptest.NewRequest(http.MethodGet, ep, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("%s: expected 200, got %d", ep, w.Code)
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("%s: expected Content-Type application/json, got %q", ep, ct)
		}
	}
}

// --- All endpoints 405 for all non-GET methods (comprehensive) ---

func TestAllEndpointsMethodNotAllowed(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	endpoints := []string{
		"/api/v1/activity",
		"/api/v1/audit",
		"/api/v1/topics",
		"/api/v1/topology",
		"/api/v1/schemas",
		"/api/v1/ratelimits",
	}
	methods := []string{http.MethodPost, http.MethodPut, http.MethodDelete}

	for _, ep := range endpoints {
		for _, method := range methods {
			req := httptest.NewRequest(method, ep, nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s: expected 405, got %d", method, ep, w.Code)
			}
		}
	}
}

// --- writeJSON edge cases ---

func TestWriteJSONNilValue(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, nil)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestWriteJSONSlice(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, []string{"a", "b", "c"})

	var result []string
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if len(result) != 3 {
		t.Errorf("expected 3 items, got %d", len(result))
	}
}

func TestWriteJSONNestedMap(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, map[string]any{
		"outer": map[string]any{
			"inner": 42,
		},
	})

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	outer, ok := result["outer"].(map[string]any)
	if !ok {
		t.Fatal("expected nested map")
	}
	if outer["inner"].(float64) != 42 {
		t.Errorf("expected inner=42, got %v", outer["inner"])
	}
}

// --- BroadcastEvent edge cases ---

func TestBroadcastEventUnmarshalableData(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())

	ch := make(chan []byte, 10)
	d.mu.Lock()
	d.sseClients["test-client"] = ch
	d.mu.Unlock()

	// Channels can't be marshaled to JSON; BroadcastEvent should not panic
	d.BroadcastEvent("test", make(chan int))

	select {
	case <-ch:
		t.Error("expected no broadcast for unmarshalable data")
	default:
		// expected: json.Marshal fails silently
	}
}

func TestBroadcastEventMultipleClients(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())

	ch1 := make(chan []byte, 10)
	ch2 := make(chan []byte, 10)
	d.mu.Lock()
	d.sseClients["client-1"] = ch1
	d.sseClients["client-2"] = ch2
	d.mu.Unlock()

	d.BroadcastEvent("ping", map[string]any{"msg": "hello"})

	if d.ConnectedClients() != 2 {
		t.Errorf("expected 2 clients, got %d", d.ConnectedClients())
	}

	for _, ch := range []chan []byte{ch1, ch2} {
		select {
		case data := <-ch:
			var event map[string]any
			if err := json.Unmarshal(data, &event); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}
			if event["type"] != "ping" {
				t.Errorf("expected type=ping, got %v", event["type"])
			}
		default:
			t.Error("expected event on channel")
		}
	}
}

// --- Schemas response content ---

func TestHandleSchemasResponseContent(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/schemas", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if _, ok := result["message"]; !ok {
		t.Error("expected 'message' key in schema response")
	}
}

// --- RateLimits response content ---

func TestHandleRateLimitsResponseContent(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ratelimits", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if _, ok := result["rate_limits"]; !ok {
		t.Error("expected 'rate_limits' key in response")
	}
}

// --- Activity response with empty audit log ---

func TestHandleActivityEmptyAuditLog(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/activity", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	total := result["total"].(float64)
	if total != 0 {
		t.Errorf("expected total=0 for empty audit log, got %v", total)
	}
}

// --- AuditTrail response with empty audit log ---

func TestHandleAuditTrailEmptyAuditLog(t *testing.T) {
	d := newTestDashboard(t)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/audit", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	total := result["total"].(float64)
	if total != 0 {
		t.Errorf("expected total=0 for empty audit log, got %v", total)
	}
}
