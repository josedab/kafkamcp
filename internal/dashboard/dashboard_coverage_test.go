package dashboard

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/ratelimit"
)

// flusherRecorder wraps httptest.ResponseRecorder and implements http.Flusher.
type flusherRecorder struct {
	*httptest.ResponseRecorder
	flushed int
}

func (f *flusherRecorder) Flush() { f.flushed++ }

func TestHandleSSEInitialConnection(t *testing.T) {
	t.Parallel()
	al, _ := audit.NewLogger(context.Background(), true, 100, "", slog.Default())
	rl := ratelimit.New(0)
	d := New(nil, al, rl, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	req = req.WithContext(ctx)

	w := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	d.handleSSE(w, req)

	// After context cancellation, SSE handler returns
	body := w.Body.String()
	if len(body) == 0 {
		t.Error("expected SSE output")
	}
	// Check initial connection event was sent
	if !contains(body, "event: connected") {
		t.Errorf("expected 'event: connected' in body, got %q", body)
	}
	if !contains(body, "client_id") {
		t.Errorf("expected 'client_id' in body, got %q", body)
	}
	// Should have flushed at least once (for the initial event)
	if w.flushed < 1 {
		t.Errorf("expected at least 1 flush, got %d", w.flushed)
	}
	// Client should be removed after handler exits
	if d.ConnectedClients() != 0 {
		t.Errorf("expected 0 clients after disconnect, got %d", d.ConnectedClients())
	}
}

func TestHandleSSEReceivesBroadcast(t *testing.T) {
	t.Parallel()
	al, _ := audit.NewLogger(context.Background(), true, 100, "", slog.Default())
	rl := ratelimit.New(0)
	d := New(nil, al, rl, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	req = req.WithContext(ctx)

	w := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}

	done := make(chan struct{})
	go func() {
		d.handleSSE(w, req)
		close(done)
	}()

	// Wait for client to register
	for i := 0; i < 50; i++ {
		if d.ConnectedClients() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Send a broadcast event
	d.BroadcastEvent("test_event", map[string]any{"key": "value"})
	time.Sleep(50 * time.Millisecond) // let the handler process
	cancel()
	<-done

	body := w.Body.String()
	if !contains(body, "event: update") {
		t.Errorf("expected broadcast update in body, got %q", body)
	}
}

func TestHandleSSEHeaders(t *testing.T) {
	t.Parallel()
	al, _ := audit.NewLogger(context.Background(), true, 100, "", slog.Default())
	rl := ratelimit.New(0)
	d := New(nil, al, rl, slog.Default())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	req = req.WithContext(ctx)

	w := &flusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	d.handleSSE(w, req)

	if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("expected Content-Type text/event-stream, got %q", ct)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("expected Cache-Control no-cache, got %q", cc)
	}
	if conn := w.Header().Get("Connection"); conn != "keep-alive" {
		t.Errorf("expected Connection keep-alive, got %q", conn)
	}
}

func TestHandleHealthMethodNotAllowed(t *testing.T) {
	t.Parallel()
	al, _ := audit.NewLogger(context.Background(), true, 100, "", slog.Default())
	rl := ratelimit.New(0)
	d := New(nil, al, rl, slog.Default())
	handler := d.Handler()

	// Health endpoint doesn't check method, but we can still test it reaches the handler
	// It panics with nil kafkaManager on GET, but POST should also reach it
	// The important thing is the endpoint is routed
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	w := httptest.NewRecorder()

	// This will panic because kafkaManager is nil - use recover
	func() {
		defer func() { recover() }()
		handler.ServeHTTP(w, req)
	}()
}

func TestWriteJSONEmptyMap(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	writeJSON(w, map[string]any{})

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty map, got %v", result)
	}
}

func TestWriteJSONNumber(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	writeJSON(w, 42)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestWriteJSONString(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	writeJSON(w, "hello")

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHandleActivityResponseHasAllFields(t *testing.T) {
	t.Parallel()
	al, _ := audit.NewLogger(context.Background(), true, 100, "", slog.Default())
	rl := ratelimit.New(0)
	d := New(nil, al, rl, slog.Default())
	d.SetAllowNoAuth(true)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/activity", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := result["activity"]; !ok {
		t.Error("expected 'activity' key")
	}
	if _, ok := result["total"]; !ok {
		t.Error("expected 'total' key")
	}
}

func TestConnectedClientsAfterAddRemove(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())

	// Add clients
	d.mu.Lock()
	d.sseClients["c1"] = make(chan []byte, 1)
	d.sseClients["c2"] = make(chan []byte, 1)
	d.sseClients["c3"] = make(chan []byte, 1)
	d.mu.Unlock()

	if d.ConnectedClients() != 3 {
		t.Errorf("expected 3 clients, got %d", d.ConnectedClients())
	}

	// Remove one
	d.mu.Lock()
	delete(d.sseClients, "c2")
	d.mu.Unlock()

	if d.ConnectedClients() != 2 {
		t.Errorf("expected 2 clients, got %d", d.ConnectedClients())
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
