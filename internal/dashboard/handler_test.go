package dashboard

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/ratelimit"
)

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, map[string]any{"key": "value"})

	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("expected application/json content type, got %q", w.Header().Get("Content-Type"))
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("expected key=value, got %v", result["key"])
	}
}

func TestWriteJSONComplex(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSON(w, map[string]any{
		"topics": []string{"a", "b"},
		"count":  2,
	})

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["count"].(float64) != 2 {
		t.Errorf("expected count=2, got %v", result["count"])
	}
}

func TestNewDashboard(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	if d == nil {
		t.Fatal("expected non-nil dashboard")
	}
}

func TestConnectedClientsInitial(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	if d.ConnectedClients() != 0 {
		t.Errorf("expected 0 connected clients, got %d", d.ConnectedClients())
	}
}

func TestBroadcastEventNoClients(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	// Should not panic
	d.BroadcastEvent("test", map[string]any{"data": "value"})
}

func TestBroadcastEventWithClients(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())

	ch := make(chan []byte, 10)
	d.mu.Lock()
	d.sseClients["test-client"] = ch
	d.mu.Unlock()

	d.BroadcastEvent("update", map[string]any{"topic": "orders"})

	select {
	case data := <-ch:
		var event map[string]any
		if err := json.Unmarshal(data, &event); err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}
		if event["type"] != "update" {
			t.Errorf("expected event type 'update', got %v", event["type"])
		}
	default:
		t.Error("expected broadcast event on channel")
	}

	if d.ConnectedClients() != 1 {
		t.Errorf("expected 1 connected client, got %d", d.ConnectedClients())
	}
}

func TestHandlerReturnsHTTPHandler(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	handler := d.Handler()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestHandleSchemas(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/schemas", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHandleRateLimits(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ratelimits", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHandleMethodNotAllowed(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	handler := d.Handler()

	endpoints := []string{"/api/v1/schemas", "/api/v1/ratelimits"}
	for _, ep := range endpoints {
		req := httptest.NewRequest(http.MethodPost, ep, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s POST: expected 405, got %d", ep, w.Code)
		}
	}
}

func TestBroadcastEventFullChannel(t *testing.T) {
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())

	ch := make(chan []byte, 1)
	ch <- []byte("existing")
	d.mu.Lock()
	d.sseClients["full-client"] = ch
	d.mu.Unlock()

	// Should not block or panic
	d.BroadcastEvent("test", map[string]any{"data": "dropped"})
}
