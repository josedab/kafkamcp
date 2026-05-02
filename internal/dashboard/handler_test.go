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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	if d == nil {
		t.Fatal("expected non-nil dashboard")
	}
}

func TestConnectedClientsInitial(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	if d.ConnectedClients() != 0 {
		t.Errorf("expected 0 connected clients, got %d", d.ConnectedClients())
	}
}

func TestBroadcastEventNoClients(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	// Should not panic
	d.BroadcastEvent("test", map[string]any{"data": "value"})
}

func TestBroadcastEventWithClients(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	handler := d.Handler()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestHandleSchemas(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	d.SetAllowNoAuth(true)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/schemas", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHandleRateLimits(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	d.SetAllowNoAuth(true)
	handler := d.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/ratelimits", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHandleMethodNotAllowed(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	d.SetAllowNoAuth(true)
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
	t.Parallel()
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

// --- SetAPIKey and requireAuth tests ---

func TestSetAPIKeyAndRequireAuth(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	d.SetAPIKey("my-secret-key")

	handler := d.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})

	t.Run("valid_bearer_token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("Authorization", "Bearer my-secret-key")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200 with valid bearer, got %d", w.Code)
		}
	})

	t.Run("valid_api_key_header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-API-Key", "my-secret-key")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200 with valid X-API-Key, got %d", w.Code)
		}
	})

	t.Run("missing_token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 for missing token, got %d", w.Code)
		}
	})

	t.Run("invalid_token", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("Authorization", "Bearer wrong-key")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 for invalid token, got %d", w.Code)
		}
	})

	t.Run("invalid_api_key", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-API-Key", "wrong-key")
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("expected 401 for invalid X-API-Key, got %d", w.Code)
		}
	})
}

func TestRequireAuthNoKey(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	d.SetAllowNoAuth(true)
	// No API key set — auth should pass through

	handler := d.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 when no API key set, got %d", w.Code)
	}
}

func TestSetAPIKeyEmpty(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	d.SetAPIKey("")

	handler := d.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Without allowNoAuth, empty API key rejects requests
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 when API key is empty and allowNoAuth is false, got %d", w.Code)
	}

	// With allowNoAuth, empty API key allows requests
	d.SetAllowNoAuth(true)
	req = httptest.NewRequest(http.MethodGet, "/test", nil)
	w = httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 when allowNoAuth is true, got %d", w.Code)
	}
}

func TestRequireAuthPartialBearerPrefix(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	d.SetAPIKey("test-key")

	handler := d.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for non-Bearer auth, got %d", w.Code)
	}
}

func TestAuthProtectsEndpoints(t *testing.T) {
	t.Parallel()
	rl := ratelimit.New(0)
	d := New(nil, nil, rl, slog.Default())
	d.SetAPIKey("secret")
	handler := d.Handler()

	endpoints := []string{"/api/v1/schemas", "/api/v1/ratelimits"}
	for _, ep := range endpoints {
		req := httptest.NewRequest(http.MethodGet, ep, nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusUnauthorized {
			t.Errorf("%s: expected 401 without auth, got %d", ep, w.Code)
		}
	}

	// With auth
	for _, ep := range endpoints {
		req := httptest.NewRequest(http.MethodGet, ep, nil)
		req.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("%s: expected 200 with auth, got %d", ep, w.Code)
		}
	}
}
