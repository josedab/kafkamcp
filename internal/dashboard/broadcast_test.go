package dashboard

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestBroadcastEvent_ConcurrentDisconnect(t *testing.T) {
	t.Parallel()
	d := New(nil, nil, nil, slog.Default())

	// Simulate multiple connected clients
	for i := 0; i < 10; i++ {
		ch := make(chan []byte, 10)
		d.mu.Lock()
		id := fmt.Sprintf("test-client-%d", i)
		d.sseClients[id] = ch
		d.mu.Unlock()
	}

	var wg sync.WaitGroup
	// Concurrently broadcast and disconnect
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			d.BroadcastEvent("test", map[string]string{"key": "value"})
		}
	}()
	go func() {
		defer wg.Done()
		d.mu.Lock()
		for id := range d.sseClients {
			delete(d.sseClients, id)
		}
		d.mu.Unlock()
	}()
	wg.Wait()
	// Should not panic
}

func TestConnectedClients_AfterConnectDisconnect(t *testing.T) {
	t.Parallel()
	d := New(nil, nil, nil, slog.Default())

	if d.ConnectedClients() != 0 {
		t.Errorf("expected 0 initial clients, got %d", d.ConnectedClients())
	}

	d.mu.Lock()
	d.sseClients["c1"] = make(chan []byte, 1)
	d.sseClients["c2"] = make(chan []byte, 1)
	d.mu.Unlock()

	if d.ConnectedClients() != 2 {
		t.Errorf("expected 2 clients, got %d", d.ConnectedClients())
	}

	d.mu.Lock()
	delete(d.sseClients, "c1")
	d.mu.Unlock()

	if d.ConnectedClients() != 1 {
		t.Errorf("expected 1 client after disconnect, got %d", d.ConnectedClients())
	}
}

func TestWriteJSON_ContentTypeHeader(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	writeJSON(w, map[string]string{"status": "ok"})

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}

	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if result["status"] != "ok" {
		t.Errorf("expected status=ok, got %s", result["status"])
	}
}

func TestWriteJSON_UnmarshalableData(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	// math.Inf is not JSON-serializable
	writeJSON(w, math.Inf(1))

	if w.Code != 500 {
		t.Errorf("expected status 500, got %d", w.Code)
	}
}

func TestBroadcastEvent_ZeroClients(t *testing.T) {
	t.Parallel()
	d := New(nil, nil, nil, slog.Default())
	// Should not panic with no clients
	d.BroadcastEvent("test", map[string]string{"key": "value"})
}

func TestBroadcastEvent_InvalidData(t *testing.T) {
	t.Parallel()
	d := New(nil, nil, nil, slog.Default())
	d.mu.Lock()
	d.sseClients["c1"] = make(chan []byte, 1)
	d.mu.Unlock()

	// math.Inf can't be marshaled to JSON; BroadcastEvent should silently return
	d.BroadcastEvent("test", math.Inf(1))

	d.mu.RLock()
	ch := d.sseClients["c1"]
	d.mu.RUnlock()

	select {
	case <-ch:
		t.Error("expected no message sent when data can't be marshaled")
	default:
		// expected
	}
}
