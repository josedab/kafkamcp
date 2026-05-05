package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDeliver_MatchingSubscription(t *testing.T) {
	t.Parallel()
	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(slog.Default())
	m.Subscribe(Subscription{ID: "s1", URL: srv.URL, Events: []string{"message"}})

	count := m.Deliver(WebhookEvent{ID: "e1", Type: "message", Topic: "orders"})
	if count != 1 {
		t.Errorf("matched = %d, want 1", count)
	}
	time.Sleep(500 * time.Millisecond)
	if received.Load() < 1 {
		t.Error("expected at least 1 delivery")
	}
}

func TestDeliver_NoMatchingSubscription(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	m.Subscribe(Subscription{ID: "s1", URL: "http://example.com", Events: []string{"schema_change"}})

	count := m.Deliver(WebhookEvent{ID: "e1", Type: "message", Topic: "orders"})
	if count != 0 {
		t.Errorf("matched = %d, want 0", count)
	}
}

func TestDeliver_InactiveSubscription(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	m.Subscribe(Subscription{ID: "s1", URL: "http://example.com", Events: []string{"message"}})

	// Deactivate
	m.mu.Lock()
	m.subscriptions["s1"].Active = false
	m.mu.Unlock()

	count := m.Deliver(WebhookEvent{ID: "e1", Type: "message", Topic: "orders"})
	if count != 0 {
		t.Errorf("matched = %d, want 0 for inactive sub", count)
	}
}

func TestMatches_ExactEventType(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	sub := &Subscription{Events: []string{"message"}, Topics: []string{}}
	if !m.matches(sub, WebhookEvent{Type: "message"}) {
		t.Error("exact event type should match")
	}
	if m.matches(sub, WebhookEvent{Type: "schema_change"}) {
		t.Error("different event type should not match")
	}
}

func TestMatches_WildcardTopic(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	sub := &Subscription{Events: []string{"*"}, Topics: []string{"orders*"}}
	if !m.matches(sub, WebhookEvent{Type: "message", Topic: "orders-us"}) {
		t.Error("wildcard topic should match")
	}
	if m.matches(sub, WebhookEvent{Type: "message", Topic: "payments"}) {
		t.Error("non-matching topic should not match")
	}
}

func TestMatches_EmptyFilters(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	sub := &Subscription{Events: []string{}, Topics: []string{}}
	if !m.matches(sub, WebhookEvent{Type: "any", Topic: "any"}) {
		t.Error("empty filters should match everything")
	}
}

func TestMatches_BothFilters(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	sub := &Subscription{Events: []string{"message"}, Topics: []string{"orders"}}
	if !m.matches(sub, WebhookEvent{Type: "message", Topic: "orders"}) {
		t.Error("both matching should succeed")
	}
	if m.matches(sub, WebhookEvent{Type: "message", Topic: "payments"}) {
		t.Error("matching event but non-matching topic should fail")
	}
	if m.matches(sub, WebhookEvent{Type: "schema", Topic: "orders"}) {
		t.Error("non-matching event but matching topic should fail")
	}
}

func TestDeliverWithRetry_SuccessOnFirstTry(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(slog.Default())
	sub := &Subscription{ID: "s1", URL: srv.URL}
	m.deliverWithRetry(sub, WebhookEvent{ID: "e1", Type: "test"})

	if callCount.Load() != 1 {
		t.Errorf("expected 1 call, got %d", callCount.Load())
	}

	history := m.DeliveryHistory("s1", 10)
	if len(history) != 1 {
		t.Fatalf("expected 1 history entry, got %d", len(history))
	}
	if !history[0].Success {
		t.Error("expected successful delivery")
	}
}

func TestDeliverWithRetry_SuccessOnRetry2(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := callCount.Add(1)
		if n < 2 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(slog.Default())
	sub := &Subscription{ID: "s1", URL: srv.URL}
	m.deliverWithRetry(sub, WebhookEvent{ID: "e1", Type: "test"})

	if callCount.Load() != 2 {
		t.Errorf("expected 2 calls, got %d", callCount.Load())
	}
	history := m.DeliveryHistory("s1", 10)
	if len(history) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(history))
	}
	if history[0].Success {
		t.Error("first attempt should have failed")
	}
	if !history[1].Success {
		t.Error("second attempt should have succeeded")
	}
}

func TestDeliverWithRetry_AllRetriesExhausted(t *testing.T) {
	t.Parallel()
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		w.WriteHeader(500)
	}))
	defer srv.Close()

	m := NewManager(slog.Default())
	sub := &Subscription{ID: "s1", URL: srv.URL}
	m.deliverWithRetry(sub, WebhookEvent{ID: "e1", Type: "test"})

	if callCount.Load() != 3 { // maxRetries = 3
		t.Errorf("expected 3 calls, got %d", callCount.Load())
	}
	history := m.DeliveryHistory("s1", 10)
	for _, h := range history {
		if h.Success {
			t.Error("all attempts should have failed")
		}
	}
}

func TestDoPost_HMACSignature(t *testing.T) {
	t.Parallel()
	secret := "my-secret"
	var receivedSig string
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig = r.Header.Get("X-Webhook-Signature")
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(slog.Default())
	sub := &Subscription{ID: "s1", URL: srv.URL, Secret: secret}
	m.deliverWithRetry(sub, WebhookEvent{ID: "e1", Type: "test"})

	if receivedSig == "" {
		t.Fatal("expected signature header")
	}

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(receivedBody)
	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if receivedSig != expected {
		t.Errorf("signature = %q, want %q", receivedSig, expected)
	}
}

func TestDoPost_EmptySecret(t *testing.T) {
	t.Parallel()
	var receivedSig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedSig = r.Header.Get("X-Webhook-Signature")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(slog.Default())
	sub := &Subscription{ID: "s1", URL: srv.URL, Secret: ""}
	m.deliverWithRetry(sub, WebhookEvent{ID: "e1", Type: "test"})

	if receivedSig != "" {
		t.Errorf("expected no signature with empty secret, got %q", receivedSig)
	}
}

func TestDoPost_EventTypeHeader(t *testing.T) {
	t.Parallel()
	var receivedEventType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedEventType = r.Header.Get("X-Webhook-Event")
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(slog.Default())
	sub := &Subscription{ID: "s1", URL: srv.URL}
	m.deliverWithRetry(sub, WebhookEvent{ID: "e1", Type: "message.created"})

	if receivedEventType != "message.created" {
		t.Errorf("event type header = %q, want message.created", receivedEventType)
	}
}

func TestRecordAttempt_UnderLimit(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	for i := 0; i < 5; i++ {
		m.recordAttempt("s1", "e1", "http://x", 200, true, "", 10)
	}

	history := m.DeliveryHistory("s1", 0)
	if len(history) != 5 {
		t.Errorf("expected 5 entries, got %d", len(history))
	}
}

func TestRecordAttempt_AtLimit(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	for i := 0; i < 105; i++ {
		m.recordAttempt("s1", "e1", "http://x", 200, true, "", int64(i))
	}

	m.mu.RLock()
	count := len(m.history["s1"])
	m.mu.RUnlock()
	if count > 100 {
		t.Errorf("expected max 100 entries, got %d", count)
	}

	history := m.DeliveryHistory("s1", 0)
	// Should have entries starting from the 6th one (oldest pruned)
	if history[0].LatencyMs != 5 {
		t.Errorf("oldest entry latency = %d, want 5 (first pruned)", history[0].LatencyMs)
	}
}

func TestRecordAttempt_Ordering(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	for i := 0; i < 3; i++ {
		m.recordAttempt("s1", "e1", "http://x", 200, true, "", int64(i*10))
	}

	history := m.DeliveryHistory("s1", 0)
	if len(history) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(history))
	}
	if history[0].LatencyMs != 0 || history[1].LatencyMs != 10 || history[2].LatencyMs != 20 {
		t.Errorf("ordering wrong: %d, %d, %d", history[0].LatencyMs, history[1].LatencyMs, history[2].LatencyMs)
	}
}

func TestConcurrentDeliver(t *testing.T) {
	t.Parallel()
	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	m := NewManager(slog.Default())
	m.Subscribe(Subscription{ID: "s1", URL: srv.URL, Events: []string{"*"}})
	m.Subscribe(Subscription{ID: "s2", URL: srv.URL, Events: []string{"*"}})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m.Deliver(WebhookEvent{ID: "e", Type: "message", Topic: "t"})
		}(i)
	}
	wg.Wait()
	time.Sleep(2 * time.Second)

	if received.Load() < 10 {
		t.Errorf("expected at least 10 deliveries, got %d", received.Load())
	}
}

func TestDeliverWithRetry_UnreachableEndpoint(t *testing.T) {
	t.Parallel()
	m := NewManager(slog.Default())
	// Use a port that's unlikely to be listening
	sub := &Subscription{ID: "s1", URL: "http://127.0.0.1:1"}
	m.deliverWithRetry(sub, WebhookEvent{ID: "e1", Type: "test"})

	history := m.DeliveryHistory("s1", 10)
	if len(history) == 0 {
		t.Fatal("expected history entries for failed delivery")
	}
	for _, h := range history {
		if h.Success {
			t.Error("delivery to unreachable endpoint should fail")
		}
	}
}

func TestDeliverWithRetry_500Response(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	m := NewManager(slog.Default())
	sub := &Subscription{ID: "s1", URL: srv.URL}
	m.deliverWithRetry(sub, WebhookEvent{ID: "e1", Type: "test"})

	history := m.DeliveryHistory("s1", 10)
	for _, h := range history {
		if h.Success {
			t.Error("500 response should not be successful")
		}
		if h.StatusCode != 500 {
			t.Errorf("StatusCode = %d, want 500", h.StatusCode)
		}
	}
}

func TestDeliverWithRetry_429Response(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
	}))
	defer srv.Close()

	m := NewManager(slog.Default())
	sub := &Subscription{ID: "s1", URL: srv.URL}
	m.deliverWithRetry(sub, WebhookEvent{ID: "e1", Type: "test"})

	history := m.DeliveryHistory("s1", 10)
	for _, h := range history {
		if h.Success {
			t.Error("429 response should not be successful")
		}
	}
}
