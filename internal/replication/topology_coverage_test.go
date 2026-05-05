package replication

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- sendWebhook tests ---

func newTestAlertManager(t *testing.T, webhookURL string) *AlertManager {
	t.Helper()
	am, err := NewAlertManager(context.Background(), NotificationConfig{CooldownMins: 1})
	if err != nil {
		t.Fatalf("NewAlertManager: %v", err)
	}
	// Inject the webhook URL and a non-TLS client for testing
	am.config.WebhookURL = webhookURL
	am.httpClient = &http.Client{Timeout: 5 * time.Second}
	return am
}

func TestSendWebhook_Healthy2xx(t *testing.T) {
	t.Parallel()
	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		if r.Header.Get("Content-Type") != "application/json" {
			t.Error("expected Content-Type: application/json")
		}
		if r.Header.Get("User-Agent") != "KafkaMCP-Replication-Monitor/1.0" {
			t.Error("expected correct User-Agent")
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	am := newTestAlertManager(t, srv.URL)
	notif := &AlertNotification{ID: "test-1", Severity: "warning", Message: "test alert"}
	err := am.sendWebhook(notif)
	if err != nil {
		t.Fatalf("sendWebhook: %v", err)
	}
	if received.Load() != 1 {
		t.Errorf("expected 1 request, got %d", received.Load())
	}
}

func TestSendWebhook_Failing5xx(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	am := newTestAlertManager(t, srv.URL)
	notif := &AlertNotification{ID: "test-1", Severity: "critical"}
	err := am.sendWebhook(notif)
	if err == nil {
		t.Fatal("expected error for 5xx response")
	}
}

func TestSendWebhook_UnreachableEndpoint(t *testing.T) {
	t.Parallel()
	am := newTestAlertManager(t, "http://127.0.0.1:1")
	notif := &AlertNotification{ID: "test-1", Severity: "warning"}
	err := am.sendWebhook(notif)
	if err == nil {
		t.Fatal("expected error for unreachable endpoint")
	}
}

func TestSendWebhook_ContextTimeout(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(15 * time.Second) // longer than the 10s context timeout
		w.WriteHeader(200)
	}))
	defer srv.Close()

	am := newTestAlertManager(t, srv.URL)
	am.httpClient.Timeout = 1 * time.Second // short timeout for test

	notif := &AlertNotification{ID: "test-timeout", Severity: "warning"}
	err := am.sendWebhook(notif)
	if err == nil {
		t.Fatal("expected error for timeout")
	}
}

func TestSendWebhook_PayloadMarshal(t *testing.T) {
	t.Parallel()
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 4096)
		n, _ := r.Body.Read(buf)
		receivedBody = buf[:n]
		w.WriteHeader(200)
	}))
	defer srv.Close()

	am := newTestAlertManager(t, srv.URL)
	notif := &AlertNotification{
		ID:       "alert-42",
		Severity: "critical",
		Source:   "cluster-a",
		Target:   "cluster-b",
		Topic:    "orders",
		Message:  "lag exceeded threshold",
	}
	err := am.sendWebhook(notif)
	if err != nil {
		t.Fatalf("sendWebhook: %v", err)
	}
	if len(receivedBody) == 0 {
		t.Error("expected non-empty body")
	}
}

// --- redactURL tests ---

func TestRedactURL_Standard(t *testing.T) {
	t.Parallel()
	got := redactURL("https://hooks.example.com/webhooks/secret123")
	if got != "https://hooks.example.com/***" {
		t.Errorf("redactURL = %q, want https://hooks.example.com/***", got)
	}
}

func TestRedactURL_Empty(t *testing.T) {
	t.Parallel()
	got := redactURL("")
	if got != "" {
		t.Errorf("redactURL empty = %q, want empty", got)
	}
}

func TestRedactURL_NoScheme(t *testing.T) {
	t.Parallel()
	got := redactURL("just-a-string")
	if got != "***" {
		t.Errorf("redactURL no scheme = %q, want ***", got)
	}
}

func TestRedactURL_WithUserInfo(t *testing.T) {
	t.Parallel()
	got := redactURL("https://user:pass@hooks.example.com/webhook")
	if got != "https://user:pass@hooks.example.com/***" {
		t.Errorf("redactURL = %q, want https://user:pass@hooks.example.com/***", got)
	}
}

func TestRedactURL_NoPath(t *testing.T) {
	t.Parallel()
	got := redactURL("https://example.com")
	if got != "https://example.com/***" {
		t.Errorf("redactURL = %q, want https://example.com/***", got)
	}
}

// --- ProcessAlert webhook integration ---

func TestProcessAlert_FirstAlertFiresWebhook(t *testing.T) {
	t.Parallel()
	var webhookCalled atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookCalled.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	am := newTestAlertManager(t, srv.URL)
	alert := Alert{Source: "a", Target: "b", Topic: "t1", Partition: 0, Lag: 5000, Threshold: 1000, Severity: "warning"}
	notif := am.ProcessAlert(alert)
	if notif == nil {
		t.Fatal("expected notification")
	}

	// Wait for async webhook
	time.Sleep(500 * time.Millisecond)
	if webhookCalled.Load() < 1 {
		t.Error("expected webhook to be called")
	}
}

func TestProcessAlert_RepeatedAlertSuppressedByCooldown(t *testing.T) {
	t.Parallel()
	am := newTestAlertManager(t, "")
	am.config.CooldownMins = 60

	alert := Alert{Source: "a", Target: "b", Topic: "t1", Partition: 0, Lag: 5000, Threshold: 1000, Severity: "warning"}

	notif1 := am.ProcessAlert(alert)
	if notif1 == nil {
		t.Fatal("first alert should produce notification")
	}

	notif2 := am.ProcessAlert(alert)
	if notif2 != nil {
		t.Error("repeated alert within cooldown should be suppressed")
	}
}

func TestProcessAlert_CounterIncrement(t *testing.T) {
	t.Parallel()
	am := newTestAlertManager(t, "")

	for i := 0; i < 3; i++ {
		am.mu.Lock()
		am.cooldowns = make(map[string]time.Time)
		am.mu.Unlock()

		notif := am.ProcessAlert(Alert{
			Source: "a", Target: "b", Topic: "t1", Partition: int32(i),
			Lag: 5000, Threshold: 1000, Severity: "warning",
		})
		if notif == nil {
			t.Fatalf("alert %d should produce notification", i)
		}
	}

	am.mu.RLock()
	if am.alertCounter != 3 {
		t.Errorf("alertCounter = %d, want 3", am.alertCounter)
	}
	am.mu.RUnlock()
}

func TestProcessAlert_NotificationRetentionMax1000(t *testing.T) {
	t.Parallel()
	am := newTestAlertManager(t, "")

	for i := 0; i < 1010; i++ {
		am.mu.Lock()
		am.cooldowns = make(map[string]time.Time)
		am.mu.Unlock()
		am.ProcessAlert(Alert{
			Source: "a", Target: "b", Topic: "t", Partition: int32(i % 100),
			Lag: 5000, Threshold: 1000, Severity: "warning",
		})
	}

	am.mu.RLock()
	count := len(am.notifications)
	am.mu.RUnlock()
	if count > 1000 {
		t.Errorf("expected max 1000 notifications, got %d", count)
	}
}

// --- Stop tests ---

func TestStop_CancelsContext(t *testing.T) {
	t.Parallel()
	am := newTestAlertManager(t, "")
	am.Stop()

	if am.ctx.Err() == nil {
		t.Error("expected context to be cancelled after Stop")
	}
}

// --- Concurrent ProcessAlert ---

func TestConcurrentProcessAlert(t *testing.T) {
	t.Parallel()
	var webhookCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webhookCalls.Add(1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	am := newTestAlertManager(t, srv.URL)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			am.mu.Lock()
			am.cooldowns = make(map[string]time.Time)
			am.mu.Unlock()
			am.ProcessAlert(Alert{
				Source: "a", Target: "b", Topic: "t", Partition: int32(idx),
				Lag: 5000, Threshold: 1000, Severity: "warning",
			})
		}(i)
	}
	wg.Wait()
	time.Sleep(1 * time.Second)

	// Verify no panics and some webhooks were sent
	if webhookCalls.Load() == 0 {
		t.Error("expected at least some webhook calls")
	}
}

// --- Semaphore enforcement ---

func TestProcessAlert_SemaphoreEnforcement(t *testing.T) {
	t.Parallel()
	// Create a slow webhook server
	var inflight atomic.Int32
	var maxInflight atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := inflight.Add(1)
		for {
			old := maxInflight.Load()
			if current <= old {
				break
			}
			if maxInflight.CompareAndSwap(old, current) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		inflight.Add(-1)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	am := newTestAlertManager(t, srv.URL)

	// Fire many alerts to try to exceed semaphore
	for i := 0; i < 30; i++ {
		am.mu.Lock()
		am.cooldowns = make(map[string]time.Time)
		am.mu.Unlock()
		am.ProcessAlert(Alert{
			Source: "a", Target: "b", Topic: "t", Partition: int32(i),
			Lag: 5000, Threshold: 1000, Severity: "warning",
		})
	}
	time.Sleep(500 * time.Millisecond)

	if maxInflight.Load() > 10 {
		t.Errorf("max in-flight webhooks = %d, expected <= 10 (semaphore cap)", maxInflight.Load())
	}
}
