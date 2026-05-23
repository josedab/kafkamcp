package watch

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestContainsString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		haystack string
		needle   string
		want     bool
	}{
		{"exact_match", "hello", "hello", true},
		{"case_insensitive", "Hello World", "hello", true},
		{"substring", "foobar", "oba", true},
		{"no_match", "abc", "xyz", false},
		{"empty_needle", "anything", "", true},
		{"empty_haystack", "", "something", false},
		{"both_empty", "", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := containsString(tt.haystack, tt.needle)
			if got != tt.want {
				t.Errorf("containsString(%q, %q) = %v, want %v", tt.haystack, tt.needle, got, tt.want)
			}
		})
	}
}

func TestJsonReader(t *testing.T) {
	t.Parallel()
	data := []byte(`{"key":"value"}`)
	r := jsonReader(data)
	if r.Len() != len(data) {
		t.Errorf("reader len = %d, want %d", r.Len(), len(data))
	}
}

func TestSubscribeMultiple(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	for i := 0; i < 5; i++ {
		_, err := m.Subscribe(fmt.Sprintf("agent-%d", i), "topic.*", "", "", "", nil)
		if err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
	}
	if m.ActiveSubscriptionCount() != 5 {
		t.Errorf("expected 5 active subscriptions, got %d", m.ActiveSubscriptionCount())
	}
}

func TestSubscribeIDsAreUnique(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	sub1, _ := m.Subscribe("agent-1", "t1.*", "", "", "", nil)
	sub2, _ := m.Subscribe("agent-1", "t2.*", "", "", "", nil)
	if sub1.ID == sub2.ID {
		t.Errorf("subscription IDs should be unique, both are %q", sub1.ID)
	}
}

func TestUnsubscribeNonExistent(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	err := m.Unsubscribe("watch-does-not-exist", "agent-1")
	if err == nil {
		t.Fatal("expected error for non-existent subscription")
	}
}

func TestListSubscriptionsEmpty(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	subs := m.ListSubscriptions("")
	if len(subs) != 0 {
		t.Errorf("expected 0 subscriptions, got %d", len(subs))
	}
}

func TestListSubscriptionsFilterByAgent(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	_, _ = m.Subscribe("agent-a", "t1.*", "", "", "", nil)
	_, _ = m.Subscribe("agent-b", "t2.*", "", "", "", nil)
	_, _ = m.Subscribe("agent-a", "t3.*", "", "", "", nil)
	_, _ = m.Subscribe("agent-c", "t4.*", "", "", "", nil)

	subs := m.ListSubscriptions("agent-a")
	if len(subs) != 2 {
		t.Errorf("expected 2 subscriptions for agent-a, got %d", len(subs))
	}

	subs = m.ListSubscriptions("agent-c")
	if len(subs) != 1 {
		t.Errorf("expected 1 subscription for agent-c, got %d", len(subs))
	}
}

func TestProcessMessageValueFilter(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	_, _ = m.Subscribe("agent-1", "events.*", "", "important", "", nil)

	ctx := context.Background()

	// Should match - value contains "important"
	results := m.ProcessMessage(ctx, MessageEvent{
		Topic:     "events.created",
		Value:     "this is important data",
		Timestamp: time.Now(),
	})
	if len(results) != 1 {
		t.Errorf("expected 1 match, got %d", len(results))
	}

	// Should not match
	results = m.ProcessMessage(ctx, MessageEvent{
		Topic:     "events.created",
		Value:     "boring data",
		Timestamp: time.Now(),
	})
	if len(results) != 0 {
		t.Errorf("expected 0 matches, got %d", len(results))
	}
}

func TestProcessMessageMultipleSubscriptionsMatch(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	_, _ = m.Subscribe("agent-1", "orders.*", "", "", "", nil)
	_, _ = m.Subscribe("agent-2", "orders.*", "", "", "", nil)

	ctx := context.Background()
	results := m.ProcessMessage(ctx, MessageEvent{
		Topic:     "orders.created",
		Timestamp: time.Now(),
	})
	if len(results) != 2 {
		t.Errorf("expected 2 matches, got %d", len(results))
	}
}

func TestProcessMessageHeaderKeyPresent(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	// Subscribe with header key only (empty value means just check presence)
	_, _ = m.Subscribe("agent-1", "orders.*", "", "", "", map[string]string{"trace-id": ""})

	ctx := context.Background()

	// Match: header key exists
	results := m.ProcessMessage(ctx, MessageEvent{
		Topic:     "orders.created",
		Headers:   map[string]string{"trace-id": "abc-123"},
		Timestamp: time.Now(),
	})
	if len(results) != 1 {
		t.Errorf("expected 1 match for present header, got %d", len(results))
	}

	// No match: header key missing
	results = m.ProcessMessage(ctx, MessageEvent{
		Topic:     "orders.created",
		Headers:   map[string]string{"other": "val"},
		Timestamp: time.Now(),
	})
	if len(results) != 0 {
		t.Errorf("expected 0 matches for missing header, got %d", len(results))
	}
}

func TestNotificationChannel(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	ch := m.NotificationChannel()
	if ch == nil {
		t.Fatal("expected non-nil notification channel")
	}

	_, _ = m.Subscribe("agent-1", "test.*", "", "", "", nil)

	ctx := context.Background()
	m.ProcessMessage(ctx, MessageEvent{
		Topic:     "test.topic",
		Timestamp: time.Now(),
		Key:       "k",
		Value:     "v",
	})

	select {
	case notif := <-ch:
		if notif.Topic != "test.topic" {
			t.Errorf("notification topic = %q, want %q", notif.Topic, "test.topic")
		}
	case <-time.After(time.Second):
		t.Fatal("expected notification on channel")
	}
}

func TestActiveSubscriptionCountAfterUnsubscribe(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	sub1, _ := m.Subscribe("agent-1", "a.*", "", "", "", nil)
	_, _ = m.Subscribe("agent-1", "b.*", "", "", "", nil)

	if m.ActiveSubscriptionCount() != 2 {
		t.Fatalf("expected 2, got %d", m.ActiveSubscriptionCount())
	}

	_ = m.Unsubscribe(sub1.ID, "agent-1")
	if m.ActiveSubscriptionCount() != 1 {
		t.Fatalf("expected 1 after unsubscribe, got %d", m.ActiveSubscriptionCount())
	}
}

func TestWebhookDeliverySuccess(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	m := NewManager(testLogger())
	defer m.Close()

	// Bypass URL validation for test (localhost webhook)
	m.mu.Lock()
	m.counter++
	m.subscriptions["watch-agent-1-1"] = &Subscription{
		ID: "watch-agent-1-1", AgentID: "agent-1",
		TopicPattern: "test.*", WebhookURL: srv.URL,
		MaxRetries: 5, CreatedAt: time.Now(), Active: true,
	}
	m.mu.Unlock()

	ctx := context.Background()
	results := m.ProcessMessage(ctx, MessageEvent{
		Topic:     "test.topic",
		Timestamp: time.Now(),
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != "delivered" {
		t.Errorf("expected delivered, got %s", results[0].Status)
	}
}

func TestWebhookDeliveryFailure(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	m := NewManager(testLogger())
	m.maxRetries = 1 // reduce retries for speed
	defer m.Close()

	// Bypass URL validation for test (localhost webhook)
	m.mu.Lock()
	m.counter++
	m.subscriptions["watch-agent-1-1"] = &Subscription{
		ID: "watch-agent-1-1", AgentID: "agent-1",
		TopicPattern: "test.*", WebhookURL: srv.URL,
		MaxRetries: 1, CreatedAt: time.Now(), Active: true,
	}
	m.mu.Unlock()

	ctx := context.Background()
	results := m.ProcessMessage(ctx, MessageEvent{
		Topic:     "test.topic",
		Timestamp: time.Now(),
	})
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != "failed" {
		t.Errorf("expected failed, got %s", results[0].Status)
	}
	if results[0].LastError == "" {
		t.Error("expected non-empty LastError")
	}
}

func TestCloseRemovesSubscriptions(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	_, _ = m.Subscribe("agent-1", "a.*", "", "", "", nil)
	_, _ = m.Subscribe("agent-2", "b.*", "", "", "", nil)

	m.Close()

	if m.ActiveSubscriptionCount() != 0 {
		t.Errorf("expected 0 after close, got %d", m.ActiveSubscriptionCount())
	}
}

func TestProcessMessageContextCancelled(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	_, _ = m.Subscribe("agent-1", "test.*", "", "", "", nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should still work for channel delivery (non-blocking)
	results := m.ProcessMessage(ctx, MessageEvent{
		Topic:     "test.topic",
		Timestamp: time.Now(),
	})
	if len(results) != 1 {
		t.Errorf("expected 1 result even with canceled ctx, got %d", len(results))
	}
}
