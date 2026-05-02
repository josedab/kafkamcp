package watch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestSubscription_ExpiredMatchSkipped(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	sub, _ := m.Subscribe("agent-1", "orders.*", "", "", "", nil)

	// Manually set expiry in the past
	m.mu.Lock()
	past := time.Now().Add(-1 * time.Hour)
	m.subscriptions[sub.ID].ExpiresAt = &past
	m.mu.Unlock()

	ctx := context.Background()
	results := m.ProcessMessage(ctx, MessageEvent{
		Topic:     "orders.created",
		Timestamp: time.Now(),
	})
	if len(results) != 0 {
		t.Errorf("expected 0 results for expired subscription, got %d", len(results))
	}
}

func TestSubscription_PausedMatchSkipped(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	sub, _ := m.Subscribe("agent-1", "orders.*", "", "", "", nil)
	m.PauseSubscription(sub.ID, "agent-1")

	ctx := context.Background()
	results := m.ProcessMessage(ctx, MessageEvent{
		Topic:     "orders.created",
		Timestamp: time.Now(),
	})
	if len(results) != 0 {
		t.Errorf("expected 0 results for paused subscription, got %d", len(results))
	}
}

func TestGetSubscription_Found(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	sub, _ := m.Subscribe("agent-1", "test.*", "", "", "", nil)
	got, err := m.GetSubscription(sub.ID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID != sub.ID {
		t.Errorf("ID mismatch: got %s, want %s", got.ID, sub.ID)
	}
}

func TestGetSubscription_NotFound(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	_, err := m.GetSubscription("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent subscription")
	}
}

func TestDeliveryCount_Increments(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	sub, _ := m.Subscribe("agent-1", "test.*", "", "", "", nil)

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		m.ProcessMessage(ctx, MessageEvent{
			Topic:     "test.topic",
			Timestamp: time.Now(),
		})
	}

	got, _ := m.GetSubscription(sub.ID)
	if got.DeliveryCount != 3 {
		t.Errorf("expected delivery count=3, got %d", got.DeliveryCount)
	}
	if got.LastMatchAt == nil {
		t.Error("expected LastMatchAt to be set")
	}
}

func TestConcurrentSubscriptions(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			m.Subscribe("agent-1", "test.*", "", "", "", nil)
		}(i)
	}
	wg.Wait()

	if m.ActiveSubscriptionCount() != 100 {
		t.Errorf("expected 100 subscriptions, got %d", m.ActiveSubscriptionCount())
	}
}

func TestWebhookDelivery_RetryOnFailureThenSuccess(t *testing.T) {
	t.Parallel()
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	m := NewManager(testLogger())
	defer m.Close()

	// Bypass URL validation
	m.mu.Lock()
	m.counter++
	m.subscriptions["watch-retry-1"] = &Subscription{
		ID: "watch-retry-1", AgentID: "agent-1",
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
		t.Errorf("expected delivered after retry, got %s", results[0].Status)
	}
	if results[0].Attempts < 3 {
		t.Errorf("expected at least 3 attempts, got %d", results[0].Attempts)
	}
}

func TestFilterExpression_ContainsMatch(t *testing.T) {
	t.Parallel()
	f := FilterExpression{
		Field:    "value",
		Operator: "contains",
		Value:    "important",
	}

	event := MessageEvent{
		Topic: "test",
		Value: "this is important data",
	}
	if !f.Match(event) {
		t.Error("expected contains match to succeed")
	}

	event2 := MessageEvent{
		Topic: "test",
		Value: "boring data",
	}
	if f.Match(event2) {
		t.Error("expected contains match to fail for non-matching value")
	}
}

func TestFilterExpression_EqualsMatch(t *testing.T) {
	t.Parallel()
	f := FilterExpression{
		Field:    "key",
		Operator: "equals",
		Value:    "user-123",
	}

	event := MessageEvent{Topic: "test", Key: "user-123"}
	if !f.Match(event) {
		t.Error("expected equals match to succeed")
	}

	event2 := MessageEvent{Topic: "test", Key: "user-456"}
	if f.Match(event2) {
		t.Error("expected equals match to fail")
	}
}

func TestFilterExpression_ExistsMatch(t *testing.T) {
	t.Parallel()
	f := FilterExpression{
		Field:    "header.trace-id",
		Operator: "exists",
	}

	event := MessageEvent{
		Topic:   "test",
		Headers: map[string]string{"trace-id": "abc"},
	}
	if !f.Match(event) {
		t.Error("expected exists match for present header")
	}

	event2 := MessageEvent{
		Topic:   "test",
		Headers: map[string]string{},
	}
	if f.Match(event2) {
		t.Error("expected exists match to fail for missing header")
	}
}
