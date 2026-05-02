package watch

import (
	"testing"
	"time"
	"log/slog"
	"os"
	"context"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func TestSubscribeUnsubscribe(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	sub, err := m.Subscribe("agent-1", "orders.*", "", "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sub.ID == "" {
		t.Fatal("expected non-empty subscription ID")
	}
	if !sub.Active {
		t.Fatal("expected subscription to be active")
	}
	if m.ActiveSubscriptionCount() != 1 {
		t.Fatalf("expected 1 active subscription, got %d", m.ActiveSubscriptionCount())
	}

	err = m.Unsubscribe(sub.ID, "agent-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if m.ActiveSubscriptionCount() != 0 {
		t.Fatalf("expected 0 active subscriptions, got %d", m.ActiveSubscriptionCount())
	}
}

func TestSubscribeValidation(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	_, err := m.Subscribe("agent-1", "", "", "", "", nil)
	if err == nil {
		t.Fatal("expected error for empty topic pattern")
	}
}

func TestUnsubscribeWrongAgent(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	sub, _ := m.Subscribe("agent-1", "test.*", "", "", "", nil)
	err := m.Unsubscribe(sub.ID, "agent-2")
	if err == nil {
		t.Fatal("expected error for wrong agent")
	}
}

func TestProcessMessageMatching(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	_, err := m.Subscribe("agent-1", "orders.*", "user-123", "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	event := MessageEvent{
		Topic:     "orders.created",
		Partition: 0,
		Offset:    42,
		Timestamp: time.Now(),
		Key:       "user-123",
		Value:     map[string]any{"amount": 100},
	}

	results := m.ProcessMessage(ctx, event)
	if len(results) != 1 {
		t.Fatalf("expected 1 delivery result, got %d", len(results))
	}
	if results[0].Status != "delivered" {
		t.Fatalf("expected delivered status, got %s", results[0].Status)
	}
}

func TestProcessMessageNoMatch(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	_, err := m.Subscribe("agent-1", "payments.*", "", "", "", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx := context.Background()
	event := MessageEvent{
		Topic:     "orders.created",
		Partition: 0,
		Offset:    1,
		Timestamp: time.Now(),
		Key:       "key1",
		Value:     "value1",
	}

	results := m.ProcessMessage(ctx, event)
	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestListSubscriptions(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	m.Subscribe("agent-1", "orders.*", "", "", "", nil)
	m.Subscribe("agent-2", "payments.*", "", "", "", nil)
	m.Subscribe("agent-1", "events.*", "", "", "", nil)

	all := m.ListSubscriptions("")
	if len(all) != 3 {
		t.Fatalf("expected 3 subscriptions, got %d", len(all))
	}

	agent1 := m.ListSubscriptions("agent-1")
	if len(agent1) != 2 {
		t.Fatalf("expected 2 subscriptions for agent-1, got %d", len(agent1))
	}
}

func TestHeaderMatching(t *testing.T) {
	t.Parallel()
	m := NewManager(testLogger())
	defer m.Close()

	m.Subscribe("agent-1", "orders.*", "", "", "", map[string]string{"source": "web"})

	ctx := context.Background()

	// Should match
	event1 := MessageEvent{
		Topic:   "orders.created",
		Headers: map[string]string{"source": "web"},
		Timestamp: time.Now(),
	}
	results := m.ProcessMessage(ctx, event1)
	if len(results) != 1 {
		t.Fatalf("expected 1 result for matching header, got %d", len(results))
	}

	// Should not match
	event2 := MessageEvent{
		Topic:   "orders.created",
		Headers: map[string]string{"source": "mobile"},
		Timestamp: time.Now(),
	}
	results = m.ProcessMessage(ctx, event2)
	if len(results) != 0 {
		t.Fatalf("expected 0 results for non-matching header, got %d", len(results))
	}
}
