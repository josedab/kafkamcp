package eventbus

import (
	"log/slog"
	"testing"
	"time"
)

func TestBusPublishSubscribe(t *testing.T) {
	bus := NewBus(DefaultBusConfig(), slog.Default())
	sub, err := bus.Subscribe(SubscriptionFilter{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	bus.Publish(Event{Type: EventToolInvocation, Source: "test", AgentID: "agent-1", Tool: "kafka_list_topics"})

	select {
	case evt := <-sub.Events:
		if evt.Type != EventToolInvocation {
			t.Errorf("Type = %q, want %q", evt.Type, EventToolInvocation)
		}
		if evt.Sequence != 1 {
			t.Errorf("Sequence = %d, want 1", evt.Sequence)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestBusFilter(t *testing.T) {
	bus := NewBus(DefaultBusConfig(), slog.Default())

	sub, _ := bus.Subscribe(SubscriptionFilter{
		EventTypes: []EventType{EventKafkaProduce},
	})

	bus.Publish(Event{Type: EventToolInvocation, Source: "test"})
	bus.Publish(Event{Type: EventKafkaProduce, Source: "test"})

	select {
	case evt := <-sub.Events:
		if evt.Type != EventKafkaProduce {
			t.Errorf("got event type %q, want kafka_produce", evt.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for filtered event")
	}

	stats := sub.Stats()
	if stats.Delivered != 1 {
		t.Errorf("Delivered = %d, want 1", stats.Delivered)
	}
}

func TestBusBackpressure(t *testing.T) {
	cfg := DefaultBusConfig()
	cfg.SubscriberBuffer = 2
	bus := NewBus(cfg, slog.Default())

	sub, _ := bus.Subscribe(SubscriptionFilter{})

	// Publish more than buffer can hold without consuming
	for i := 0; i < 5; i++ {
		bus.Publish(Event{Type: EventToolInvocation, Source: "test"})
	}

	stats := sub.Stats()
	if stats.Dropped == 0 {
		t.Error("expected some dropped events due to backpressure")
	}
	if stats.Delivered+stats.Dropped != 5 {
		t.Errorf("delivered(%d) + dropped(%d) != 5", stats.Delivered, stats.Dropped)
	}
}

func TestBusUnsubscribe(t *testing.T) {
	bus := NewBus(DefaultBusConfig(), slog.Default())
	sub, _ := bus.Subscribe(SubscriptionFilter{})

	if err := bus.Unsubscribe(sub.ID); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if err := bus.Unsubscribe(sub.ID); err == nil {
		t.Error("expected error for double unsubscribe")
	}
}

func TestBusReplay(t *testing.T) {
	bus := NewBus(DefaultBusConfig(), slog.Default())

	for i := 0; i < 10; i++ {
		bus.Publish(Event{Type: EventToolInvocation, Source: "test", AgentID: "agent-1"})
	}
	for i := 0; i < 5; i++ {
		bus.Publish(Event{Type: EventKafkaProduce, Source: "test", AgentID: "agent-2"})
	}

	// Replay all
	events := bus.Replay(SubscriptionFilter{}, 100)
	if len(events) != 15 {
		t.Errorf("Replay all = %d, want 15", len(events))
	}

	// Replay filtered
	events = bus.Replay(SubscriptionFilter{EventTypes: []EventType{EventKafkaProduce}}, 100)
	if len(events) != 5 {
		t.Errorf("Replay filtered = %d, want 5", len(events))
	}

	// Replay with limit
	events = bus.Replay(SubscriptionFilter{}, 3)
	if len(events) != 3 {
		t.Errorf("Replay limited = %d, want 3", len(events))
	}
}

func TestBusStats(t *testing.T) {
	bus := NewBus(DefaultBusConfig(), slog.Default())
	_, _ = bus.Subscribe(SubscriptionFilter{})
	bus.Publish(Event{Type: EventToolInvocation, Source: "test"})

	stats := bus.Stats()
	if stats.TotalPublished != 1 {
		t.Errorf("TotalPublished = %d, want 1", stats.TotalPublished)
	}
	if stats.TotalSubscribers != 1 {
		t.Errorf("TotalSubscribers = %d, want 1", stats.TotalSubscribers)
	}
}

func TestBusMaxSubscribers(t *testing.T) {
	cfg := DefaultBusConfig()
	cfg.MaxSubscribers = 2
	bus := NewBus(cfg, slog.Default())

	_, _ = bus.Subscribe(SubscriptionFilter{})
	_, _ = bus.Subscribe(SubscriptionFilter{})
	_, err := bus.Subscribe(SubscriptionFilter{})
	if err == nil {
		t.Error("expected error when exceeding max subscribers")
	}
}

func TestMatchesFilter(t *testing.T) {
	evt := Event{
		Type:    EventToolInvocation,
		AgentID: "agent-1",
		Tool:    "kafka_list_topics",
		Cluster: "prod",
		Source:  "mcp-server",
	}

	tests := []struct {
		name   string
		filter SubscriptionFilter
		want   bool
	}{
		{"empty filter matches all", SubscriptionFilter{}, true},
		{"matching type", SubscriptionFilter{EventTypes: []EventType{EventToolInvocation}}, true},
		{"non-matching type", SubscriptionFilter{EventTypes: []EventType{EventKafkaProduce}}, false},
		{"matching agent", SubscriptionFilter{AgentIDs: []string{"agent-1"}}, true},
		{"non-matching agent", SubscriptionFilter{AgentIDs: []string{"agent-2"}}, false},
		{"matching source pattern", SubscriptionFilter{SourcePattern: "mcp"}, true},
		{"non-matching source pattern", SubscriptionFilter{SourcePattern: "other"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchesFilter(evt, tt.filter); got != tt.want {
				t.Errorf("matchesFilter = %v, want %v", got, tt.want)
			}
		})
	}
}
