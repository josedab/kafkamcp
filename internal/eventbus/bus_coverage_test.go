package eventbus

import (
	"log/slog"
	"testing"
	"time"
)

// --- ListSubscriptions ---

func TestListSubscriptions_Empty(t *testing.T) {
	t.Parallel()
	bus := NewBus(DefaultBusConfig(), slog.Default())
	subs := bus.ListSubscriptions()
	if len(subs) != 0 {
		t.Errorf("expected 0 subscriptions, got %d", len(subs))
	}
}

func TestListSubscriptions_Multiple(t *testing.T) {
	t.Parallel()
	bus := NewBus(DefaultBusConfig(), slog.Default())

	bus.Subscribe(SubscriptionFilter{EventTypes: []EventType{EventToolInvocation}})
	bus.Subscribe(SubscriptionFilter{AgentIDs: []string{"agent-1"}})
	bus.Subscribe(SubscriptionFilter{})

	subs := bus.ListSubscriptions()
	if len(subs) != 3 {
		t.Errorf("expected 3, got %d", len(subs))
	}
}

func TestListSubscriptions_StatsAccuracy(t *testing.T) {
	t.Parallel()
	bus := NewBus(DefaultBusConfig(), slog.Default())

	sub, _ := bus.Subscribe(SubscriptionFilter{EventTypes: []EventType{EventKafkaProduce}})

	// Publish matching and non-matching events
	bus.Publish(Event{Type: EventKafkaProduce, Source: "test"})
	bus.Publish(Event{Type: EventKafkaProduce, Source: "test"})
	bus.Publish(Event{Type: EventToolInvocation, Source: "test"})

	// Drain to ensure delivery
	for i := 0; i < 2; i++ {
		select {
		case <-sub.Events:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for event")
		}
	}

	subs := bus.ListSubscriptions()
	if len(subs) != 1 {
		t.Fatalf("expected 1, got %d", len(subs))
	}
	if subs[0].Delivered != 2 {
		t.Errorf("expected 2 delivered, got %d", subs[0].Delivered)
	}
	if subs[0].Dropped != 0 {
		t.Errorf("expected 0 dropped, got %d", subs[0].Dropped)
	}
}

func TestListSubscriptions_DroppedCounts(t *testing.T) {
	t.Parallel()
	cfg := DefaultBusConfig()
	cfg.SubscriberBuffer = 1
	bus := NewBus(cfg, slog.Default())

	bus.Subscribe(SubscriptionFilter{})

	// Publish multiple without consuming
	for i := 0; i < 5; i++ {
		bus.Publish(Event{Type: EventToolInvocation, Source: "test"})
	}

	subs := bus.ListSubscriptions()
	total := subs[0].Delivered + subs[0].Dropped
	if total != 5 {
		t.Errorf("delivered+dropped should be 5, got %d", total)
	}
	if subs[0].Dropped == 0 {
		t.Error("expected some drops with buffer=1 and 5 publishes")
	}
}

// --- matchesFilter edge cases ---

func TestMatchesFilter_CombinedFilters(t *testing.T) {
	t.Parallel()
	evt := Event{
		Type:    EventToolInvocation,
		AgentID: "agent-1",
		Tool:    "kafka_list_topics",
		Source:  "mcp-server",
	}

	// All filter fields matching
	filter := SubscriptionFilter{
		EventTypes: []EventType{EventToolInvocation},
		AgentIDs:   []string{"agent-1"},
		Tools:      []string{"kafka_list_topics"},
	}
	if !matchesFilter(evt, filter) {
		t.Error("expected match when all filters match")
	}

	// One field non-matching
	filter.AgentIDs = []string{"agent-2"}
	if matchesFilter(evt, filter) {
		t.Error("expected no match when AgentID doesn't match")
	}
}

func TestMatchesFilter_SourcePatternSpecialChars(t *testing.T) {
	t.Parallel()
	evt := Event{Source: "mcp.server[v2]"}

	// SourcePattern is substring match (not regex)
	if !matchesFilter(evt, SubscriptionFilter{SourcePattern: "server[v2]"}) {
		t.Error("expected match for substring with special chars")
	}
	if matchesFilter(evt, SubscriptionFilter{SourcePattern: "server[v3]"}) {
		t.Error("expected no match for non-matching substring")
	}
}

func TestMatchesFilter_EmptySourcePattern(t *testing.T) {
	t.Parallel()
	evt := Event{Source: "any-source"}

	if !matchesFilter(evt, SubscriptionFilter{SourcePattern: ""}) {
		t.Error("empty SourcePattern should match all")
	}
}

func TestMatchesFilter_ToolsFilter(t *testing.T) {
	t.Parallel()
	evt := Event{Tool: "kafka_produce"}

	if !matchesFilter(evt, SubscriptionFilter{Tools: []string{"kafka_produce"}}) {
		t.Error("expected match for matching tool")
	}
	if matchesFilter(evt, SubscriptionFilter{Tools: []string{"kafka_consume"}}) {
		t.Error("expected no match for non-matching tool")
	}
}

func TestMatchesFilter_ClustersFilter(t *testing.T) {
	t.Parallel()
	evt := Event{Cluster: "prod"}

	if !matchesFilter(evt, SubscriptionFilter{Clusters: []string{"prod"}}) {
		t.Error("expected match for matching cluster")
	}
	if matchesFilter(evt, SubscriptionFilter{Clusters: []string{"staging"}}) {
		t.Error("expected no match for non-matching cluster")
	}
}

func TestMatchesFilter_AllFieldsEmpty(t *testing.T) {
	t.Parallel()
	evt := Event{
		Type:    EventCustom,
		AgentID: "x",
		Tool:    "y",
		Cluster: "z",
		Source:  "w",
	}
	if !matchesFilter(evt, SubscriptionFilter{}) {
		t.Error("empty filter should match any event")
	}
}

// --- NewBus edge cases ---

func TestNewBus_ZeroBufferSize(t *testing.T) {
	t.Parallel()
	cfg := BusConfig{RingBufferSize: 0}
	bus := NewBus(cfg, slog.Default())

	if len(bus.ring) != 8192 {
		t.Errorf("expected default 8192 for zero buffer, got %d", len(bus.ring))
	}
}

func TestNewBus_CustomBufferSize(t *testing.T) {
	t.Parallel()
	cfg := BusConfig{RingBufferSize: 64}
	bus := NewBus(cfg, slog.Default())

	if len(bus.ring) != 64 {
		t.Errorf("expected 64, got %d", len(bus.ring))
	}
}

func TestNewBus_PublishWithSmallRing(t *testing.T) {
	t.Parallel()
	cfg := BusConfig{RingBufferSize: 4, SubscriberBuffer: 256, MaxSubscribers: 10}
	bus := NewBus(cfg, slog.Default())

	// Publish more than ring size to test wrap-around
	for i := 0; i < 10; i++ {
		bus.Publish(Event{Type: EventToolInvocation, Source: "test"})
	}

	stats := bus.Stats()
	if stats.TotalPublished != 10 {
		t.Errorf("expected 10 published, got %d", stats.TotalPublished)
	}
	if stats.BufferUsed != 4 {
		t.Errorf("expected buffer used 4 (ring full), got %d", stats.BufferUsed)
	}
}
