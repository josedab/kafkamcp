package webhook

import (
	"log/slog"
	"testing"
)

func TestSubscribeAndList(t *testing.T) {
	m := NewManager(slog.Default())

	sub, err := m.Subscribe(Subscription{ID: "s1", URL: "https://example.com/hook", Events: []string{"message"}})
	if err != nil {
		t.Fatal(err)
	}
	if sub.ID != "s1" || !sub.Active {
		t.Errorf("unexpected subscription: %+v", sub)
	}

	subs := m.ListSubscriptions()
	if len(subs) != 1 {
		t.Errorf("expected 1 subscription, got %d", len(subs))
	}
}

func TestSubscribeDuplicate(t *testing.T) {
	m := NewManager(slog.Default())
	_, _ = m.Subscribe(Subscription{ID: "s1", URL: "https://a.com"})
	_, err := m.Subscribe(Subscription{ID: "s1", URL: "https://b.com"})
	if err == nil {
		t.Error("expected error for duplicate subscription")
	}
}

func TestSubscribeInvalidURL(t *testing.T) {
	m := NewManager(slog.Default())
	_, err := m.Subscribe(Subscription{ID: "s1", URL: "ftp://bad.com"})
	if err == nil {
		t.Error("expected error for non-http URL")
	}
}

func TestSubscribeEmptyFields(t *testing.T) {
	m := NewManager(slog.Default())
	_, err := m.Subscribe(Subscription{ID: "", URL: "https://a.com"})
	if err == nil {
		t.Error("expected error for empty ID")
	}
	_, err = m.Subscribe(Subscription{ID: "s1", URL: ""})
	if err == nil {
		t.Error("expected error for empty URL")
	}
}

func TestUnsubscribe(t *testing.T) {
	m := NewManager(slog.Default())
	_, _ = m.Subscribe(Subscription{ID: "s1", URL: "https://a.com"})

	err := m.Unsubscribe("s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.ListSubscriptions()) != 0 {
		t.Error("expected 0 subscriptions after unsubscribe")
	}

	err = m.Unsubscribe("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent subscription")
	}
}

func TestGetSubscription(t *testing.T) {
	m := NewManager(slog.Default())
	_, _ = m.Subscribe(Subscription{ID: "s1", URL: "https://a.com"})

	sub, err := m.GetSubscription("s1")
	if err != nil {
		t.Fatal(err)
	}
	if sub.ID != "s1" {
		t.Errorf("expected s1, got %s", sub.ID)
	}

	_, err = m.GetSubscription("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent subscription")
	}
}

func TestDeliveryHistoryEmpty(t *testing.T) {
	m := NewManager(slog.Default())
	history := m.DeliveryHistory("s1", 10)
	if len(history) != 0 {
		t.Errorf("expected empty history, got %d", len(history))
	}
}

func TestMatchesFilter(t *testing.T) {
	if !matchesFilter(nil, "anything") {
		t.Error("empty filter should match all")
	}
	if !matchesFilter([]string{"message"}, "message") {
		t.Error("exact match should work")
	}
	if matchesFilter([]string{"message"}, "schema_change") {
		t.Error("non-matching should fail")
	}
	if !matchesFilter([]string{"mess*"}, "message") {
		t.Error("wildcard prefix match should work")
	}
	if matchesFilter([]string{"xyz*"}, "message") {
		t.Error("non-matching wildcard should fail")
	}
}
