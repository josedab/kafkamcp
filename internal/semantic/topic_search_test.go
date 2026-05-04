package semantic

import (
	"testing"
)

func TestTopicIndex_IndexAndSearch(t *testing.T) {
	t.Parallel()
	idx := NewTopicIndex()
	idx.IndexTopic("user-events", "User activity events like login, logout, signup", []string{"users", "auth"}, nil, 1000, nil)
	idx.IndexTopic("order-created", "E-commerce order creation events", []string{"orders", "commerce"}, nil, 500, nil)
	idx.IndexTopic("payment-processed", "Payment processing completion events", []string{"payments", "billing"}, nil, 300, nil)

	results := idx.SearchTopics("user login activity", 10)
	if len(results) == 0 {
		t.Fatal("expected results for 'user login activity'")
	}
	if results[0].Topic != "user-events" {
		t.Errorf("expected user-events as top result, got %s", results[0].Topic)
	}
}

func TestTopicIndex_SearchOrders(t *testing.T) {
	t.Parallel()
	idx := NewTopicIndex()
	idx.IndexTopic("user-events", "User activity events", []string{"users"}, nil, 1000, nil)
	idx.IndexTopic("order-created", "E-commerce order creation events", []string{"orders", "commerce"}, nil, 500, nil)
	idx.IndexTopic("order-shipped", "Order shipping notification events", []string{"orders", "shipping"}, nil, 200, nil)

	results := idx.SearchTopics("orders and commerce", 10)
	if len(results) == 0 {
		t.Fatal("expected results for 'orders and commerce'")
	}
	// Both order topics should appear
	found := 0
	for _, r := range results {
		if r.Topic == "order-created" || r.Topic == "order-shipped" {
			found++
		}
	}
	if found < 1 {
		t.Error("expected at least one order-related topic in results")
	}
}

func TestTopicIndex_EmptyQuery(t *testing.T) {
	t.Parallel()
	idx := NewTopicIndex()
	idx.IndexTopic("test-topic", "Test", nil, nil, 0, nil)

	results := idx.SearchTopics("", 10)
	if len(results) != 0 {
		t.Errorf("expected 0 results for empty query, got %d", len(results))
	}
}

func TestTopicIndex_RemoveTopic(t *testing.T) {
	t.Parallel()
	idx := NewTopicIndex()
	idx.IndexTopic("topic-a", "Description A", nil, nil, 100, nil)
	idx.IndexTopic("topic-b", "Description B", nil, nil, 200, nil)

	removed := idx.RemoveTopic("topic-a")
	if !removed {
		t.Error("expected topic-a to be removed")
	}
	if idx.Count() != 1 {
		t.Errorf("expected 1 topic, got %d", idx.Count())
	}
}

func TestTopicIndex_Count(t *testing.T) {
	t.Parallel()
	idx := NewTopicIndex()
	if idx.Count() != 0 {
		t.Errorf("expected 0 count, got %d", idx.Count())
	}
	idx.IndexTopic("t1", "", nil, nil, 0, nil)
	idx.IndexTopic("t2", "", nil, nil, 0, nil)
	if idx.Count() != 2 {
		t.Errorf("expected 2 count, got %d", idx.Count())
	}
}

func TestTopicIndex_GetProfile(t *testing.T) {
	t.Parallel()
	idx := NewTopicIndex()
	idx.IndexTopic("my-topic", "My topic desc", []string{"tag1"}, []string{"key1"}, 42, map[string]string{"owner": "team-a"})

	p, ok := idx.GetProfile("my-topic")
	if !ok {
		t.Fatal("expected profile to exist")
	}
	if p.Name != "my-topic" {
		t.Errorf("expected my-topic, got %s", p.Name)
	}
	if p.MessageCount != 42 {
		t.Errorf("expected 42 messages, got %d", p.MessageCount)
	}
}

func TestTopicIndex_DirectNameMatch(t *testing.T) {
	t.Parallel()
	idx := NewTopicIndex()
	idx.IndexTopic("kafka-metrics", "Kafka broker metrics", nil, nil, 0, nil)
	idx.IndexTopic("app-logs", "Application log events", nil, nil, 0, nil)

	results := idx.SearchTopics("kafka-metrics", 10)
	if len(results) == 0 {
		t.Fatal("expected results for exact name match")
	}
	if results[0].Topic != "kafka-metrics" {
		t.Errorf("expected kafka-metrics as top result, got %s", results[0].Topic)
	}
}

func TestTopicIndex_LimitResults(t *testing.T) {
	t.Parallel()
	idx := NewTopicIndex()
	for i := 0; i < 20; i++ {
		idx.IndexTopic("event-topic-"+string(rune('a'+i)), "Events for processing", []string{"events"}, nil, 100, nil)
	}

	results := idx.SearchTopics("events processing", 5)
	if len(results) > 5 {
		t.Errorf("expected max 5 results, got %d", len(results))
	}
}

func TestTopicIndex_ListProfiles(t *testing.T) {
	t.Parallel()
	idx := NewTopicIndex()
	idx.IndexTopic("t1", "", nil, nil, 0, nil)
	idx.IndexTopic("t2", "", nil, nil, 0, nil)

	profiles := idx.ListProfiles()
	if len(profiles) != 2 {
		t.Errorf("expected 2 profiles, got %d", len(profiles))
	}
}
