package watch

import (
	"log/slog"
	"os"
	"testing"
	"time"
)

func testWatchLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// --- FilterExpression tests ---

func TestFilterExpression_Compile_Regex(t *testing.T) {
	f := &FilterExpression{Field: "key", Operator: "regex", Value: `^order-\d+$`}
	if err := f.Compile(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.compiled == nil {
		t.Error("expected compiled regex")
	}
}

func TestFilterExpression_Compile_InvalidRegex(t *testing.T) {
	f := &FilterExpression{Field: "key", Operator: "regex", Value: `[invalid`}
	if err := f.Compile(); err == nil {
		t.Error("expected error for invalid regex")
	}
}

func TestFilterExpression_Compile_NonRegex(t *testing.T) {
	f := &FilterExpression{Field: "key", Operator: "contains", Value: "test"}
	if err := f.Compile(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.compiled != nil {
		t.Error("expected nil compiled for non-regex operator")
	}
}

func TestFilterExpression_Match_KeyContains(t *testing.T) {
	f := &FilterExpression{Field: "key", Operator: "contains", Value: "order"}
	event := MessageEvent{Key: "my-order-123"}
	if !f.Match(event) {
		t.Error("expected key contains match")
	}
	event.Key = "payment-456"
	if f.Match(event) {
		t.Error("expected no match for non-matching key")
	}
}

func TestFilterExpression_Match_KeyEquals(t *testing.T) {
	f := &FilterExpression{Field: "key", Operator: "equals", Value: "exact-key"}
	event := MessageEvent{Key: "exact-key"}
	if !f.Match(event) {
		t.Error("expected key equals match")
	}
	event.Key = "different"
	if f.Match(event) {
		t.Error("expected no match for different key")
	}
}

func TestFilterExpression_Match_KeyRegex(t *testing.T) {
	f := &FilterExpression{Field: "key", Operator: "regex", Value: `^order-\d+$`}
	f.Compile()

	event := MessageEvent{Key: "order-123"}
	if !f.Match(event) {
		t.Error("expected regex match")
	}
	event.Key = "payment-123"
	if f.Match(event) {
		t.Error("expected no regex match")
	}
}

func TestFilterExpression_Match_ValueContains(t *testing.T) {
	f := &FilterExpression{Field: "value", Operator: "contains", Value: "error"}
	event := MessageEvent{Value: "timeout error occurred"}
	if !f.Match(event) {
		t.Error("expected value contains match")
	}
}

func TestFilterExpression_Match_HeaderExists(t *testing.T) {
	f := &FilterExpression{Field: "header.x-trace-id", Operator: "exists"}
	event := MessageEvent{Headers: map[string]string{"x-trace-id": "abc"}}
	if !f.Match(event) {
		t.Error("expected header exists match")
	}

	event.Headers = map[string]string{}
	if f.Match(event) {
		t.Error("expected no match when header missing")
	}
}

func TestFilterExpression_Match_HeaderEquals(t *testing.T) {
	f := &FilterExpression{Field: "header.source", Operator: "equals", Value: "svc-a"}
	event := MessageEvent{Headers: map[string]string{"source": "svc-a"}}
	if !f.Match(event) {
		t.Error("expected header equals match")
	}
}

func TestFilterExpression_Match_UnknownField(t *testing.T) {
	f := &FilterExpression{Field: "unknown_field", Operator: "contains", Value: "x"}
	event := MessageEvent{Key: "test"}
	if f.Match(event) {
		t.Error("expected no match for unknown field")
	}
}

func TestFilterExpression_Match_DefaultOperator(t *testing.T) {
	f := &FilterExpression{Field: "key", Operator: "unsupported", Value: "test"}
	event := MessageEvent{Key: "test value"}
	if !f.Match(event) {
		t.Error("expected default operator (contains) to match")
	}
}

func TestFilterExpression_Match_RegexNotCompiled(t *testing.T) {
	f := &FilterExpression{Field: "key", Operator: "regex", Value: "test"}
	// Not compiled - should not match
	event := MessageEvent{Key: "test"}
	if f.Match(event) {
		t.Error("expected no match for uncompiled regex")
	}
}

func TestFilterExpression_Match_ExistsOnNonHeader(t *testing.T) {
	f := &FilterExpression{Field: "key", Operator: "exists"}
	event := MessageEvent{Key: "something"}
	if !f.Match(event) {
		t.Error("expected exists to check non-empty value")
	}
}

// --- ConsumerPool tests ---

func TestNewConsumerPool_DefaultMax(t *testing.T) {
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(mgr, nil, "default", 0)
	defer pool.Stop()

	if pool.maxConsumers != 100 {
		t.Errorf("expected default max 100, got %d", pool.maxConsumers)
	}
}

func TestConsumerPool_ActiveCount(t *testing.T) {
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(mgr, nil, "default", 10)
	defer pool.Stop()

	if pool.ActiveCount() != 0 {
		t.Errorf("expected 0 active consumers, got %d", pool.ActiveCount())
	}
}

func TestConsumerPool_Stop(t *testing.T) {
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(mgr, nil, "default", 10)
	pool.Stop()

	// After stop, pool should be empty
	pool.mu.RLock()
	count := len(pool.consumers)
	pool.mu.RUnlock()
	if count != 0 {
		t.Errorf("expected 0 consumers after stop, got %d", count)
	}
}

func TestConsumerPool_EnsureConsumer_PoolLimitReached(t *testing.T) {
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(mgr, nil, "default", 1)
	defer pool.Stop()

	// Manually set active consumers to max
	pool.activeConsumers.Store(1)

	err := pool.EnsureConsumer("topic2")
	if err == nil {
		t.Error("expected error when pool limit reached")
	}
}

// --- DeliveryTracker tests ---

func TestDeliveryTracker_RecordDelivered(t *testing.T) {
	dt := NewDeliveryTracker()

	dt.Record(DeliveryResult{SubscriptionID: "sub-1", Status: "delivered"})
	dt.Record(DeliveryResult{SubscriptionID: "sub-1", Status: "delivered"})

	stats := dt.Stats()
	if stats["total_delivered"].(int64) != 2 {
		t.Errorf("expected 2 delivered, got %v", stats["total_delivered"])
	}
	if stats["subscriptions"].(int) != 1 {
		t.Errorf("expected 1 subscription, got %v", stats["subscriptions"])
	}
}

func TestDeliveryTracker_RecordFailed(t *testing.T) {
	dt := NewDeliveryTracker()

	dt.Record(DeliveryResult{SubscriptionID: "sub-1", Status: "failed", LastError: "timeout"})

	stats := dt.Stats()
	if stats["total_failed"].(int64) != 1 {
		t.Errorf("expected 1 failed, got %v", stats["total_failed"])
	}

	subStats := dt.GetSubscriptionStats("sub-1")
	if subStats == nil || subStats.LastError != "timeout" {
		t.Error("expected subscription stats with error")
	}
}

func TestDeliveryTracker_RecordRetrying(t *testing.T) {
	dt := NewDeliveryTracker()
	dt.Record(DeliveryResult{SubscriptionID: "sub-1", Status: "retrying"})

	stats := dt.Stats()
	if stats["total_retried"].(int64) != 1 {
		t.Errorf("expected 1 retried, got %v", stats["total_retried"])
	}
}

func TestDeliveryTracker_GetSubscriptionStats_Unknown(t *testing.T) {
	dt := NewDeliveryTracker()
	if dt.GetSubscriptionStats("unknown") != nil {
		t.Error("expected nil for unknown subscription")
	}
}

func TestDeliveryTracker_MultipleSubscriptions(t *testing.T) {
	dt := NewDeliveryTracker()
	dt.Record(DeliveryResult{SubscriptionID: "sub-1", Status: "delivered"})
	dt.Record(DeliveryResult{SubscriptionID: "sub-2", Status: "failed", LastError: "err"})
	dt.Record(DeliveryResult{SubscriptionID: "sub-1", Status: "delivered"})

	stats := dt.Stats()
	if stats["subscriptions"].(int) != 2 {
		t.Errorf("expected 2 subscriptions, got %v", stats["subscriptions"])
	}

	s1 := dt.GetSubscriptionStats("sub-1")
	if s1.Delivered != 2 {
		t.Errorf("expected 2 delivered for sub-1, got %d", s1.Delivered)
	}

	s2 := dt.GetSubscriptionStats("sub-2")
	if s2.Failed != 1 {
		t.Errorf("expected 1 failed for sub-2, got %d", s2.Failed)
	}
}

func TestDeliveryTracker_LastDeliveryTimestamp(t *testing.T) {
	dt := NewDeliveryTracker()
	dt.Record(DeliveryResult{SubscriptionID: "sub-1", Status: "delivered"})

	stats := dt.GetSubscriptionStats("sub-1")
	if stats.LastDelivery.IsZero() {
		t.Error("expected non-zero last delivery timestamp")
	}
	if time.Since(stats.LastDelivery) > 5*time.Second {
		t.Error("expected recent last delivery timestamp")
	}
}
