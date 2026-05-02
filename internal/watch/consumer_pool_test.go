package watch

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

func testWatchLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// --- FilterExpression tests ---

func TestFilterExpression_Compile_Regex(t *testing.T) {
	t.Parallel()
	f := &FilterExpression{Field: "key", Operator: "regex", Value: `^order-\d+$`}
	if err := f.Compile(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.compiled == nil {
		t.Error("expected compiled regex")
	}
}

func TestFilterExpression_Compile_InvalidRegex(t *testing.T) {
	t.Parallel()
	f := &FilterExpression{Field: "key", Operator: "regex", Value: `[invalid`}
	if err := f.Compile(); err == nil {
		t.Error("expected error for invalid regex")
	}
}

func TestFilterExpression_Compile_NonRegex(t *testing.T) {
	t.Parallel()
	f := &FilterExpression{Field: "key", Operator: "contains", Value: "test"}
	if err := f.Compile(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.compiled != nil {
		t.Error("expected nil compiled for non-regex operator")
	}
}

func TestFilterExpression_Match_KeyContains(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	f := &FilterExpression{Field: "value", Operator: "contains", Value: "error"}
	event := MessageEvent{Value: "timeout error occurred"}
	if !f.Match(event) {
		t.Error("expected value contains match")
	}
}

func TestFilterExpression_Match_HeaderExists(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	f := &FilterExpression{Field: "header.source", Operator: "equals", Value: "svc-a"}
	event := MessageEvent{Headers: map[string]string{"source": "svc-a"}}
	if !f.Match(event) {
		t.Error("expected header equals match")
	}
}

func TestFilterExpression_Match_UnknownField(t *testing.T) {
	t.Parallel()
	f := &FilterExpression{Field: "unknown_field", Operator: "contains", Value: "x"}
	event := MessageEvent{Key: "test"}
	if f.Match(event) {
		t.Error("expected no match for unknown field")
	}
}

func TestFilterExpression_Match_DefaultOperator(t *testing.T) {
	t.Parallel()
	f := &FilterExpression{Field: "key", Operator: "unsupported", Value: "test"}
	event := MessageEvent{Key: "test value"}
	if !f.Match(event) {
		t.Error("expected default operator (contains) to match")
	}
}

func TestFilterExpression_Match_RegexNotCompiled(t *testing.T) {
	t.Parallel()
	f := &FilterExpression{Field: "key", Operator: "regex", Value: "test"}
	// Not compiled - should not match
	event := MessageEvent{Key: "test"}
	if f.Match(event) {
		t.Error("expected no match for uncompiled regex")
	}
}

func TestFilterExpression_Match_ExistsOnNonHeader(t *testing.T) {
	t.Parallel()
	f := &FilterExpression{Field: "key", Operator: "exists"}
	event := MessageEvent{Key: "something"}
	if !f.Match(event) {
		t.Error("expected exists to check non-empty value")
	}
}

// --- ConsumerPool tests ---

func TestNewConsumerPool_DefaultMax(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 0)
	defer pool.Stop()

	if pool.maxConsumers != 100 {
		t.Errorf("expected default max 100, got %d", pool.maxConsumers)
	}
}

func TestConsumerPool_ActiveCount(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 10)
	defer pool.Stop()

	if pool.ActiveCount() != 0 {
		t.Errorf("expected 0 active consumers, got %d", pool.ActiveCount())
	}
}

func TestConsumerPool_Stop(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 10)
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
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 1)
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	dt := NewDeliveryTracker()
	dt.Record(DeliveryResult{SubscriptionID: "sub-1", Status: "retrying"})

	stats := dt.Stats()
	if stats["total_retried"].(int64) != 1 {
		t.Errorf("expected 1 retried, got %v", stats["total_retried"])
	}
}

func TestDeliveryTracker_GetSubscriptionStats_Unknown(t *testing.T) {
	t.Parallel()
	dt := NewDeliveryTracker()
	if dt.GetSubscriptionStats("unknown") != nil {
		t.Error("expected nil for unknown subscription")
	}
}

func TestDeliveryTracker_MultipleSubscriptions(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

// --- runConsumer and hasSubscriptionsForTopic tests ---

func TestHasSubscriptionsForTopic_Matching(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	// Create a subscription with a matching topic pattern
	mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 10)
	defer pool.Stop()

	if !pool.hasSubscriptionsForTopic("orders.created") {
		t.Error("expected subscription match for orders.created")
	}
}

func TestHasSubscriptionsForTopic_NoMatch(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	mgr.Subscribe("agent-1", "orders.*", "", "", "", nil)

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 10)
	defer pool.Stop()

	if pool.hasSubscriptionsForTopic("payments.completed") {
		t.Error("expected no subscription match for payments.completed")
	}
}

func TestHasSubscriptionsForTopic_NoSubscriptions(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 10)
	defer pool.Stop()

	if pool.hasSubscriptionsForTopic("anything") {
		t.Error("expected false with no subscriptions")
	}
}

func TestHasSubscriptionsForTopic_ExactMatch(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	mgr.Subscribe("agent-1", "orders", "", "", "", nil)

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 10)
	defer pool.Stop()

	if !pool.hasSubscriptionsForTopic("orders") {
		t.Error("expected exact match for orders")
	}
}

func TestRunConsumer_ExitsOnContextCancellation(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	// No subscription so runConsumer will exit quickly when it detects no subscriptions
	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 10)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	pool.activeConsumers.Add(1)
	go func() {
		pool.runConsumer(ctx, "test-topic")
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// OK - exited
	case <-time.After(2 * time.Second):
		t.Fatal("runConsumer did not exit within 1s after context cancellation")
	}
}

func TestRunConsumer_SkipsNonMatchingTopics(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	// Subscribe to "orders" only
	mgr.Subscribe("agent-1", "orders", "", "", "", nil)

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 10)
	defer pool.Stop()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	pool.activeConsumers.Add(1)
	go func() {
		pool.runConsumer(ctx, "payments") // not matching "orders"
		close(done)
	}()

	// runConsumer should exit because "payments" has no subscriptions
	select {
	case <-done:
		// OK
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("runConsumer should exit when topic has no subscriptions")
	}
	cancel()
}

func TestRunConsumer_WithMockKafka(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	mgr.Subscribe("agent-1", "orders", "", "", "", nil)

	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 1, Key: "k1", Value: "v1", Headers: map[string]string{}},
			}, nil
		},
	}

	pool := NewConsumerPool(context.Background(), mgr, mock, "default", 10)
	defer pool.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	pool.activeConsumers.Add(1)
	go func() {
		pool.runConsumer(ctx, "orders")
		close(done)
	}()

	// Let it process at least one tick
	time.Sleep(1500 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// OK
	case <-time.After(3 * time.Second):
		t.Fatal("runConsumer did not exit")
	}
}

func TestRunConsumer_HandlesConsumeError(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	mgr.Subscribe("agent-1", "orders", "", "", "", nil)

	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}

	pool := NewConsumerPool(context.Background(), mgr, mock, "default", 10)
	defer pool.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	pool.activeConsumers.Add(1)
	go func() {
		pool.runConsumer(ctx, "orders")
		close(done)
	}()

	// Should continue running despite errors
	time.Sleep(1500 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// OK
	case <-time.After(3 * time.Second):
		t.Fatal("runConsumer did not exit after cancel")
	}
}

func TestConsumerPool_ConcurrentEnsureConsumer(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 100)
	defer pool.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			topic := fmt.Sprintf("topic-%d", idx%5)
			pool.EnsureConsumer(topic)
		}(i)
	}
	wg.Wait()
}

func TestConsumerPool_EnsureConsumer_Idempotent(t *testing.T) {
	t.Parallel()
	mgr := NewManager(testWatchLogger())
	defer mgr.Close()

	pool := NewConsumerPool(context.Background(), mgr, nil, "default", 10)
	defer pool.Stop()

	// First call creates the consumer
	err := pool.EnsureConsumer("test-topic")
	if err != nil {
		t.Fatalf("first EnsureConsumer: %v", err)
	}

	// Second call is idempotent
	err = pool.EnsureConsumer("test-topic")
	if err != nil {
		t.Fatalf("second EnsureConsumer: %v", err)
	}
}
