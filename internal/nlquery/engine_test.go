package nlquery

import (
	"testing"
)

func TestParseFilterRecent(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	plan, err := e.Parse("show me all failed orders in the last 2 hours", []string{"orders", "payments"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.TimeRange == nil {
		t.Error("expected time range")
	}
	if len(plan.Topics) == 0 {
		t.Error("expected topic resolution for 'orders'")
	}
}

func TestParseCount(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	plan, err := e.Parse("count errors in error-logs in the last 1 hour", []string{"error-logs", "access-logs"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plan.Intent != "count" {
		t.Errorf("expected count intent, got %s", plan.Intent)
	}
	if plan.Aggregation == nil || plan.Aggregation.Function != "count" {
		t.Error("expected count aggregation")
	}
}

func TestParseFilterWhere(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	plan, err := e.Parse("show me orders where status is failed", []string{"orders"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(plan.Filters) != 1 {
		t.Fatalf("expected 1 filter, got %d", len(plan.Filters))
	}
	if plan.Filters[0].Field != "status" || plan.Filters[0].Value != "failed" {
		t.Errorf("unexpected filter: %+v", plan.Filters[0])
	}
}

func TestFuzzyFieldMatch(t *testing.T) {
	t.Parallel()
	fields := map[string]string{"order_status": "string", "user_email": "string", "amount": "number"}

	if got := fuzzyFieldMatch("status", fields); got != "order_status" {
		t.Errorf("expected order_status, got %s", got)
	}
	if got := fuzzyFieldMatch("amount", fields); got != "amount" {
		t.Errorf("expected amount, got %s", got)
	}
}

func TestResolveTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	topics := []string{"user-events", "order-events", "payment-transactions"}

	matched := e.resolveTopics("show me user events", topics)
	if len(matched) == 0 {
		t.Error("expected to match user-events")
	}
}

func TestEmptyQuery(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	_, err := e.Parse("", nil)
	if err == nil {
		t.Error("expected error for empty query")
	}
}
