package dlq

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

func testDLQLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// --- PolicyManager tests ---

func TestPolicyManager_AddPolicy_Defaults(t *testing.T) {
	pm := NewPolicyManager(testDLQLogger())

	pm.AddPolicy(RemediationPolicy{TopicPattern: "dlq.*"})

	policies := pm.ListPolicies()
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].MaxRetries != 3 {
		t.Errorf("expected default MaxRetries=3, got %d", policies[0].MaxRetries)
	}
	if policies[0].BackoffBaseMs != 1000 {
		t.Errorf("expected default BackoffBaseMs=1000, got %d", policies[0].BackoffBaseMs)
	}
	if policies[0].BackoffMaxMs != 30000 {
		t.Errorf("expected default BackoffMaxMs=30000, got %d", policies[0].BackoffMaxMs)
	}
}

func TestPolicyManager_AddPolicy_ReplaceExisting(t *testing.T) {
	pm := NewPolicyManager(testDLQLogger())

	pm.AddPolicy(RemediationPolicy{TopicPattern: "dlq.*", Action: "retry"})
	pm.AddPolicy(RemediationPolicy{TopicPattern: "dlq.*", Action: "skip"})

	policies := pm.ListPolicies()
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy after replace, got %d", len(policies))
	}
	if policies[0].Action != "skip" {
		t.Errorf("expected replaced action 'skip', got %q", policies[0].Action)
	}
}

func TestPolicyManager_RemovePolicy(t *testing.T) {
	pm := NewPolicyManager(testDLQLogger())
	pm.AddPolicy(RemediationPolicy{TopicPattern: "dlq.*"})

	if !pm.RemovePolicy("dlq.*") {
		t.Error("expected successful removal")
	}
	if pm.RemovePolicy("dlq.*") {
		t.Error("expected false for already removed")
	}
	if len(pm.ListPolicies()) != 0 {
		t.Error("expected empty policies after removal")
	}
}

func TestPolicyManager_FindPolicy(t *testing.T) {
	pm := NewPolicyManager(testDLQLogger())
	pm.AddPolicy(RemediationPolicy{TopicPattern: "dlq.*", Action: "retry", Priority: 1})
	pm.AddPolicy(RemediationPolicy{TopicPattern: "dlq.orders*", Action: "route", Priority: 10})

	// Exact match with higher priority
	p := pm.FindPolicy("dlq.orders.topic")
	if p == nil || p.Action != "route" {
		t.Errorf("expected route policy for dlq.orders.topic, got %v", p)
	}

	// Only prefix match
	p = pm.FindPolicy("dlq.payments")
	if p == nil || p.Action != "retry" {
		t.Errorf("expected retry policy for dlq.payments, got %v", p)
	}

	// No match
	p = pm.FindPolicy("other-topic")
	if p != nil {
		t.Errorf("expected nil for non-matching topic, got %v", p)
	}
}

func TestPolicyManager_FindPolicy_WildcardAll(t *testing.T) {
	pm := NewPolicyManager(testDLQLogger())
	pm.AddPolicy(RemediationPolicy{TopicPattern: "*", Action: "retry"})

	p := pm.FindPolicy("anything")
	if p == nil || p.Action != "retry" {
		t.Error("expected wildcard match for any topic")
	}
}

// --- matchTopicPattern tests ---

func TestMatchTopicPattern(t *testing.T) {
	tests := []struct {
		pattern string
		topic   string
		want    bool
	}{
		{"*", "anything", true},
		{"dlq.*", "dlq.orders", true},
		{"dlq.*", "other", false},
		{"*orders", "dlq.orders", true},
		{"*orders", "payments", false},
		{"exact-topic", "exact-topic", true},
		{"exact-topic", "other", false},
	}

	for _, tt := range tests {
		got := matchTopicPattern(tt.pattern, tt.topic)
		if got != tt.want {
			t.Errorf("matchTopicPattern(%q, %q) = %v, want %v", tt.pattern, tt.topic, got, tt.want)
		}
	}
}

// --- CalculateBackoff tests ---

func TestCalculateBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		baseMs  int64
		maxMs   int64
		wantMs  int64
	}{
		{0, 1000, 30000, 1000},    // 1000 * 2^0 = 1000
		{1, 1000, 30000, 2000},    // 1000 * 2^1 = 2000
		{2, 1000, 30000, 4000},    // 1000 * 2^2 = 4000
		{5, 1000, 30000, 30000},   // 1000 * 2^5 = 32000 → capped at 30000
		{10, 1000, 30000, 30000},  // capped
	}

	for _, tt := range tests {
		got := CalculateBackoff(tt.attempt, tt.baseMs, tt.maxMs)
		wantDur := time.Duration(tt.wantMs) * time.Millisecond
		if got != wantDur {
			t.Errorf("CalculateBackoff(%d, %d, %d) = %v, want %v", tt.attempt, tt.baseMs, tt.maxMs, got, wantDur)
		}
	}
}

// --- DryRun tests ---

func TestDryRun_ClassifiesErrors(t *testing.T) {
	logger := testDLQLogger()
	r := NewRemediator(logger)
	r.AddPolicy(Policy{TopicPattern: "dlq.*", Action: "retry"})

	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				TotalMessages: 100,
				ErrorTypes: map[string]int{
					"timeout error":        30,
					"schema mismatch":      20,
					"auth denied":          10,
					"validation failed":    15,
					"unknown":              25,
				},
			}, nil
		},
	}

	result, err := r.DryRun(context.Background(), mock, "", "dlq.orders", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if result.TotalMessages != 100 {
		t.Errorf("expected 100 total messages, got %d", result.TotalMessages)
	}
	// timeout=30 retry, schema=20 route, auth=10 skip, validation=15 route, unknown=25 retry (from matched policy)
	if result.WouldRetry != 55 {
		t.Errorf("expected 55 retries (timeout+unknown), got %d", result.WouldRetry)
	}
	if result.WouldRoute != 35 {
		t.Errorf("expected 35 routes (schema+validation), got %d", result.WouldRoute)
	}
	if result.WouldSkip != 10 {
		t.Errorf("expected 10 skips (auth), got %d", result.WouldSkip)
	}
}

func TestDryRun_DefaultLimit(t *testing.T) {
	logger := testDLQLogger()
	r := NewRemediator(logger)

	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			if limit != 100 {
				t.Errorf("expected default limit 100, got %d", limit)
			}
			return &kafka.AnalyzeDLQResult{TotalMessages: 0, ErrorTypes: map[string]int{}}, nil
		},
	}

	r.DryRun(context.Background(), mock, "", "topic", 0)
}

func TestDryRun_EstimatedSuccess(t *testing.T) {
	logger := testDLQLogger()
	r := NewRemediator(logger)

	mock := &kafka.MockKafkaManager{
		AnalyzeDLQFn: func(ctx context.Context, cluster, topic string, limit int) (*kafka.AnalyzeDLQResult, error) {
			return &kafka.AnalyzeDLQResult{
				TotalMessages: 100,
				ErrorTypes:    map[string]int{"timeout error": 100},
			}, nil
		},
	}

	result, _ := r.DryRun(context.Background(), mock, "", "topic", 100)
	if result.EstimatedSuccess < 0.6 || result.EstimatedSuccess > 0.8 {
		t.Errorf("expected ~0.7 success rate for all retries, got %f", result.EstimatedSuccess)
	}
}

// --- Escalate tests ---

func TestEscalate_NoWebhook(t *testing.T) {
	logger := testDLQLogger()
	r := NewRemediator(logger)

	err := r.Escalate(context.Background(), "topic", "error", 10, 5, "")
	if err != nil {
		t.Errorf("expected nil error when no webhook configured, got %v", err)
	}
}

func TestEscalate_WebhookSuccess(t *testing.T) {
	logger := testDLQLogger()
	r := NewRemediator(logger)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	err := r.Escalate(context.Background(), "topic", "timeout", 10, 5, server.URL)
	if err != nil {
		t.Errorf("expected successful escalation, got %v", err)
	}
}

func TestEscalate_WebhookError(t *testing.T) {
	logger := testDLQLogger()
	r := NewRemediator(logger)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	err := r.Escalate(context.Background(), "topic", "timeout", 10, 5, server.URL)
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestEscalate_InvalidURL(t *testing.T) {
	logger := testDLQLogger()
	r := NewRemediator(logger)

	err := r.Escalate(context.Background(), "topic", "error", 10, 5, "http://127.0.0.1:1")
	if err == nil {
		t.Error("expected error for unreachable URL")
	}
}

// --- EscalationEvent struct test ---

func TestEscalationEvent_Fields(t *testing.T) {
	e := EscalationEvent{
		Topic:        "dlq.orders",
		ErrorType:    "timeout",
		FailureCount: 50,
		Threshold:    25,
		Timestamp:    time.Now(),
	}
	if e.Topic != "dlq.orders" || e.FailureCount != 50 {
		t.Errorf("unexpected event: %+v", e)
	}
}
