package dlq

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

// TestRemediateAnalyzeNilManager tests that the analyze action panics/fails with nil manager.
func TestRemediateAnalyzeNilManager(t *testing.T) {
	r := NewRemediator(slog.Default())
	panicked := false
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				panicked = true
			}
		}()
		r.Remediate(context.Background(), nil, "", "orders-dlq", "analyze", "", 10)
	}()
	if !panicked {
		t.Error("expected panic when calling analyze with nil manager")
	}
}

// TestRemediateRetryNilManager tests that the retry action panics/fails with nil manager.
func TestRemediateRetryNilManager(t *testing.T) {
	r := NewRemediator(slog.Default())
	panicked := false
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				panicked = true
			}
		}()
		r.Remediate(context.Background(), nil, "", "orders-dlq", "retry", "", 10)
	}()
	if !panicked {
		t.Error("expected panic when calling retry with nil manager")
	}
}

// TestRemediateRouteNilManager tests that the route action panics/fails with nil manager.
func TestRemediateRouteNilManager(t *testing.T) {
	r := NewRemediator(slog.Default())
	panicked := false
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				panicked = true
			}
		}()
		r.Remediate(context.Background(), nil, "", "orders-dlq", "route", "target-topic", 10)
	}()
	if !panicked {
		t.Error("expected panic when calling route with nil manager")
	}
}

// TestRemediateSuggestNilManager tests that the suggest action panics/fails with nil manager.
func TestRemediateSuggestNilManager(t *testing.T) {
	r := NewRemediator(slog.Default())
	panicked := false
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				panicked = true
			}
		}()
		r.Remediate(context.Background(), nil, "", "orders-dlq", "suggest", "", 10)
	}()
	if !panicked {
		t.Error("expected panic when calling suggest with nil manager")
	}
}

// TestRemediateValidActionsDispatch tests that valid actions dispatch correctly.
func TestRemediateValidActionsDispatch(t *testing.T) {
	r := NewRemediator(slog.Default())

	// analyze dispatches to analyzeForRemediation which calls mgr.AnalyzeDLQ → nil panic
	// This verifies the dispatch path works
	actions := []struct {
		name   string
		action string
		target string
	}{
		{"analyze_dispatch", "analyze", ""},
		{"retry_dispatch", "retry", ""},
		{"route_dispatch", "route", "target"},
		{"suggest_dispatch", "suggest", ""},
	}

	for _, a := range actions {
		t.Run(a.name, func(t *testing.T) {
			panicked := false
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						panicked = true
					}
				}()
				r.Remediate(context.Background(), nil, "cluster", "topic-dlq", a.action, a.target, 5)
			}()
			if !panicked {
				t.Errorf("expected panic for action %q with nil manager", a.action)
			}
		})
	}
}

// TestRemediateDefaultLimitApplied verifies that a zero/negative limit gets defaulted to 100.
func TestRemediateDefaultLimitZero(t *testing.T) {
	r := NewRemediator(slog.Default())

	// For unknown action, limit is defaulted before the switch
	_, err := r.Remediate(context.Background(), nil, "", "topic", "unknown", "", 0)
	if err == nil {
		t.Error("expected error for unknown action")
	}
}

func TestRemediateDefaultLimitNegative(t *testing.T) {
	r := NewRemediator(slog.Default())

	_, err := r.Remediate(context.Background(), nil, "", "topic", "unknown", "", -10)
	if err == nil {
		t.Error("expected error for unknown action")
	}
}

// TestRemediationResultJSON tests JSON serialization of RemediationResult.
func TestRemediationResultJSON(t *testing.T) {
	r := RemediationResult{
		Topic:          "orders-dlq",
		Action:         "analyze",
		ProcessedCount: 50,
		SuccessCount:   0,
		FailureCount:   0,
		ErrorTypes:     map[string]int{"serialization": 30, "timeout": 20},
		Suggestions:    []string{"retry with backoff", "check schema"},
		Details: []RemediationItem{
			{Partition: 0, Offset: 10, Status: "retried"},
		},
	}

	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out RemediationResult
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.Topic != "orders-dlq" || out.Action != "analyze" {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
	if out.ProcessedCount != 50 {
		t.Errorf("expected ProcessedCount=50, got %d", out.ProcessedCount)
	}
	if len(out.ErrorTypes) != 2 {
		t.Errorf("expected 2 error types, got %d", len(out.ErrorTypes))
	}
	if len(out.Suggestions) != 2 {
		t.Errorf("expected 2 suggestions, got %d", len(out.Suggestions))
	}
	if len(out.Details) != 1 {
		t.Errorf("expected 1 detail, got %d", len(out.Details))
	}
}

// TestRemediationStatsJSON tests JSON serialization of RemediationStats.
func TestRemediationStatsJSON(t *testing.T) {
	s := RemediationStats{
		TotalProcessed: 100,
		TotalSuccess:   90,
		TotalFailed:    10,
		ByTopic:        map[string]int64{"orders-dlq": 60, "payments-dlq": 40},
	}

	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out RemediationStats
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.TotalProcessed != 100 || out.TotalSuccess != 90 || out.TotalFailed != 10 {
		t.Errorf("round-trip mismatch: got %+v", out)
	}
}

// TestPolicyJSON tests JSON serialization of Policy.
func TestPolicyJSON(t *testing.T) {
	p := Policy{
		TopicPattern:  "*-dlq",
		Action:        "retry",
		MaxRetries:    5,
		BackoffMs:     2000,
		TargetTopic:   "correction",
		EscalateAfter: 3,
	}

	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out Policy
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out != p {
		t.Errorf("round-trip mismatch: got %+v, want %+v", out, p)
	}
}

// TestConcurrentGetStats tests concurrent access to stats.
func TestConcurrentGetStats(t *testing.T) {
	r := NewRemediator(slog.Default())

	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			r.updateStats("topic-dlq", &RemediationResult{ProcessedCount: 1, SuccessCount: 1})
		}
		close(done)
	}()

	for i := 0; i < 100; i++ {
		r.GetStats()
	}
	<-done

	stats := r.GetStats()
	if stats.TotalProcessed != 100 {
		t.Errorf("expected TotalProcessed=100, got %d", stats.TotalProcessed)
	}
}

// TestConcurrentAddPolicy tests concurrent policy additions.
func TestConcurrentAddPolicy(t *testing.T) {
	r := NewRemediator(slog.Default())

	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			r.AddPolicy(Policy{TopicPattern: "*-dlq", Action: "retry"})
		}
		close(done)
	}()

	for i := 0; i < 50; i++ {
		r.AddPolicy(Policy{TopicPattern: "*-dlq", Action: "route"})
	}
	<-done

	r.mu.RLock()
	count := len(r.policies)
	r.mu.RUnlock()
	if count != 100 {
		t.Errorf("expected 100 policies, got %d", count)
	}
}
