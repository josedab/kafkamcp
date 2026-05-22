package metrics

import (
	"fmt"
	"strings"
	"testing"
)

func TestSanitizeTopicLabel_Package(t *testing.T) {
	// Not parallel: this test exercises the package-level defaultMetrics
	// shared state, which `New()` overwrites whenever any other test runs.
	out := SanitizeTopicLabel("orders.events")
	if out == "" {
		t.Error("expected non-empty topic label")
	}
	if !(out == "orders.events" || strings.HasPrefix(out, "topic_overflow_")) {
		t.Errorf("unexpected label %q (expected pass-through or overflow bucket)", out)
	}
	// Calling twice with the same topic should be stable (verbatim if known,
	// or an overflow bucket that is deterministic for the topic name).
	if again := SanitizeTopicLabel("orders.events"); again != out {
		t.Errorf("SanitizeTopicLabel not stable: first=%q second=%q", out, again)
	}
}

func TestMetrics_SanitizeTopicLabel_Overflow(t *testing.T) {
	t.Parallel()
	m := New()
	// Fill up to one below the cap so we have unambiguous pass-through behavior.
	for i := 0; i < maxUniqueTopics-1; i++ {
		out := m.SanitizeTopicLabel(fmt.Sprintf("topic_%d", i))
		if out != fmt.Sprintf("topic_%d", i) {
			t.Fatalf("expected pass-through at iter %d, got %q", i, out)
		}
	}
	// One more known topic to hit exactly the cap.
	if out := m.SanitizeTopicLabel("topic_last"); out != "topic_last" {
		t.Fatalf("expected pass-through for last slot, got %q", out)
	}
	// Now the cap is reached; the next new topic should overflow.
	overflow := m.SanitizeTopicLabel("topic_after_limit")
	if !strings.HasPrefix(overflow, "topic_overflow_") {
		t.Errorf("expected overflow bucket label, got %q", overflow)
	}
}

func TestMetrics_SanitizeTopicLabel_KnownReturnedVerbatim(t *testing.T) {
	t.Parallel()
	m := New()
	m.SanitizeTopicLabel("orders")
	if got := m.SanitizeTopicLabel("orders"); got != "orders" {
		t.Errorf("expected pass-through for known topic, got %q", got)
	}
}
