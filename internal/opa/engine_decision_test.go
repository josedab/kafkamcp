package opa

import (
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
)

func TestRecordDecision_Appends(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	d := Decision{ID: "d1", PolicyID: "p1", Allowed: true, Reason: "test"}
	pe.RecordDecision(d)

	decisions := pe.RecentDecisions(0)
	if len(decisions) != 1 {
		t.Fatalf("expected 1 decision, got %d", len(decisions))
	}
	if decisions[0].ID != "d1" {
		t.Errorf("expected d1, got %s", decisions[0].ID)
	}
}

func TestRecordDecision_TrimToMaxDecLog(t *testing.T) {
	t.Parallel()
	pe := &PolicyEngine{
		policies:  make(map[string]*Policy),
		bundles:   make(map[string]*PolicyBundle),
		decisions: make([]Decision, 0),
		logger:    slog.Default(),
		maxDecLog: 5,
	}

	for i := 0; i < 10; i++ {
		pe.RecordDecision(Decision{ID: "d" + string(rune('0'+i))})
	}

	decisions := pe.RecentDecisions(0)
	if len(decisions) != 5 {
		t.Fatalf("expected 5 decisions after trimming, got %d", len(decisions))
	}
}

func TestRecentDecisions_Last5From20(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	for i := 0; i < 20; i++ {
		pe.RecordDecision(Decision{ID: "d" + itoa(i)})
	}

	recent := pe.RecentDecisions(5)
	if len(recent) != 5 {
		t.Fatalf("expected 5 decisions, got %d", len(recent))
	}
	// Should be the last 5 (d15 through d19)
	if recent[0].ID != "d15" {
		t.Errorf("expected d15, got %s", recent[0].ID)
	}
	if recent[4].ID != "d19" {
		t.Errorf("expected d19, got %s", recent[4].ID)
	}
}

func TestRecentDecisions_ZeroReturnsAll(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	for i := 0; i < 7; i++ {
		pe.RecordDecision(Decision{ID: "d" + itoa(i)})
	}

	all := pe.RecentDecisions(0)
	if len(all) != 7 {
		t.Fatalf("expected 7 decisions for limit 0, got %d", len(all))
	}
}

func TestRecentDecisions_EmptyLog(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	decisions := pe.RecentDecisions(5)
	if len(decisions) != 0 {
		t.Errorf("expected 0 decisions from empty log, got %d", len(decisions))
	}
}

func TestExportPolicies_ValidJSON(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	exported, err := pe.ExportPolicies()
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(exported), &parsed); err != nil {
		t.Fatalf("exported JSON is invalid: %v", err)
	}

	// Default policies should be present
	if len(parsed) < 3 {
		t.Errorf("expected at least 3 policies in export, got %d", len(parsed))
	}
}

func TestExportPolicies_EmptyEngine(t *testing.T) {
	t.Parallel()
	pe := &PolicyEngine{
		policies:  make(map[string]*Policy),
		bundles:   make(map[string]*PolicyBundle),
		decisions: make([]Decision, 0),
		logger:    slog.Default(),
		maxDecLog: 10000,
	}

	exported, err := pe.ExportPolicies()
	if err != nil {
		t.Fatalf("export failed: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal([]byte(exported), &parsed); err != nil {
		t.Fatalf("exported JSON is invalid: %v", err)
	}

	if len(parsed) != 0 {
		t.Errorf("expected 0 policies from empty engine, got %d", len(parsed))
	}
}

func TestConcurrentRecordAndRecentDecisions(t *testing.T) {
	t.Parallel()
	pe := newTestEngine()

	var wg sync.WaitGroup
	// Write concurrently
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			pe.RecordDecision(Decision{ID: "d" + itoa(idx)})
		}(i)
	}
	// Read concurrently
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pe.RecentDecisions(5)
		}()
	}

	wg.Wait()

	all := pe.RecentDecisions(0)
	if len(all) != 50 {
		t.Errorf("expected 50 decisions after concurrent writes, got %d", len(all))
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		digits = append([]byte{'-'}, digits...)
	}
	return string(digits)
}
