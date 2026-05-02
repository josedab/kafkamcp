package lagdiag

import (
	"fmt"
	"testing"
)

func TestNewAnalyzer(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer()
	if a == nil {
		t.Fatal("NewAnalyzer returned nil")
	}
	if a.history == nil {
		t.Fatal("history map not initialized")
	}
}

func TestRecordSnapshot(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer()
	snap := LagSnapshot{GroupID: "g1", Topic: "t1", Partition: 0, Lag: 100, Timestamp: "t0"}
	a.RecordSnapshot(snap)

	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.history["g1"]) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(a.history["g1"]))
	}
	if a.history["g1"][0].Lag != 100 {
		t.Fatalf("expected lag 100, got %d", a.history["g1"][0].Lag)
	}
}

func TestRecordSnapshotRingBuffer(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer()
	for i := 0; i < 150; i++ {
		a.RecordSnapshot(LagSnapshot{
			GroupID:   "g1",
			Topic:     "t1",
			Partition: 0,
			Lag:       int64(i),
			Timestamp: fmt.Sprintf("t%d", i),
		})
	}

	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.history["g1"]) != maxHistorySize {
		t.Fatalf("expected %d snapshots, got %d", maxHistorySize, len(a.history["g1"]))
	}
	// Oldest entry should be index 50 (150-100).
	if a.history["g1"][0].Lag != 50 {
		t.Fatalf("expected oldest lag 50, got %d", a.history["g1"][0].Lag)
	}
}

func TestDiagnoseGrowingLag(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer()
	// Record snapshots with growing lag.
	for i := 0; i < 10; i++ {
		a.RecordSnapshot(LagSnapshot{
			GroupID:   "g1",
			Topic:     "t1",
			Partition: 0,
			Lag:       int64((i + 1) * 100),
			Timestamp: fmt.Sprintf("t%d", i),
		})
	}

	partitionLags := []LagSnapshot{
		{GroupID: "g1", Topic: "t1", Partition: 0, Lag: 500},
		{GroupID: "g1", Topic: "t1", Partition: 1, Lag: 500},
	}

	diag := a.Diagnose("g1", 1000, partitionLags)
	if diag == nil {
		t.Fatal("Diagnose returned nil")
	}
	if diag.GroupID != "g1" {
		t.Fatalf("expected group g1, got %s", diag.GroupID)
	}
	if diag.TotalLag != 1000 {
		t.Fatalf("expected total lag 1000, got %d", diag.TotalLag)
	}
	if diag.RootCause != "slow_consumer" {
		t.Fatalf("expected root cause slow_consumer, got %s", diag.RootCause)
	}
	if diag.Confidence <= 0 {
		t.Fatal("expected positive confidence")
	}
	if len(diag.Recommendations) == 0 {
		t.Fatal("expected recommendations")
	}
	if diag.Topic != "t1" {
		t.Fatalf("expected topic t1, got %s", diag.Topic)
	}
}

func TestDiagnosePartitionImbalance(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer()
	partitionLags := []LagSnapshot{
		{GroupID: "g1", Topic: "t1", Partition: 0, Lag: 10},
		{GroupID: "g1", Topic: "t1", Partition: 1, Lag: 10},
		{GroupID: "g1", Topic: "t1", Partition: 2, Lag: 10000},
	}

	diag := a.Diagnose("g1", 10020, partitionLags)
	if diag == nil {
		t.Fatal("Diagnose returned nil")
	}
	if diag.RootCause != "partition_imbalance" {
		t.Fatalf("expected root cause partition_imbalance, got %s", diag.RootCause)
	}
	if len(diag.Recommendations) == 0 {
		t.Fatal("expected recommendations")
	}
}

func TestDiagnoseNoHistory(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer()
	partitionLags := []LagSnapshot{
		{GroupID: "g1", Topic: "t1", Partition: 0, Lag: 50},
		{GroupID: "g1", Topic: "t1", Partition: 1, Lag: 50},
	}

	diag := a.Diagnose("g1", 100, partitionLags)
	if diag == nil {
		t.Fatal("Diagnose returned nil")
	}
	if diag.RootCause != "unknown" {
		t.Fatalf("expected unknown root cause, got %s", diag.RootCause)
	}
}

func TestDiagnoseEmptyPartitions(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer()
	diag := a.Diagnose("g1", 0, nil)
	if diag == nil {
		t.Fatal("Diagnose returned nil")
	}
	if diag.Topic != "" {
		t.Fatalf("expected empty topic, got %s", diag.Topic)
	}
}

func TestDiagnoseLagTrendShrinking(t *testing.T) {
	t.Parallel()
	// Shrinking lag should produce a low-score factor.
	var snapshots []LagSnapshot
	for i := 0; i < 10; i++ {
		snapshots = append(snapshots, LagSnapshot{
			GroupID:   "g1",
			Topic:     "t1",
			Partition: 0,
			Lag:       int64(1000 - i*100),
			Timestamp: fmt.Sprintf("t%d", i),
		})
	}

	factor := diagnoseLagTrend(snapshots)
	if factor.Score > 0 && factor.Name != "shrinking_lag" {
		t.Fatalf("expected shrinking_lag or zero factor, got %s", factor.Name)
	}
}

func TestDiagnoseLagTrendInsufficient(t *testing.T) {
	t.Parallel()
	factor := diagnoseLagTrend(nil)
	if factor.Score != 0 {
		t.Fatal("expected zero score for nil snapshots")
	}

	factor = diagnoseLagTrend([]LagSnapshot{{Lag: 100, Timestamp: "t0"}})
	if factor.Score != 0 {
		t.Fatal("expected zero score for single snapshot")
	}
}

func TestDiagnosePartitionImbalanceSinglePartition(t *testing.T) {
	t.Parallel()
	factor := diagnosePartitionImbalance([]LagSnapshot{
		{Partition: 0, Lag: 100},
	})
	if factor.Score != 0 {
		t.Fatal("expected zero score for single partition")
	}
}

func TestDiagnosePartitionImbalanceZeroLag(t *testing.T) {
	t.Parallel()
	factor := diagnosePartitionImbalance([]LagSnapshot{
		{Partition: 0, Lag: 0},
		{Partition: 1, Lag: 0},
	})
	if factor.Score != 0 {
		t.Fatal("expected zero score for zero total lag")
	}
}

func TestDetermineRootCauseEmpty(t *testing.T) {
	t.Parallel()
	cause, conf := determineRootCause(nil)
	if cause != "unknown" || conf != 0.0 {
		t.Fatalf("expected unknown/0.0, got %s/%.2f", cause, conf)
	}
}

func TestDetermineRootCausePicksHighest(t *testing.T) {
	t.Parallel()
	factors := []DiagFactor{
		{Category: "slow_consumer", Score: 0.3},
		{Category: "partition_imbalance", Score: 0.8},
		{Category: "network", Score: 0.5},
	}
	cause, conf := determineRootCause(factors)
	if cause != "partition_imbalance" {
		t.Fatalf("expected partition_imbalance, got %s", cause)
	}
	if conf != 0.8 {
		t.Fatalf("expected 0.8 confidence, got %.2f", conf)
	}
}

func TestGenerateRecommendations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		rootCause string
		wantMin   int
	}{
		{"slow_consumer", 2},
		{"producer_spike", 2},
		{"partition_imbalance", 2},
		{"network", 2},
		{"gc_pressure", 2},
		{"unknown", 1},
	}
	for _, tc := range cases {
		recs := generateRecommendations(tc.rootCause, nil)
		if len(recs) < tc.wantMin {
			t.Errorf("rootCause=%s: expected at least %d recommendations, got %d", tc.rootCause, tc.wantMin, len(recs))
		}
	}
}

func TestGenerateRecommendationsUrgent(t *testing.T) {
	t.Parallel()
	factors := []DiagFactor{
		{Name: "growing_lag", Category: "slow_consumer", Score: 0.9},
	}
	recs := generateRecommendations("slow_consumer", factors)
	found := false
	for _, r := range recs {
		if r == "Lag is growing rapidly; investigate consumer bottlenecks urgently" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected urgent recommendation for high-score growing_lag")
	}
}
