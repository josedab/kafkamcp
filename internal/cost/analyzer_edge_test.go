package cost

import (
	"testing"
	"time"
)

func TestSumIdleCost_Empty(t *testing.T) {
	t.Parallel()
	result := sumIdleCost(nil)
	if result != 0 {
		t.Errorf("expected 0 for empty slice, got %f", result)
	}
}

func TestSumIdleCost_Multiple(t *testing.T) {
	t.Parallel()
	topics := []IdleTopic{
		{Name: "t1", MonthlyCost: 10.50},
		{Name: "t2", MonthlyCost: 20.25},
		{Name: "t3", MonthlyCost: 5.00},
	}
	result := sumIdleCost(topics)
	expected := 35.75
	if result != expected {
		t.Errorf("expected %f, got %f", expected, result)
	}
}

func TestSumIdleCost_ZeroCost(t *testing.T) {
	t.Parallel()
	topics := []IdleTopic{
		{Name: "t1", MonthlyCost: 0},
		{Name: "t2", MonthlyCost: 0},
	}
	result := sumIdleCost(topics)
	if result != 0 {
		t.Errorf("expected 0, got %f", result)
	}
}

func TestSumIdleCost_NegativeCost(t *testing.T) {
	t.Parallel()
	topics := []IdleTopic{
		{Name: "t1", MonthlyCost: -5.0},
		{Name: "t2", MonthlyCost: 10.0},
	}
	result := sumIdleCost(topics)
	if result != 5.0 {
		t.Errorf("expected 5.0, got %f", result)
	}
}

func TestSumIdleCost_Single(t *testing.T) {
	t.Parallel()
	topics := []IdleTopic{
		{Name: "t1", MonthlyCost: 42.5},
	}
	result := sumIdleCost(topics)
	if result != 42.5 {
		t.Errorf("expected 42.5, got %f", result)
	}
}

func TestDetectWaste_NoConsumerGroups(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer(nil)
	topics := []TopicCostInput{
		{Name: "orphan", Partitions: 3, ReplicationFactor: 3, RetentionMs: 604800000, AvgMsgSizeBytes: 500, MsgsPerSecond: 10},
	}
	// Empty consumer groups map - topic not in map at all
	report, err := a.DetectWaste(topics, map[string]time.Time{}, "aws-msk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Topic with no entry in consumerGroups should not be flagged as idle
	// (idle detection requires an entry with old timestamp)
	if len(report.IdleTopics) != 0 {
		t.Errorf("expected 0 idle topics (no consumer group entry), got %d", len(report.IdleTopics))
	}
}

func TestDetectWaste_CompressionAlreadyOptimal(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer(nil)
	topics := []TopicCostInput{
		{Name: "compressed", Partitions: 3, ReplicationFactor: 3, RetentionMs: 604800000, AvgMsgSizeBytes: 500, MsgsPerSecond: 10, CompressionType: "lz4"},
	}
	report, err := a.DetectWaste(topics, map[string]time.Time{}, "aws-msk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.CompressionOpps) != 0 {
		t.Errorf("expected 0 compression opps for lz4, got %d", len(report.CompressionOpps))
	}
}

func TestDetectWaste_EmptyTopics(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer(nil)
	report, err := a.DetectWaste(nil, nil, "aws-msk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.TotalMonthlyWaste != 0 {
		t.Errorf("expected 0 waste, got %f", report.TotalMonthlyWaste)
	}
	if len(report.Recommendations) != 0 {
		t.Errorf("expected 0 recommendations, got %d", len(report.Recommendations))
	}
}

func TestDetectWaste_RetentionOpportunity(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer(nil)
	topics := []TopicCostInput{
		{Name: "high-ret", Partitions: 6, ReplicationFactor: 3, RetentionMs: 90 * 24 * 60 * 60 * 1000, AvgMsgSizeBytes: 1024, MsgsPerSecond: 200},
	}
	report, err := a.DetectWaste(topics, map[string]time.Time{}, "aws-msk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.RetentionOpps) != 1 {
		t.Errorf("expected 1 retention opp, got %d", len(report.RetentionOpps))
	}
}

func TestDetectWaste_RecommendationMessages(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer(nil)
	topics := []TopicCostInput{
		{Name: "idle", Partitions: 3, ReplicationFactor: 3, RetentionMs: 604800000, AvgMsgSizeBytes: 500, MsgsPerSecond: 10},
		{Name: "over-rep", Partitions: 6, ReplicationFactor: 5, RetentionMs: 604800000, AvgMsgSizeBytes: 500, MsgsPerSecond: 50},
		{Name: "no-compress", Partitions: 6, ReplicationFactor: 3, RetentionMs: 604800000, AvgMsgSizeBytes: 500, MsgsPerSecond: 50, CompressionType: ""},
		{Name: "high-ret", Partitions: 6, ReplicationFactor: 3, RetentionMs: 90 * 24 * 60 * 60 * 1000, AvgMsgSizeBytes: 1024, MsgsPerSecond: 200},
	}
	consumers := map[string]time.Time{
		"idle": time.Now().Add(-30 * 24 * time.Hour),
	}
	report, err := a.DetectWaste(topics, consumers, "aws-msk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.Recommendations) < 3 {
		t.Errorf("expected at least 3 recommendations, got %d", len(report.Recommendations))
	}
}
