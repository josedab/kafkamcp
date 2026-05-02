package cost

import (
	"testing"
	"time"
)

func TestAnalyzeTopic(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer(nil)

	input := TopicCostInput{
		Name:              "orders",
		Partitions:        12,
		ReplicationFactor: 3,
		RetentionMs:       7 * 24 * 60 * 60 * 1000,
		AvgMsgSizeBytes:   1024,
		MsgsPerSecond:     100,
		CompressionType:   "none",
	}

	result, err := a.AnalyzeTopic(input, "aws-msk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.StorageGB <= 0 {
		t.Error("expected positive storage")
	}
	if result.TotalCostMonth <= 0 {
		t.Error("expected positive cost")
	}
	if result.TotalCostYear <= 0 {
		t.Error("expected positive yearly cost")
	}
}

func TestDetectWaste(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer(nil)

	topics := []TopicCostInput{
		{Name: "idle-topic", Partitions: 3, ReplicationFactor: 3, RetentionMs: 604800000, AvgMsgSizeBytes: 500, MsgsPerSecond: 10},
		{Name: "over-rep", Partitions: 6, ReplicationFactor: 5, RetentionMs: 604800000, AvgMsgSizeBytes: 500, MsgsPerSecond: 50},
		{Name: "no-compress", Partitions: 6, ReplicationFactor: 3, RetentionMs: 604800000, AvgMsgSizeBytes: 500, MsgsPerSecond: 50, CompressionType: "none"},
	}

	consumers := map[string]time.Time{
		"idle-topic": time.Now().Add(-30 * 24 * time.Hour),
	}

	report, err := a.DetectWaste(topics, consumers, "aws-msk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(report.IdleTopics) != 1 {
		t.Errorf("expected 1 idle topic, got %d", len(report.IdleTopics))
	}
	if len(report.OverReplicated) != 1 {
		t.Errorf("expected 1 over-replicated, got %d", len(report.OverReplicated))
	}
	if len(report.CompressionOpps) == 0 {
		t.Error("expected compression opportunities")
	}
}

func TestWhatIf(t *testing.T) {
	t.Parallel()
	a := NewAnalyzer(nil)

	current := TopicCostInput{
		Name:              "orders",
		Partitions:        12,
		ReplicationFactor: 3,
		RetentionMs:       30 * 24 * 60 * 60 * 1000,
		AvgMsgSizeBytes:   1024,
		MsgsPerSecond:     100,
	}

	newRetention := int64(7 * 24 * 60 * 60 * 1000)
	result, err := a.WhatIf(current, WhatIfScenario{Topic: "orders", NewRetentionMs: &newRetention}, "aws-msk")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Savings <= 0 {
		t.Error("expected positive savings from reducing retention")
	}
	if result.SavingsPct <= 0 {
		t.Error("expected positive savings percentage")
	}
}
