package advisor

import (
	"log/slog"
	"testing"
)

func TestScan_UnderReplicated(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "orders", Partitions: 6, ReplicationFactor: 1, RetentionMs: 86400000, MessageCount: 1000},
	}
	result := e.Scan("cluster", topics)
	found := false
	for _, r := range result.Recommendations {
		if r.Category == "replication" && r.Severity == "critical" {
			found = true
		}
	}
	if !found {
		t.Error("expected critical replication recommendation for RF=1")
	}
}

func TestScan_IdleTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "orphan", Partitions: 3, ReplicationFactor: 3, RetentionMs: 86400000, MessageCount: 500},
	}
	result := e.Scan("cluster", topics)
	found := false
	for _, ap := range result.AntiPatterns {
		if ap.Type == "orphaned_topics" {
			found = true
		}
	}
	if !found {
		t.Error("expected orphaned_topics anti-pattern for topic with no consumer groups")
	}
}

func TestScan_PartitionHotspot(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "hot", Partitions: 1, ReplicationFactor: 3, RetentionMs: 86400000, MessageCount: 50000},
	}
	result := e.Scan("cluster", topics)
	found := false
	for _, ap := range result.AntiPatterns {
		if ap.Type == "single_partition_high_volume" {
			found = true
		}
	}
	if !found {
		t.Error("expected single_partition_high_volume anti-pattern")
	}
}

func TestScan_InternalTopicsSkipped(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "__consumer_offsets", Partitions: 50, ReplicationFactor: 1, IsInternal: true, MessageCount: 100},
	}
	result := e.Scan("cluster", topics)
	for _, r := range result.Recommendations {
		if r.Topic == "__consumer_offsets" {
			t.Error("internal topics should be skipped in best practice checks")
		}
	}
}

func TestScan_AllHealthyTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "orders.v1", Partitions: 6, ReplicationFactor: 3, RetentionMs: 86400000, MessageCount: 1000, ConsumerGroups: []string{"g1"}},
	}
	result := e.Scan("cluster", topics)
	if result.Score < 80 {
		t.Errorf("expected high score for healthy topic, got %d", result.Score)
	}
}

func TestScan_EmptyTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	result := e.Scan("cluster", nil)
	if result.TopicsScanned != 0 {
		t.Errorf("expected 0 scanned, got %d", result.TopicsScanned)
	}
	if result.Score != 100 {
		t.Errorf("expected score 100 for empty, got %d", result.Score)
	}
}

func TestScan_HighRetention(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "logs", Partitions: 3, ReplicationFactor: 3, RetentionMs: 90 * 24 * 60 * 60 * 1000, MessageCount: 100, ConsumerGroups: []string{"g1"}},
	}
	result := e.Scan("cluster", topics)
	found := false
	for _, r := range result.Recommendations {
		if r.Category == "retention" {
			found = true
		}
	}
	if !found {
		t.Error("expected retention recommendation for 90-day retention")
	}
}

func TestScan_LowRetention(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "ephemeral", Partitions: 3, ReplicationFactor: 3, RetentionMs: 1000, MessageCount: 100, ConsumerGroups: []string{"g1"}},
	}
	result := e.Scan("cluster", topics)
	found := false
	for _, r := range result.Recommendations {
		if r.Category == "retention" && r.Severity == "warning" {
			found = true
		}
	}
	if !found {
		t.Error("expected warning for very short retention")
	}
}

func TestScan_ExcessivePartitions(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "wide", Partitions: 200, ReplicationFactor: 3, RetentionMs: 86400000, MessageCount: 100, ConsumerGroups: []string{"g1"}},
	}
	result := e.Scan("cluster", topics)
	found := false
	for _, r := range result.Recommendations {
		if r.Category == "partitioning" {
			found = true
		}
	}
	if !found {
		t.Error("expected partitioning recommendation for 200 partitions")
	}
}

func TestScan_NamingConvention(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "my topic with spaces", Partitions: 3, ReplicationFactor: 3, RetentionMs: 86400000},
	}
	result := e.Scan("cluster", topics)
	found := false
	for _, r := range result.Recommendations {
		if r.Category == "naming" {
			found = true
		}
	}
	if !found {
		t.Error("expected naming recommendation for topic with spaces")
	}
}

func TestSetConfig(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	cfg := DefaultRuleConfig()
	cfg.MinReplicationFactor = 2
	e.SetConfig(cfg)

	topics := []TopicMetrics{
		{Name: "t1", Partitions: 3, ReplicationFactor: 2, RetentionMs: 86400000},
	}
	result := e.Scan("cluster", topics)
	for _, r := range result.Recommendations {
		if r.Category == "replication" {
			t.Error("RF=2 should be OK with MinReplicationFactor=2")
		}
	}
}

func TestDefaultRuleConfig(t *testing.T) {
	t.Parallel()
	cfg := DefaultRuleConfig()
	if cfg.MinReplicationFactor != 3 {
		t.Errorf("expected min RF 3, got %d", cfg.MinReplicationFactor)
	}
	if cfg.TopicNamingConvention != "dot-separated" {
		t.Errorf("expected dot-separated, got %s", cfg.TopicNamingConvention)
	}
	if cfg.MaxPartitionsPerTopic != 100 {
		t.Errorf("expected max partitions 100, got %d", cfg.MaxPartitionsPerTopic)
	}
}

func TestGenerateReport_ZeroTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	report := e.GenerateReport("cluster", nil)
	if report.TopicCount != 0 {
		t.Errorf("expected 0 topics, got %d", report.TopicCount)
	}
	if report.OverallScore != 100 {
		t.Errorf("expected score 100, got %d", report.OverallScore)
	}
}

func TestGenerateReport_SingleTopic(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "orders.v1", Partitions: 6, ReplicationFactor: 3, RetentionMs: 86400000, MessageCount: 1000, ConsumerGroups: []string{"g1"}},
	}
	report := e.GenerateReport("cluster", topics)
	if report.TopicCount != 1 {
		t.Errorf("expected 1 topic, got %d", report.TopicCount)
	}
	if report.PartitionCount != 6 {
		t.Errorf("expected 6 partitions, got %d", report.PartitionCount)
	}
	if report.Summary == "" {
		t.Error("expected non-empty summary")
	}
}

func TestGenerateReport_ManyTopics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	var topics []TopicMetrics
	for i := 0; i < 5; i++ {
		topics = append(topics, TopicMetrics{
			Name: "t", Partitions: 3, ReplicationFactor: 1, RetentionMs: 86400000, MessageCount: 100,
		})
	}
	report := e.GenerateReport("cluster", topics)
	if report.TopicCount != 5 {
		t.Errorf("expected 5 topics, got %d", report.TopicCount)
	}
	if len(report.CategoryScores) == 0 {
		t.Error("expected category scores")
	}
}

func TestGenerateReport_CategoryScoreDeduction(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "t", Partitions: 3, ReplicationFactor: 1, RetentionMs: 86400000, MessageCount: 100},
	}
	report := e.GenerateReport("cluster", topics)
	if report.CategoryScores["replication"] >= 100 {
		t.Error("expected replication score < 100 due to low RF")
	}
}

func TestScan_NegativeMetrics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	topics := []TopicMetrics{
		{Name: "t", Partitions: -1, ReplicationFactor: -1, RetentionMs: -1, MessageCount: -1},
	}
	result := e.Scan("cluster", topics)
	// Should not panic; negative values trigger replication check
	if result.TopicsScanned != 1 {
		t.Errorf("expected 1 scanned, got %d", result.TopicsScanned)
	}
}

func TestScan_ScoreFloorAtZero(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	var topics []TopicMetrics
	for i := 0; i < 50; i++ {
		topics = append(topics, TopicMetrics{
			Name: "t", Partitions: 200, ReplicationFactor: 1, RetentionMs: 86400000, MessageCount: 50000,
		})
	}
	result := e.Scan("cluster", topics)
	if result.Score < 0 {
		t.Errorf("score should not be negative, got %d", result.Score)
	}
}
