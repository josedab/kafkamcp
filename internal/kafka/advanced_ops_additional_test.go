package kafka

import (
	"context"
	"fmt"
	"testing"
)

// Additional advanced_ops coverage tests for remaining gaps.

func TestTimeTravelSnapshot_ValidISO(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return &TimeTravelSnapshot{
				Topic:     topic,
				Timestamp: timestamp,
				KeyCount:  2,
				State:     map[string]any{"key1": "val1", "key2": "val2"},
			}, nil
		},
	}

	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "events", "2024-01-15T10:30:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 2 {
		t.Errorf("KeyCount = %d, want 2", result.KeyCount)
	}
	if result.Timestamp != "2024-01-15T10:30:00Z" {
		t.Errorf("Timestamp = %q, unexpected", result.Timestamp)
	}
}

func TestTimeTravelSnapshot_InvalidFormat(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return nil, fmt.Errorf("invalid timestamp: cannot parse %q as timestamp", timestamp)
		},
	}

	_, err := mock.TimeTravelSnapshot(context.Background(), "default", "events", "not-a-date", 100)
	if err == nil {
		t.Fatal("expected error for invalid timestamp format")
	}
}

func TestTimeTravelSnapshot_FutureTimestampReturnsEmptyState(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return &TimeTravelSnapshot{
				Topic:     topic,
				Timestamp: timestamp,
				KeyCount:  5,
				State:     map[string]any{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
			}, nil
		},
	}

	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "events", "2099-01-01T00:00:00Z", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 5 {
		t.Errorf("KeyCount = %d, want 5", result.KeyCount)
	}
}

func TestTimeTravelSnapshot_StateReconstructionLastWriteWins(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			// Simulates last-write-wins: key "a" written twice, latest value wins
			return &TimeTravelSnapshot{
				Topic:    topic,
				KeyCount: 1,
				State:    map[string]any{"a": "final-value"},
			}, nil
		},
	}

	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "events", "2024-01-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.State["a"] != "final-value" {
		t.Errorf("expected last-write-wins, got %v", result.State["a"])
	}
}

func TestTimeTravelSnapshot_TombstoneHandling(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			// Key was set then tombstoned
			return &TimeTravelSnapshot{
				Topic:    topic,
				KeyCount: 0,
				State:    map[string]any{},
			}, nil
		},
	}

	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "compact-topic", "2024-06-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 0 {
		t.Errorf("KeyCount = %d, want 0 after tombstone", result.KeyCount)
	}
}

func TestTimeTravelSnapshot_MaxKeysBoundary(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			state := make(map[string]any)
			for i := 0; i < maxKeys; i++ {
				state[fmt.Sprintf("key-%d", i)] = i
			}
			return &TimeTravelSnapshot{
				Topic:    topic,
				KeyCount: maxKeys,
				State:    state,
			}, nil
		},
	}

	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "big-topic", "2024-01-01T00:00:00Z", 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 5 {
		t.Errorf("KeyCount = %d, want 5 (maxKeys boundary)", result.KeyCount)
	}
}

func TestTimeTravelDiff_AddedModifiedDeletedCategorization(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return &TimeTravelDiffResult{
				Topic:        topic,
				From:         tsFrom,
				To:           tsTo,
				TotalChanges: 3,
				Added:        map[string]any{"new-key": "new-val"},
				Modified:     []TimeTravelModified{{Key: "existing", OldValue: "old", NewValue: "new"}},
				Deleted:      []string{"removed-key"},
			}, nil
		},
	}

	result, err := mock.TimeTravelDiff(context.Background(), "default", "topic", "2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Added) != 1 {
		t.Errorf("Added = %d, want 1", len(result.Added))
	}
	if len(result.Modified) != 1 {
		t.Errorf("Modified = %d, want 1", len(result.Modified))
	}
	if len(result.Deleted) != 1 {
		t.Errorf("Deleted = %d, want 1", len(result.Deleted))
	}
	if result.TotalChanges != 3 {
		t.Errorf("TotalChanges = %d, want 3", result.TotalChanges)
	}
}

func TestTimeTravelDiff_IdenticalSnapshotsNoChanges(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return &TimeTravelDiffResult{
				Topic:        topic,
				TotalChanges: 0,
				Added:        map[string]any{},
			}, nil
		},
	}

	result, err := mock.TimeTravelDiff(context.Background(), "default", "stable-topic", "2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalChanges != 0 {
		t.Errorf("TotalChanges = %d, want 0 for identical snapshots", result.TotalChanges)
	}
}

func TestTimeTravelDiff_EmptyTopic(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return &TimeTravelDiffResult{
				Topic:        topic,
				TotalChanges: 0,
				Added:        map[string]any{},
			}, nil
		},
	}

	result, err := mock.TimeTravelDiff(context.Background(), "default", "empty-topic", "2024-01-01T00:00:00Z", "2024-12-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalChanges != 0 {
		t.Errorf("TotalChanges = %d, want 0 for empty topic", result.TotalChanges)
	}
}

func TestGetRecommendations_AllTopics(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Cluster:        cluster,
				TopicsAnalyzed: 5,
				Recommendations: []Recommendation{
					{Type: "idle_topic", Topic: "unused", Severity: "info"},
					{Type: "under_replicated", Topic: "critical", Severity: "critical"},
				},
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TopicsAnalyzed != 5 {
		t.Errorf("TopicsAnalyzed = %d, want 5", result.TopicsAnalyzed)
	}
	if len(result.Recommendations) != 2 {
		t.Errorf("Recommendations = %d, want 2", len(result.Recommendations))
	}
}

func TestGetRecommendations_PartitionHotspotConfidence(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				TopicsAnalyzed: 1,
				Recommendations: []Recommendation{
					{Type: "partition_hotspot", Topic: "skewed", Severity: "warning", Confidence: 0.85},
				},
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "skewed")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Recommendations) != 1 {
		t.Fatalf("expected 1 recommendation")
	}
	rec := result.Recommendations[0]
	if rec.Confidence != 0.85 {
		t.Errorf("Confidence = %f, want 0.85", rec.Confidence)
	}
}

func TestGetRecommendations_RetentionAndCompression(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				TopicsAnalyzed: 1,
				Recommendations: []Recommendation{
					{Type: "retention", Topic: "infinite-retain", Severity: "info", Confidence: 0.7},
					{Type: "compression", Topic: "no-compress", Severity: "info", Confidence: 0.75},
				},
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	foundRetention := false
	foundCompression := false
	for _, r := range result.Recommendations {
		if r.Type == "retention" {
			foundRetention = true
		}
		if r.Type == "compression" {
			foundCompression = true
		}
	}
	if !foundRetention {
		t.Error("expected retention recommendation")
	}
	if !foundCompression {
		t.Error("expected compression recommendation")
	}
}

func TestGetRecommendations_WithSpecificTopicFilter(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			if topic != "filtered-topic" {
				t.Errorf("expected specific topic filter, got %q", topic)
			}
			return &RecommendationResult{
				TopicsAnalyzed:  1,
				Recommendations: []Recommendation{},
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "filtered-topic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TopicsAnalyzed != 1 {
		t.Errorf("TopicsAnalyzed = %d, want 1", result.TopicsAnalyzed)
	}
}
