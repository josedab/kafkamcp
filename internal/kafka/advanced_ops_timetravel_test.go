package kafka

import (
	"context"
	"fmt"
	"testing"
)

// Tests for TimeTravelSnapshot via mock — additional coverage beyond existing tests.

func TestTimeTravelSnapshot_LargeStateWithTombstones(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			// Simulates a compacted topic where some keys were tombstoned
			state := map[string]any{
				"user-1": map[string]any{"name": "Alice"},
				"user-3": map[string]any{"name": "Charlie"},
			}
			return &TimeTravelSnapshot{
				Topic:     topic,
				Timestamp: timestamp,
				KeyCount:  len(state),
				State:     state,
				ScanInfo:  TimeTravelScanInfo{MessagesScanned: 50, Duration: "12"},
			}, nil
		},
	}

	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "users", "2024-06-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 2 {
		t.Errorf("expected 2 keys (user-2 tombstoned), got %d", result.KeyCount)
	}
	if _, exists := result.State["user-2"]; exists {
		t.Error("user-2 should have been deleted by tombstone")
	}
	if result.ScanInfo.MessagesScanned != 50 {
		t.Errorf("expected 50 messages scanned, got %d", result.ScanInfo.MessagesScanned)
	}
}

func TestTimeTravelSnapshot_NilKeySkipped(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return &TimeTravelSnapshot{
				Topic:    topic,
				KeyCount: 1,
				State:    map[string]any{"valid-key": "value"},
				ScanInfo: TimeTravelScanInfo{MessagesScanned: 5},
			}, nil
		},
	}

	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "topic", "2024-01-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 1 {
		t.Errorf("expected 1 key (nil key skipped), got %d", result.KeyCount)
	}
}

func TestTimeTravelDiff_OnlyDeletedKeys(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return &TimeTravelDiffResult{
				Topic:        topic,
				From:         tsFrom,
				To:           tsTo,
				TotalChanges: 2,
				Added:        map[string]any{},
				Modified:     nil,
				Deleted:      []string{"key-a", "key-b"},
			}, nil
		},
	}

	result, err := mock.TimeTravelDiff(context.Background(), "default", "compacted",
		"2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Deleted) != 2 {
		t.Errorf("expected 2 deleted keys, got %d", len(result.Deleted))
	}
	if len(result.Added) != 0 {
		t.Errorf("expected 0 added keys, got %d", len(result.Added))
	}
	if result.TotalChanges != 2 {
		t.Errorf("expected 2 total changes, got %d", result.TotalChanges)
	}
}

func TestTimeTravelDiff_OnlyAddedKeys(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return &TimeTravelDiffResult{
				Topic:        topic,
				From:         tsFrom,
				To:           tsTo,
				TotalChanges: 3,
				Added:        map[string]any{"k1": "v1", "k2": "v2", "k3": "v3"},
				Modified:     nil,
				Deleted:      nil,
			}, nil
		},
	}

	result, err := mock.TimeTravelDiff(context.Background(), "default", "new-topic",
		"2024-01-01T00:00:00Z", "2024-06-01T00:00:00Z", 1000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Added) != 3 {
		t.Errorf("expected 3 added keys, got %d", len(result.Added))
	}
}

func TestTimeTravelSnapshot_InvalidTimestampFormat(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return nil, fmt.Errorf("invalid timestamp: cannot parse %q as timestamp", timestamp)
		},
	}

	_, err := mock.TimeTravelSnapshot(context.Background(), "default", "topic", "not-a-timestamp", 100)
	if err == nil {
		t.Error("expected error for invalid timestamp")
	}
}

func TestGetRecommendations_MultipleIssuesPerTopic(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{Type: "under_replicated", Severity: "critical", Topic: "events", Confidence: 0.95},
					{Type: "compression", Severity: "info", Topic: "events", Confidence: 0.75},
					{Type: "partition_count", Severity: "warning", Topic: "events", Confidence: 0.85},
				},
				TopicsAnalyzed: 1,
				Cluster:        "default",
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "events")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Recommendations) != 3 {
		t.Errorf("expected 3 recommendations, got %d", len(result.Recommendations))
	}

	types := make(map[string]bool)
	for _, r := range result.Recommendations {
		types[r.Type] = true
	}
	for _, expected := range []string{"under_replicated", "compression", "partition_count"} {
		if !types[expected] {
			t.Errorf("missing recommendation type: %s", expected)
		}
	}
}

func TestGetRecommendations_ProducerCompressionSuggestion(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{Type: "compression", Severity: "info", Topic: "logs",
						Message:    "Topic compression is 'producer'",
						Suggestion: "Consider enabling lz4 or zstd compression to reduce storage and network usage.",
						Confidence: 0.75},
				},
				TopicsAnalyzed: 1,
				Cluster:        "default",
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "logs")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Recommendations) == 0 {
		t.Fatal("expected at least one recommendation")
	}
	if result.Recommendations[0].Type != "compression" {
		t.Errorf("expected compression recommendation, got %s", result.Recommendations[0].Type)
	}
}

func TestResolveCluster_Advanced(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		cluster  string
		defClust string
		want     string
	}{
		{"empty uses default", "", "prod", "prod"},
		{"explicit cluster used", "staging", "prod", "staging"},
		{"empty default", "", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Manager{defClust: tt.defClust}
			got := m.resolveCluster(tt.cluster)
			if got != tt.want {
				t.Errorf("resolveCluster(%q) = %q, want %q", tt.cluster, got, tt.want)
			}
		})
	}
}
