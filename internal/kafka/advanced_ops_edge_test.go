package kafka

import (
	"context"
	"errors"
	"testing"
)

func TestTimeTravelSnapshot_EmptyTopicEdge(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return &TimeTravelSnapshot{
				Topic:     topic,
				Timestamp: timestamp,
				KeyCount:  0,
				State:     map[string]any{},
				ScanInfo:  TimeTravelScanInfo{MessagesScanned: 0, Duration: "0"},
			}, nil
		},
	}
	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "empty-topic", "2025-01-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 0 {
		t.Errorf("expected 0 keys, got %d", result.KeyCount)
	}
	if len(result.State) != 0 {
		t.Errorf("expected empty state, got %v", result.State)
	}
}

func TestTimeTravelSnapshot_FutureTimestamp(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return &TimeTravelSnapshot{
				Topic:     topic,
				Timestamp: timestamp,
				KeyCount:  2,
				State:     map[string]any{"k1": "v1", "k2": "v2"},
				ScanInfo:  TimeTravelScanInfo{MessagesScanned: 2, Duration: "1"},
			}, nil
		},
	}
	result, err := mock.TimeTravelSnapshot(context.Background(), "default", "topic", "2099-01-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.KeyCount != 2 {
		t.Errorf("expected 2 keys for future timestamp, got %d", result.KeyCount)
	}
}

func TestTimeTravelSnapshot_MaxKeysZeroEdge(t *testing.T) {
	t.Parallel()
	var capturedMaxKeys int
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			capturedMaxKeys = maxKeys
			return &TimeTravelSnapshot{
				Topic:    topic,
				KeyCount: 0,
				State:    map[string]any{},
			}, nil
		},
	}
	_, err := mock.TimeTravelSnapshot(context.Background(), "default", "topic", "2026-01-01T00:00:00Z", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedMaxKeys != 0 {
		t.Errorf("expected maxKeys=0 passed through, got %d", capturedMaxKeys)
	}
}

func TestTimeTravelDiff_SameTimestampsEdge(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return &TimeTravelDiffResult{
				Topic:        topic,
				From:         tsFrom,
				To:           tsTo,
				TotalChanges: 0,
				Added:        map[string]any{},
			}, nil
		},
	}
	result, err := mock.TimeTravelDiff(context.Background(), "default", "topic", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalChanges != 0 {
		t.Errorf("expected 0 changes for same timestamps, got %d", result.TotalChanges)
	}
}

func TestTimeTravelDiff_AllChangeTypesEdge(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return &TimeTravelDiffResult{
				Topic:   topic,
				From:    tsFrom,
				To:      tsTo,
				Added:   map[string]any{"k3": "v3"},
				Deleted: []string{"k1"},
				Modified: []TimeTravelModified{
					{Key: "k2", OldValue: "v2a", NewValue: "v2b"},
				},
				TotalChanges: 3,
			}, nil
		},
	}
	result, err := mock.TimeTravelDiff(context.Background(), "default", "topic", "2025-01-01T00:00:00Z", "2025-06-01T00:00:00Z", 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Added) != 1 {
		t.Errorf("expected 1 added, got %d", len(result.Added))
	}
	if len(result.Deleted) != 1 {
		t.Errorf("expected 1 deleted, got %d", len(result.Deleted))
	}
	if len(result.Modified) != 1 {
		t.Errorf("expected 1 modified, got %d", len(result.Modified))
	}
	if result.TotalChanges != 3 {
		t.Errorf("expected 3 total changes, got %d", result.TotalChanges)
	}
}

func TestGetRecommendations_EmptyTopicListEdge(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: nil,
				TopicsAnalyzed:  0,
				Cluster:         cluster,
			}, nil
		},
	}
	result, err := mock.GetRecommendations(context.Background(), "default", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TopicsAnalyzed != 0 {
		t.Errorf("expected 0 topics, got %d", result.TopicsAnalyzed)
	}
}

func TestTimeTravelSnapshot_InvalidTimestampEdge(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelSnapshotFn: func(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
			return nil, errors.New("invalid timestamp")
		},
	}
	_, err := mock.TimeTravelSnapshot(context.Background(), "default", "topic", "not-a-date", 100)
	if err == nil {
		t.Fatal("expected error for invalid timestamp")
	}
}

func TestTimeTravelDiff_FromSnapshotErrorEdge(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		TimeTravelDiffFn: func(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
			return nil, errors.New("building from-snapshot: consume error")
		},
	}
	_, err := mock.TimeTravelDiff(context.Background(), "default", "topic", "bad", "2025-01-01T00:00:00Z", 100)
	if err == nil {
		t.Fatal("expected error from from-snapshot")
	}
}

func TestGetRecommendations_MixedEdge(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{Type: "under_replicated", Severity: "critical", Topic: "t1"},
					{Type: "idle_topic", Severity: "info", Topic: "t2"},
					{Type: "partition_hotspot", Severity: "warning", Topic: "t3"},
				},
				TopicsAnalyzed: 3,
				Cluster:        cluster,
			}, nil
		},
	}
	result, err := mock.GetRecommendations(context.Background(), "default", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TopicsAnalyzed != 3 {
		t.Errorf("expected 3, got %d", result.TopicsAnalyzed)
	}
	if len(result.Recommendations) != 3 {
		t.Errorf("expected 3 recommendations, got %d", len(result.Recommendations))
	}
}
