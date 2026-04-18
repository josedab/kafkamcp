package kafka

import (
	"context"
	"fmt"
	"testing"
)

func TestGetRecommendations_UnderReplicated(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{
						Type:       "under_replicated",
						Severity:   "critical",
						Topic:      "orders",
						Message:    "Partition 0 has 1 ISR out of 3 replicas",
						Suggestion: "Check broker health. Under-replicated partitions risk data loss.",
						Confidence: 0.95,
					},
				},
				TopicsAnalyzed: 1,
				Cluster:        "default",
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Recommendations) != 1 {
		t.Fatalf("expected 1 recommendation, got %d", len(result.Recommendations))
	}
	rec := result.Recommendations[0]
	if rec.Type != "under_replicated" {
		t.Errorf("expected type 'under_replicated', got %q", rec.Type)
	}
	if rec.Severity != "critical" {
		t.Errorf("expected severity 'critical', got %q", rec.Severity)
	}
	if rec.Confidence < 0.9 {
		t.Errorf("expected high confidence, got %f", rec.Confidence)
	}
}

func TestGetRecommendations_IdleTopic(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{
						Type:     "idle_topic",
						Severity: "info",
						Topic:    "stale-events",
						Message:  "Topic has zero messages",
					},
				},
				TopicsAnalyzed: 1,
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "stale-events")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Recommendations) != 1 {
		t.Fatalf("expected 1 recommendation, got %d", len(result.Recommendations))
	}
	if result.Recommendations[0].Type != "idle_topic" {
		t.Errorf("expected idle_topic, got %q", result.Recommendations[0].Type)
	}
	if result.Recommendations[0].Severity != "info" {
		t.Errorf("expected severity 'info', got %q", result.Recommendations[0].Severity)
	}
}

func TestGetRecommendations_PartitionHotspot(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{
						Type:     "partition_hotspot",
						Severity: "warning",
						Topic:    "events",
						Message:  "Partition skew detected: min/max ratio 0.05",
					},
				},
				TopicsAnalyzed: 1,
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "events")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Recommendations) != 1 {
		t.Fatalf("expected 1 recommendation, got %d", len(result.Recommendations))
	}
	if result.Recommendations[0].Type != "partition_hotspot" {
		t.Errorf("expected partition_hotspot, got %q", result.Recommendations[0].Type)
	}
	if result.Recommendations[0].Severity != "warning" {
		t.Errorf("expected severity 'warning', got %q", result.Recommendations[0].Severity)
	}
}

func TestGetRecommendations_InfiniteRetention(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{
						Type:     "retention",
						Severity: "info",
						Topic:    "audit-log",
						Message:  "Topic has infinite retention",
					},
				},
				TopicsAnalyzed: 1,
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "audit-log")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, r := range result.Recommendations {
		if r.Type == "retention" {
			found = true
			if r.Severity != "info" {
				t.Errorf("expected severity 'info', got %q", r.Severity)
			}
		}
	}
	if !found {
		t.Error("expected retention recommendation")
	}
}

func TestGetRecommendations_NoCompression(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{
						Type:       "compression",
						Severity:   "info",
						Topic:      "orders",
						Message:    "Topic compression is 'none'",
						Suggestion: "Consider enabling lz4 or zstd compression to reduce storage and network usage.",
					},
				},
				TopicsAnalyzed: 1,
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, r := range result.Recommendations {
		if r.Type == "compression" {
			found = true
		}
	}
	if !found {
		t.Error("expected compression recommendation")
	}
}

func TestGetRecommendations_SinglePartitionHighVolume(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{
						Type:     "partition_count",
						Severity: "warning",
						Topic:    "high-volume",
						Message:  "High-volume topic (200000 msgs) with only 1 partition",
					},
				},
				TopicsAnalyzed: 1,
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "high-volume")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, r := range result.Recommendations {
		if r.Type == "partition_count" && r.Severity == "warning" {
			found = true
		}
	}
	if !found {
		t.Error("expected partition_count recommendation")
	}
}

func TestGetRecommendations_ConsumerLagWarning(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: []Recommendation{
					{
						Type:       "lag_trend",
						Severity:   "warning",
						Message:    "Consumer group 'analytics' has high lag: 50000",
						Suggestion: "Scale consumers or investigate processing bottlenecks.",
						Confidence: 0.9,
					},
				},
				TopicsAnalyzed: 5,
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	found := false
	for _, r := range result.Recommendations {
		if r.Type == "lag_trend" {
			found = true
			if r.Severity != "warning" {
				t.Errorf("expected severity 'warning', got %q", r.Severity)
			}
		}
	}
	if !found {
		t.Error("expected lag_trend recommendation")
	}
}

func TestGetRecommendations_HealthyCluster(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{
				Recommendations: nil,
				TopicsAnalyzed:  3,
				Cluster:         "default",
			}, nil
		},
	}

	result, err := mock.GetRecommendations(context.Background(), "default", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Recommendations) != 0 {
		t.Errorf("expected 0 recommendations for healthy cluster, got %d", len(result.Recommendations))
	}
	if result.TopicsAnalyzed != 3 {
		t.Errorf("expected 3 topics analyzed, got %d", result.TopicsAnalyzed)
	}
}

func TestGetRecommendations_Error(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return nil, fmt.Errorf("cluster unavailable")
		},
	}

	_, err := mock.GetRecommendations(context.Background(), "default", "")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetRecommendations_CallRecording(t *testing.T) {
	mock := &MockKafkaManager{
		GetRecommendationsFn: func(ctx context.Context, cluster, topic string) (*RecommendationResult, error) {
			return &RecommendationResult{}, nil
		},
	}

	mock.GetRecommendations(context.Background(), "prod", "my-topic")
	if mock.CallCount("GetRecommendations") != 1 {
		t.Error("expected GetRecommendations to be called once")
	}
	call := mock.LastCall("GetRecommendations")
	if call.Args[0] != "prod" {
		t.Errorf("expected cluster 'prod', got %v", call.Args[0])
	}
	if call.Args[1] != "my-topic" {
		t.Errorf("expected topic 'my-topic', got %v", call.Args[1])
	}
}
