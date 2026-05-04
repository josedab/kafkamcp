package plugin

import (
	"log/slog"
	"testing"
)

func TestReviewStore(t *testing.T) {
	t.Parallel()
	rs := NewReviewStore()

	review, err := rs.AddReview("kafka-metrics-exporter", "plugin", "user1", 4.5, "Great plugin", "Works perfectly")
	if err != nil {
		t.Fatalf("AddReview: %v", err)
	}
	if review.ID == "" {
		t.Error("expected review ID")
	}

	rs.AddReview("kafka-metrics-exporter", "plugin", "user2", 5.0, "Excellent", "Love it")

	reviews := rs.GetReviews("kafka-metrics-exporter")
	if len(reviews) != 2 {
		t.Errorf("expected 2 reviews, got %d", len(reviews))
	}

	avg := rs.AverageRating("kafka-metrics-exporter")
	if avg != 4.75 {
		t.Errorf("expected avg 4.75, got %f", avg)
	}
}

func TestReviewValidation(t *testing.T) {
	t.Parallel()
	rs := NewReviewStore()

	_, err := rs.AddReview("", "plugin", "user1", 4.0, "", "")
	if err == nil {
		t.Error("expected error for empty item name")
	}

	_, err = rs.AddReview("test", "plugin", "user1", 0.5, "", "")
	if err == nil {
		t.Error("expected error for rating < 1.0")
	}

	_, err = rs.AddReview("test", "plugin", "user1", 5.5, "", "")
	if err == nil {
		t.Error("expected error for rating > 5.0")
	}
}

func TestDiscover(t *testing.T) {
	t.Parallel()
	logger := slog.Default()
	registry := NewRegistry(logger)
	marketplace := NewMarketplace(registry)
	store := NewAgentConfigStore()

	result := marketplace.Discover("", "", store)
	if result.TotalResults == 0 {
		t.Error("expected results from discovery")
	}
	if len(result.Plugins) == 0 {
		t.Error("expected plugins")
	}
	if len(result.AgentConfigs) == 0 {
		t.Error("expected agent configs")
	}
}

func TestDiscoverByCategory(t *testing.T) {
	t.Parallel()
	logger := slog.Default()
	registry := NewRegistry(logger)
	marketplace := NewMarketplace(registry)
	store := NewAgentConfigStore()

	result := marketplace.Discover("", "security", store)
	for _, p := range result.Plugins {
		if p.Category != "security" {
			t.Errorf("expected security category, got %s", p.Category)
		}
	}
}

func TestGetStats(t *testing.T) {
	t.Parallel()
	logger := slog.Default()
	registry := NewRegistry(logger)
	marketplace := NewMarketplace(registry)
	store := NewAgentConfigStore()
	reviewStore := NewReviewStore()
	reviewStore.AddReview("test", "plugin", "user", 4.0, "Good", "Nice")

	stats := marketplace.GetStats(store, reviewStore)
	if stats.TotalPlugins == 0 {
		t.Error("expected plugins in stats")
	}
	if stats.TotalAgentConfigs == 0 {
		t.Error("expected agent configs in stats")
	}
	if len(stats.Categories) == 0 {
		t.Error("expected categories")
	}
	if stats.TotalReviews != 1 {
		t.Errorf("expected 1 review, got %d", stats.TotalReviews)
	}
}
