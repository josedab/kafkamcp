package healthscore

import (
	"log/slog"
	"testing"
)

func TestEvaluate_HealthyCluster(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	report := e.Evaluate("test-cluster", ClusterMetricsInput{
		TotalBrokers:          3,
		OnlineBrokers:         3,
		TotalPartitions:       100,
		ActiveControllerCount: 1,
	})

	if report.OverallScore < 90 {
		t.Errorf("expected healthy score >= 90, got %f", report.OverallScore)
	}
	if report.Status != "healthy" {
		t.Errorf("expected status healthy, got %s", report.Status)
	}
}

func TestEvaluate_DegradedCluster(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	report := e.Evaluate("test-cluster", ClusterMetricsInput{
		TotalBrokers:              3,
		OnlineBrokers:             2,
		UnderReplicatedPartitions: 10,
		TotalPartitions:           100,
		ISRShrinkRate:             2.0,
		MaxConsumerLag:            500000,
		ActiveControllerCount:     1,
	})

	if report.OverallScore >= 90 {
		t.Errorf("expected degraded score < 90, got %f", report.OverallScore)
	}
	if len(report.Recommendations) == 0 {
		t.Error("expected recommendations for degraded cluster")
	}
}

func TestEvaluate_TrendTracking(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.Evaluate("test-cluster", ClusterMetricsInput{
		TotalBrokers: 3, OnlineBrokers: 2, TotalPartitions: 100, ActiveControllerCount: 1,
	})
	report := e.Evaluate("test-cluster", ClusterMetricsInput{
		TotalBrokers: 3, OnlineBrokers: 3, TotalPartitions: 100, ActiveControllerCount: 1,
	})

	if report.TrendDirection != "improving" {
		t.Errorf("expected improving trend, got %s", report.TrendDirection)
	}
}

func TestGetHistory(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	for i := 0; i < 5; i++ {
		e.Evaluate("test-cluster", ClusterMetricsInput{
			TotalBrokers: 3, OnlineBrokers: 3, TotalPartitions: 100, ActiveControllerCount: 1,
		})
	}

	history := e.GetHistory("test-cluster", 3)
	if len(history) != 3 {
		t.Errorf("expected 3 history entries, got %d", len(history))
	}
}
