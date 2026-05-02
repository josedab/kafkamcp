package dataquality

import (
	"testing"
	"time"
)

func TestNewEngine(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	if e == nil {
		t.Fatal("NewEngine returned nil")
	}
}

func TestEvaluate(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	metrics := TopicMetrics{
		Topic:              "orders",
		MessageCount:       10000,
		NullFieldRate:      0.02,
		SchemaErrorRate:    0.01,
		AnomalyRate:        0.005,
		LastMessageTime:    time.Now().Add(-30 * time.Second),
		AvgMessageRate:     100,
		CurrentMessageRate: 95,
		SampleSize:         500,
	}

	score := e.Evaluate(metrics)
	if score.Topic != "orders" {
		t.Errorf("Topic = %q, want orders", score.Topic)
	}
	if score.OverallScore < 0.5 {
		t.Errorf("OverallScore = %f, expected > 0.5", score.OverallScore)
	}
	if len(score.Dimensions) != 5 {
		t.Errorf("expected 5 dimensions, got %d", len(score.Dimensions))
	}
	if score.Grade == "" {
		t.Error("Grade should not be empty")
	}
}

func TestEvaluatePoorQuality(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	metrics := TopicMetrics{
		Topic:           "stale-topic",
		NullFieldRate:   0.8,
		SchemaErrorRate: 0.5,
		AnomalyRate:     0.3,
		LastMessageTime: time.Now().Add(-48 * time.Hour),
		SampleSize:      100,
	}

	score := e.Evaluate(metrics)
	if score.OverallScore > 0.5 {
		t.Errorf("expected low score for poor quality data, got %f", score.OverallScore)
	}
}

func TestSetAndGetSLA(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	err := e.SetSLA(SLAConfig{
		Topic:           "orders",
		MinFreshness:    0.8,
		MinCompleteness: 0.95,
		MinOverallScore: 0.85,
	})
	if err != nil {
		t.Fatalf("SetSLA error: %v", err)
	}

	slas := e.ListSLAs()
	if len(slas) != 1 {
		t.Errorf("expected 1 SLA, got %d", len(slas))
	}
}

func TestSLABreach(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	_ = e.SetSLA(SLAConfig{
		Topic:           "orders",
		MinFreshness:    0.9,
		MinCompleteness: 0.95,
		MinOverallScore: 0.9,
	})

	// Evaluate with metrics that will breach SLA
	e.Evaluate(TopicMetrics{
		Topic:           "orders",
		NullFieldRate:   0.2,
		LastMessageTime: time.Now().Add(-2 * time.Hour),
		SampleSize:      100,
	})

	status, ok := e.GetSLAStatus("orders")
	if !ok {
		t.Fatal("SLA status not found")
	}
	if status.Compliant {
		t.Error("expected SLA breach")
	}
	if len(status.Breaches) == 0 {
		t.Error("expected breach details")
	}
}

func TestGetReport(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	e.Evaluate(TopicMetrics{Topic: "test", SampleSize: 50, LastMessageTime: time.Now()})

	report, err := e.GetReport("test")
	if err != nil {
		t.Fatalf("GetReport error: %v", err)
	}
	if report.Topic != "test" {
		t.Errorf("Topic = %q, want test", report.Topic)
	}
}

func TestGetReportNotFound(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	_, err := e.GetReport("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent topic")
	}
}

func TestSetSLAEmptyTopic(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)
	err := e.SetSLA(SLAConfig{})
	if err == nil {
		t.Error("expected error for empty topic")
	}
}

func TestGrade(t *testing.T) {
	t.Parallel()
	tests := []struct {
		score float64
		want  string
	}{
		{0.96, "A+"},
		{0.91, "A"},
		{0.86, "B+"},
		{0.81, "B"},
		{0.71, "C"},
		{0.61, "D"},
		{0.4, "F"},
	}
	for _, tt := range tests {
		got := grade(tt.score)
		if got != tt.want {
			t.Errorf("grade(%f) = %q, want %q", tt.score, got, tt.want)
		}
	}
}
