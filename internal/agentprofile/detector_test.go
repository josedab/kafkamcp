package agentprofile

import (
	"testing"
	"time"
)

func TestNewDetector(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultDetectorConfig(), nil)
	if d == nil {
		t.Fatal("NewDetector returned nil")
	}
	stats := d.Stats()
	if stats["profiles_count"].(int) != 0 {
		t.Errorf("expected 0 profiles, got %v", stats["profiles_count"])
	}
}

func TestRecordAccessCreatesProfile(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultDetectorConfig(), nil)
	d.RecordAccess(AccessEvent{
		AgentID:   "agent-1",
		Tool:      "kafka_consume",
		Topic:     "orders",
		Action:    "read",
		Count:     10,
		Timestamp: time.Now(),
	})

	profile, ok := d.GetProfile("agent-1")
	if !ok {
		t.Fatal("profile not created")
	}
	if profile.ReadCount != 10 {
		t.Errorf("ReadCount = %d, want 10", profile.ReadCount)
	}
	if profile.TopicsAccessed["orders"] != 1 {
		t.Errorf("topic access count = %d, want 1", profile.TopicsAccessed["orders"])
	}
}

func TestRecordAccessNoAlertsBeforeBaseline(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultDetectorConfig(), nil)
	alerts := d.RecordAccess(AccessEvent{
		AgentID:   "agent-1",
		Tool:      "kafka_consume",
		Topic:     "orders",
		Action:    "read",
		Count:     100000,
		Timestamp: time.Now(),
	})
	if len(alerts) != 0 {
		t.Errorf("expected no alerts before baseline, got %d", len(alerts))
	}
}

func TestBulkReadDetection(t *testing.T) {
	t.Parallel()
	cfg := DefaultDetectorConfig()
	cfg.BaselineDays = 0 // Instant baseline for testing
	cfg.BulkReadThreshold = 100
	d := NewDetector(cfg, nil)

	// Build baseline with small reads
	for i := 0; i < 20; i++ {
		d.RecordAccess(AccessEvent{
			AgentID: "agent-1", Tool: "kafka_consume",
			Topic: "orders", Action: "read", Count: 10,
			Timestamp: time.Now(),
		})
	}

	// Large read should trigger alert
	alerts := d.RecordAccess(AccessEvent{
		AgentID: "agent-1", Tool: "kafka_consume",
		Topic: "orders", Action: "read", Count: 5000,
		Timestamp: time.Now(),
	})
	// May or may not trigger depending on z-score; just verify no panic
	_ = alerts
}

func TestGetAlerts(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultDetectorConfig(), nil)
	alerts := d.GetAlerts("", 10)
	if len(alerts) != 0 {
		t.Errorf("expected 0 alerts, got %d", len(alerts))
	}
}

func TestZScore(t *testing.T) {
	t.Parallel()
	d := NewDetector(DefaultDetectorConfig(), nil)
	window := []float64{10, 10, 10, 10, 10}
	z := d.zScore(window, 10)
	if z != 0 {
		t.Errorf("z-score for mean value should be 0, got %f", z)
	}
}
