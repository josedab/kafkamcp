package predictalert

import (
	"log/slog"
	"testing"
)

func TestRecordAndAnalyze_Empty(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	analysis := e.Analyze("test-group")
	if analysis.LagTrend != "unknown" {
		t.Errorf("expected unknown trend for empty group, got %s", analysis.LagTrend)
	}
}

func TestRecordAndAnalyze_Stable(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	for i := 0; i < 10; i++ {
		e.RecordSample("test-group", 1000, 3)
	}
	analysis := e.Analyze("test-group")
	if analysis.LagTrend != "stable" {
		t.Errorf("expected stable trend, got %s", analysis.LagTrend)
	}
	if analysis.CurrentLag != 1000 {
		t.Errorf("expected current lag 1000, got %d", analysis.CurrentLag)
	}
}

func TestRecordAndAnalyze_RebalanceRisk(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	for i := 0; i < 10; i++ {
		members := 3
		if i%2 == 0 {
			members = 5
		}
		e.RecordSample("test-group", 1000, members)
	}
	analysis := e.Analyze("test-group")
	if analysis.RebalanceRisk == "low" {
		t.Error("expected non-low rebalance risk with frequent member changes")
	}
}

func TestListAlerts(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	for i := 0; i < 20; i++ {
		lag := int64(1000 * (i + 1))
		e.RecordSample("growing-group", lag, 3)
	}
	e.Analyze("growing-group")

	alerts := e.ListAlerts("", 50)
	// Alerts are generated based on analysis, just verify no panic
	_ = alerts
}

func TestAcknowledgeAlert(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	err := e.AcknowledgeAlert("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent alert")
	}
}
