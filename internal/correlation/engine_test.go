package correlation

import (
	"log/slog"
	"testing"
	"time"
)

func TestIngestAndQuery(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	now := time.Now()
	for i := 0; i < 10; i++ {
		e.IngestSignal(Signal{
			Source:    SourceKafka,
			Metric:    "throughput",
			Value:     float64(100 + i*10),
			Timestamp: now.Add(time.Duration(-i) * time.Minute),
		})
	}

	signals := e.QuerySignals(SourceKafka, "throughput", now.Add(-1*time.Hour), 0)
	if len(signals) != 10 {
		t.Fatalf("expected 10 signals, got %d", len(signals))
	}
}

func TestCorrelate(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	now := time.Now()
	// Two correlated metrics: kafka throughput drops as CPU spikes
	for i := 0; i < 20; i++ {
		ts := now.Add(time.Duration(-i) * time.Minute)
		e.IngestSignal(Signal{
			Source:    SourceKafka,
			Metric:    "throughput",
			Value:     float64(1000 - i*50),
			Timestamp: ts,
			Severity:  "warning",
		})
		e.IngestSignal(Signal{
			Source:    SourcePrometheus,
			Metric:    "cpu_usage",
			Value:     float64(20 + i*4),
			Timestamp: ts,
		})
	}

	result := e.Correlate(time.Hour, "")
	if result.SignalCount != 40 {
		t.Errorf("expected 40 signals, got %d", result.SignalCount)
	}
	if len(result.Timeline) == 0 {
		t.Error("expected timeline events")
	}
	if len(result.RootCauses) == 0 {
		t.Error("expected root causes")
	}
}

func TestRegisterEndpoint(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	e.RegisterEndpoint(ExternalEndpoint{
		Source: SourcePrometheus,
		URL:    "http://prometheus:9090",
		APIKey: "secret-key",
		Active: true,
	})

	endpoints := e.ListEndpoints()
	if len(endpoints) != 1 {
		t.Fatalf("expected 1 endpoint, got %d", len(endpoints))
	}
	if endpoints[0].APIKey != "***" {
		t.Error("API key should be redacted")
	}
}

func TestMaxSignals(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.maxSignals = 100

	for i := 0; i < 200; i++ {
		e.IngestSignal(Signal{
			Source: SourceKafka,
			Metric: "test",
			Value:  float64(i),
		})
	}

	signals := e.QuerySignals("", "", time.Time{}, 0)
	if len(signals) > 100 {
		t.Errorf("expected max 100 signals, got %d", len(signals))
	}
}
