package benchmark

import (
	"log/slog"
	"testing"
)

func TestGeneratePayload(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	payload := e.GeneratePayload(1024)
	if len(payload) != 1024 {
		t.Errorf("expected 1024 bytes, got %d", len(payload))
	}
}

func TestGeneratePayload_ZeroSize(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	payload := e.GeneratePayload(0)
	if len(payload) != 100 {
		t.Errorf("expected default 100 bytes, got %d", len(payload))
	}
}

func TestCreateBenchmark(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	result := e.CreateBenchmark(BenchmarkConfig{
		Topic:            "test-topic",
		MessageCount:     500,
		MessageSizeBytes: 256,
		Type:             "producer",
	})
	if result.Status != "pending" {
		t.Errorf("expected pending, got %s", result.Status)
	}
	if result.Config.MessageCount != 500 {
		t.Errorf("expected 500 messages, got %d", result.Config.MessageCount)
	}
}

func TestCreateBenchmark_Defaults(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	result := e.CreateBenchmark(BenchmarkConfig{Topic: "test"})
	if result.Config.MessageCount != 1000 {
		t.Errorf("expected default 1000 messages, got %d", result.Config.MessageCount)
	}
	if result.Config.Type != "producer" {
		t.Errorf("expected default type producer, got %s", result.Config.Type)
	}
}

func TestRecordMetrics(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	bench := e.CreateBenchmark(BenchmarkConfig{Topic: "test", Type: "producer"})

	latencies := []float64{1.0, 2.0, 3.0, 5.0, 10.0}
	err := e.RecordProducerMetrics(bench.ID, latencies, 5000, 0)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := e.GetResult(bench.ID)
	if got.Status != "completed" {
		t.Errorf("expected completed, got %s", got.Status)
	}
	if got.ProducerMetrics == nil {
		t.Error("expected producer metrics")
	}
	if got.ProducerMetrics.TotalMessages != 5 {
		t.Errorf("expected 5 messages, got %d", got.ProducerMetrics.TotalMessages)
	}
}

func TestRecordMetrics_Both(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	bench := e.CreateBenchmark(BenchmarkConfig{Topic: "test", Type: "both"})

	err := e.RecordProducerMetrics(bench.ID, []float64{1.0, 2.0}, 2000, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := e.GetResult(bench.ID)
	if got.Status == "completed" {
		t.Error("should not be completed until consumer metrics recorded")
	}

	err = e.RecordConsumerMetrics(bench.ID, []float64{1.5, 2.5}, 2000, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = e.GetResult(bench.ID)
	if got.Status != "completed" {
		t.Errorf("expected completed after both metrics, got %s", got.Status)
	}
}

func TestListResults(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())
	e.CreateBenchmark(BenchmarkConfig{Topic: "test-1"})
	e.CreateBenchmark(BenchmarkConfig{Topic: "test-2"})

	results := e.ListResults(10)
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}
