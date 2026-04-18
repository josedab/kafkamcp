package anomaly

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// mockCollector implements MetricCollector for testing.
type mockCollector struct {
	lagFn        func(ctx context.Context, cluster string) ([]DataPoint, error)
	throughputFn func(ctx context.Context, cluster string) ([]DataPoint, error)
	errorRateFn  func(ctx context.Context, cluster string) ([]DataPoint, error)
}

func (m *mockCollector) CollectLag(ctx context.Context, cluster string) ([]DataPoint, error) {
	if m.lagFn != nil {
		return m.lagFn(ctx, cluster)
	}
	return nil, nil
}

func (m *mockCollector) CollectThroughput(ctx context.Context, cluster string) ([]DataPoint, error) {
	if m.throughputFn != nil {
		return m.throughputFn(ctx, cluster)
	}
	return nil, nil
}

func (m *mockCollector) CollectErrorRate(ctx context.Context, cluster string) ([]DataPoint, error) {
	if m.errorRateFn != nil {
		return m.errorRateFn(ctx, cluster)
	}
	return nil, nil
}

func TestStartStop_CleanShutdown(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleIntervalSec = 1
	d := NewDetector(cfg, slog.Default())

	collector := &mockCollector{
		lagFn: func(ctx context.Context, cluster string) ([]DataPoint, error) {
			return []DataPoint{{Timestamp: time.Now(), Value: 100, Metric: MetricConsumerLag, Source: "test"}}, nil
		},
	}

	d.Start(collector, "default")

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	// Stop should complete without hanging
	done := make(chan struct{})
	go func() {
		d.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Clean shutdown
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not complete in time — possible goroutine leak")
	}
}

func TestCollectAndAnalyze_WithMockCollector(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg, slog.Default())

	var lagCalled, throughputCalled, errorRateCalled atomic.Int32

	collector := &mockCollector{
		lagFn: func(ctx context.Context, cluster string) ([]DataPoint, error) {
			lagCalled.Add(1)
			return []DataPoint{
				{Timestamp: time.Now(), Value: 500, Metric: MetricConsumerLag, Source: "group-1"},
			}, nil
		},
		throughputFn: func(ctx context.Context, cluster string) ([]DataPoint, error) {
			throughputCalled.Add(1)
			return []DataPoint{
				{Timestamp: time.Now(), Value: 1000, Metric: MetricThroughput, Source: "topic-1"},
			}, nil
		},
		errorRateFn: func(ctx context.Context, cluster string) ([]DataPoint, error) {
			errorRateCalled.Add(1)
			return []DataPoint{
				{Timestamp: time.Now(), Value: 0.01, Metric: MetricErrorRate, Source: "topic-1"},
			}, nil
		},
	}

	d.collectAndAnalyze(context.Background(), collector, "default")

	if lagCalled.Load() != 1 {
		t.Errorf("expected lag collector called once, got %d", lagCalled.Load())
	}
	if throughputCalled.Load() != 1 {
		t.Errorf("expected throughput collector called once, got %d", throughputCalled.Load())
	}
	if errorRateCalled.Load() != 1 {
		t.Errorf("expected error rate collector called once, got %d", errorRateCalled.Load())
	}

	status := d.GetStatus()
	if status["active_metrics"].(int) != 3 {
		t.Errorf("expected 3 active metrics after collection, got %v", status["active_metrics"])
	}
}

func TestCollectionLoop_FiresOnInterval(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleIntervalSec = 1
	d := NewDetector(cfg, slog.Default())

	var callCount atomic.Int32
	collector := &mockCollector{
		lagFn: func(ctx context.Context, cluster string) ([]DataPoint, error) {
			callCount.Add(1)
			return nil, nil
		},
	}

	d.Start(collector, "default")
	time.Sleep(2500 * time.Millisecond)
	d.Stop()

	calls := callCount.Load()
	if calls < 1 {
		t.Errorf("expected at least 1 collection cycle, got %d", calls)
	}
}

func TestCollectorReturningError_NoPanic(t *testing.T) {
	cfg := DefaultConfig()
	d := NewDetector(cfg, slog.Default())

	collector := &mockCollector{
		lagFn: func(ctx context.Context, cluster string) ([]DataPoint, error) {
			return nil, fmt.Errorf("kafka connection refused")
		},
		throughputFn: func(ctx context.Context, cluster string) ([]DataPoint, error) {
			return nil, fmt.Errorf("timeout")
		},
		errorRateFn: func(ctx context.Context, cluster string) ([]DataPoint, error) {
			return nil, fmt.Errorf("unknown error")
		},
	}

	// Should not panic
	d.collectAndAnalyze(context.Background(), collector, "default")

	status := d.GetStatus()
	if status["active_metrics"].(int) != 0 {
		t.Errorf("expected 0 metrics on errors, got %v", status["active_metrics"])
	}
}

func TestMultipleStartStop_Idempotency(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleIntervalSec = 1
	d := NewDetector(cfg, slog.Default())

	collector := &mockCollector{}

	for i := 0; i < 3; i++ {
		d.Start(collector, "default")
		time.Sleep(50 * time.Millisecond)
		d.Stop()
	}
	// Should not panic or deadlock
}

func TestStartStop_ContextCancel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SampleIntervalSec = 60 // long interval
	d := NewDetector(cfg, slog.Default())

	collector := &mockCollector{}

	d.Start(collector, "default")

	// Cancel via Stop (which calls d.cancel())
	done := make(chan struct{})
	go func() {
		d.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Good - context cancel caused quick shutdown
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() blocked too long — context cancel not working")
	}
}
