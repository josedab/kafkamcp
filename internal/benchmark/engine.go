package benchmark

import (
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// BenchmarkConfig defines the parameters for a benchmark run.
type BenchmarkConfig struct {
	Topic           string `json:"topic"`
	MessageCount    int    `json:"message_count"`
	MessageSizeBytes int   `json:"message_size_bytes"`
	Concurrency     int    `json:"concurrency"`
	Type            string `json:"type"` // producer, consumer, both
}

// BenchmarkResult contains the results of a benchmark run.
type BenchmarkResult struct {
	ID               string           `json:"id"`
	Config           BenchmarkConfig  `json:"config"`
	Status           string           `json:"status"`
	StartedAt        time.Time        `json:"started_at"`
	CompletedAt      *time.Time       `json:"completed_at,omitempty"`
	DurationMs       float64          `json:"duration_ms"`
	ProducerMetrics  *ThroughputMetrics `json:"producer_metrics,omitempty"`
	ConsumerMetrics  *ThroughputMetrics `json:"consumer_metrics,omitempty"`
	Summary          string           `json:"summary"`
}

// ThroughputMetrics contains throughput and latency statistics.
type ThroughputMetrics struct {
	TotalMessages    int     `json:"total_messages"`
	TotalBytes       int64   `json:"total_bytes"`
	MessagesPerSec   float64 `json:"messages_per_sec"`
	BytesPerSec      float64 `json:"bytes_per_sec"`
	MBPerSec         float64 `json:"mb_per_sec"`
	LatencyP50Ms     float64 `json:"latency_p50_ms"`
	LatencyP95Ms     float64 `json:"latency_p95_ms"`
	LatencyP99Ms     float64 `json:"latency_p99_ms"`
	LatencyAvgMs     float64 `json:"latency_avg_ms"`
	LatencyMinMs     float64 `json:"latency_min_ms"`
	LatencyMaxMs     float64 `json:"latency_max_ms"`
	ErrorCount       int     `json:"error_count"`
}

// Engine manages benchmark execution and results.
type Engine struct {
	mu      sync.RWMutex
	results map[string]*BenchmarkResult
	logger  *slog.Logger
	nextID  int64
}

// NewEngine creates a new benchmark engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		results: make(map[string]*BenchmarkResult),
		logger:  logger,
	}
}

// GeneratePayload creates a synthetic message payload of the given size.
func (e *Engine) GeneratePayload(sizeBytes int) []byte {
	if sizeBytes <= 0 {
		sizeBytes = 100
	}
	if sizeBytes > 10*1024*1024 {
		sizeBytes = 10 * 1024 * 1024
	}
	payload := make([]byte, sizeBytes)
	for i := range payload {
		payload[i] = byte('a' + rand.Intn(26))
	}
	return payload
}

// CreateBenchmark initializes a new benchmark run.
func (e *Engine) CreateBenchmark(cfg BenchmarkConfig) *BenchmarkResult {
	if cfg.MessageCount <= 0 {
		cfg.MessageCount = 1000
	}
	if cfg.MessageCount > 100000 {
		cfg.MessageCount = 100000
	}
	if cfg.MessageSizeBytes <= 0 {
		cfg.MessageSizeBytes = 100
	}
	if cfg.MessageSizeBytes > 1048576 {
		cfg.MessageSizeBytes = 1048576
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.Concurrency > 20 {
		cfg.Concurrency = 20
	}
	if cfg.Type == "" {
		cfg.Type = "producer"
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextID++
	id := fmt.Sprintf("bench_%d", e.nextID)

	result := &BenchmarkResult{
		ID:        id,
		Config:    cfg,
		Status:    "pending",
		StartedAt: time.Now(),
	}
	e.results[id] = result
	return result
}

// RecordProducerMetrics records the producer benchmark results from latency samples.
func (e *Engine) RecordProducerMetrics(benchID string, latencies []float64, totalBytes int64, errorCount int) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	result, ok := e.results[benchID]
	if !ok {
		return fmt.Errorf("benchmark %q not found", benchID)
	}

	metrics := computeMetrics(latencies, totalBytes, errorCount, result.StartedAt)
	result.ProducerMetrics = metrics
	result.Status = "running"

	if result.Config.Type == "producer" || (result.Config.Type == "both" && result.ConsumerMetrics != nil) {
		e.completeResult(result)
	}
	return nil
}

// RecordConsumerMetrics records the consumer benchmark results from latency samples.
func (e *Engine) RecordConsumerMetrics(benchID string, latencies []float64, totalBytes int64, errorCount int) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	result, ok := e.results[benchID]
	if !ok {
		return fmt.Errorf("benchmark %q not found", benchID)
	}

	metrics := computeMetrics(latencies, totalBytes, errorCount, result.StartedAt)
	result.ConsumerMetrics = metrics
	result.Status = "running"

	if result.Config.Type == "consumer" || (result.Config.Type == "both" && result.ProducerMetrics != nil) {
		e.completeResult(result)
	}
	return nil
}

func (e *Engine) completeResult(result *BenchmarkResult) {
	now := time.Now()
	result.CompletedAt = &now
	result.DurationMs = now.Sub(result.StartedAt).Seconds() * 1000
	result.Status = "completed"

	var parts []string
	if result.ProducerMetrics != nil {
		parts = append(parts, fmt.Sprintf("Producer: %.0f msgs/sec, %.2f MB/sec, P99=%.1fms",
			result.ProducerMetrics.MessagesPerSec, result.ProducerMetrics.MBPerSec, result.ProducerMetrics.LatencyP99Ms))
	}
	if result.ConsumerMetrics != nil {
		parts = append(parts, fmt.Sprintf("Consumer: %.0f msgs/sec, %.2f MB/sec, P99=%.1fms",
			result.ConsumerMetrics.MessagesPerSec, result.ConsumerMetrics.MBPerSec, result.ConsumerMetrics.LatencyP99Ms))
	}
	for _, p := range parts {
		if result.Summary != "" {
			result.Summary += "; "
		}
		result.Summary += p
	}
}

func computeMetrics(latencies []float64, totalBytes int64, errorCount int, startedAt time.Time) *ThroughputMetrics {
	if len(latencies) == 0 {
		return &ThroughputMetrics{ErrorCount: errorCount}
	}

	elapsed := time.Since(startedAt).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}

	sort.Float64s(latencies)

	var sum float64
	for _, l := range latencies {
		sum += l
	}

	return &ThroughputMetrics{
		TotalMessages:  len(latencies),
		TotalBytes:     totalBytes,
		MessagesPerSec: float64(len(latencies)) / elapsed,
		BytesPerSec:    float64(totalBytes) / elapsed,
		MBPerSec:       float64(totalBytes) / elapsed / 1048576,
		LatencyAvgMs:   sum / float64(len(latencies)),
		LatencyMinMs:   latencies[0],
		LatencyMaxMs:   latencies[len(latencies)-1],
		LatencyP50Ms:   percentile(latencies, 0.50),
		LatencyP95Ms:   percentile(latencies, 0.95),
		LatencyP99Ms:   percentile(latencies, 0.99),
		ErrorCount:     errorCount,
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// GetResult returns a benchmark result by ID.
func (e *Engine) GetResult(id string) (*BenchmarkResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result, ok := e.results[id]
	if !ok {
		return nil, fmt.Errorf("benchmark %q not found", id)
	}
	return result, nil
}

// ListResults returns recent benchmark results.
func (e *Engine) ListResults(limit int) []BenchmarkResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 20
	}

	var results []BenchmarkResult
	for _, r := range e.results {
		results = append(results, *r)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].StartedAt.After(results[j].StartedAt) })
	if len(results) > limit {
		results = results[:limit]
	}
	return results
}
