package anomaly

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
)

// Sensitivity levels for anomaly detection.
const (
	SensitivityLow    = "low"
	SensitivityMedium = "medium"
	SensitivityHigh   = "high"
)

// MetricType identifies the kind of metric being tracked.
type MetricType string

// MetricConsumerLag and related values define constants for this package.
const (
	MetricConsumerLag MetricType = "consumer_lag"
	MetricThroughput  MetricType = "throughput"
	MetricErrorRate   MetricType = "error_rate"
)

// DataPoint represents a single metric observation.
type DataPoint struct {
	Timestamp time.Time  `json:"timestamp"`
	Value     float64    `json:"value"`
	Metric    MetricType `json:"metric"`
	Source    string     `json:"source"` // topic or consumer group name
}

// Alert represents a detected anomaly.
type Alert struct {
	ID         string     `json:"id"`
	Metric     MetricType `json:"metric"`
	Source     string     `json:"source"`
	Severity   string     `json:"severity"` // warning, critical
	Value      float64    `json:"value"`
	Expected   float64    `json:"expected"`
	ZScore     float64    `json:"z_score"`
	DetectedAt time.Time  `json:"detected_at"`
	Message    string     `json:"message"`
	Hypothesis string     `json:"hypothesis"`
}

// DetectorConfig configures anomaly detection behavior.
type DetectorConfig struct {
	SampleIntervalSec int     `json:"sample_interval_sec" yaml:"sample_interval_sec"`
	BaselineWindowSec int     `json:"baseline_window_sec" yaml:"baseline_window_sec"`
	Sensitivity       string  `json:"sensitivity" yaml:"sensitivity"`
	ZScoreThreshold   float64 `json:"z_score_threshold" yaml:"z_score_threshold"`
	IQRMultiplier     float64 `json:"iqr_multiplier" yaml:"iqr_multiplier"`
	MaxAlerts         int     `json:"max_alerts" yaml:"max_alerts"`
	BufferSize        int     `json:"buffer_size" yaml:"buffer_size"`
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() DetectorConfig {
	return DetectorConfig{
		SampleIntervalSec: 10,
		BaselineWindowSec: 3600, // 1 hour
		Sensitivity:       SensitivityMedium,
		ZScoreThreshold:   3.0,
		IQRMultiplier:     1.5,
		MaxAlerts:         1000,
		BufferSize:        4096,
	}
}

// MetricCollector defines the interface for collecting metrics from Kafka.
type MetricCollector interface {
	CollectLag(ctx context.Context, cluster string) ([]DataPoint, error)
	CollectThroughput(ctx context.Context, cluster string) ([]DataPoint, error)
	CollectErrorRate(ctx context.Context, cluster string) ([]DataPoint, error)
}

// ringBuffer is a fixed-size circular buffer for data points.
type ringBuffer struct {
	mu    sync.RWMutex
	data  []DataPoint
	size  int
	head  int
	count int
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{
		data: make([]DataPoint, size),
		size: size,
	}
}

// Add is auto-documented.
func (rb *ringBuffer) Add(dp DataPoint) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.data[rb.head] = dp
	rb.head = (rb.head + 1) % rb.size
	if rb.count < rb.size {
		rb.count++
	}
}

// Snapshot is auto-documented.
func (rb *ringBuffer) Snapshot(since time.Time) []DataPoint {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	var result []DataPoint
	start := rb.head - rb.count
	if start < 0 {
		start += rb.size
	}
	for i := 0; i < rb.count; i++ {
		idx := (start + i) % rb.size
		if !rb.data[idx].Timestamp.Before(since) {
			result = append(result, rb.data[idx])
		}
	}
	return result
}

// Detector is the main anomaly detection engine.
type Detector struct {
	config    DetectorConfig
	buffers   map[string]*ringBuffer // keyed by "metric:source"
	alerts    []Alert
	alertsMu  sync.RWMutex
	buffersMu sync.RWMutex
	logger    *slog.Logger
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewDetector creates a new anomaly detector.
func NewDetector(cfg DetectorConfig, logger *slog.Logger) *Detector {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = DefaultConfig().BufferSize
	}
	if cfg.MaxAlerts <= 0 {
		cfg.MaxAlerts = DefaultConfig().MaxAlerts
	}
	if cfg.ZScoreThreshold <= 0 {
		cfg.ZScoreThreshold = DefaultConfig().ZScoreThreshold
	}
	if cfg.IQRMultiplier <= 0 {
		cfg.IQRMultiplier = DefaultConfig().IQRMultiplier
	}
	if cfg.SampleIntervalSec <= 0 {
		cfg.SampleIntervalSec = DefaultConfig().SampleIntervalSec
	}
	if cfg.BaselineWindowSec <= 0 {
		cfg.BaselineWindowSec = DefaultConfig().BaselineWindowSec
	}
	return &Detector{
		config:  cfg,
		buffers: make(map[string]*ringBuffer),
		logger:  logger,
	}
}

// Start begins background metric collection.
func (d *Detector) Start(ctx context.Context, collector MetricCollector, cluster string) {
	ctx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.wg.Add(1)
	go d.collectionLoop(ctx, collector, cluster)
}

// Stop halts the background collection.
func (d *Detector) Stop() {
	if d.cancel != nil {
		d.cancel()
	}
	d.wg.Wait()
}

func (d *Detector) collectionLoop(ctx context.Context, collector MetricCollector, cluster string) {
	defer d.wg.Done()
	ticker := time.NewTicker(time.Duration(d.config.SampleIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.collectAndAnalyze(ctx, collector, cluster)
		}
	}
}

func (d *Detector) collectAndAnalyze(ctx context.Context, collector MetricCollector, cluster string) {
	collectCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	if points, err := collector.CollectLag(collectCtx, cluster); err == nil {
		for _, p := range points {
			d.ingest(p)
		}
	}
	if points, err := collector.CollectThroughput(collectCtx, cluster); err == nil {
		for _, p := range points {
			d.ingest(p)
		}
	}
	if points, err := collector.CollectErrorRate(collectCtx, cluster); err == nil {
		for _, p := range points {
			d.ingest(p)
		}
	}
}

func (d *Detector) bufferKey(metric MetricType, source string) string {
	return fmt.Sprintf("%s:%s", metric, source)
}

func (d *Detector) ingest(dp DataPoint) {
	key := d.bufferKey(dp.Metric, dp.Source)

	d.buffersMu.Lock()
	buf, ok := d.buffers[key]
	if !ok {
		buf = newRingBuffer(d.config.BufferSize)
		d.buffers[key] = buf
	}
	d.buffersMu.Unlock()

	buf.Add(dp)

	baselineStart := time.Now().Add(-time.Duration(d.config.BaselineWindowSec) * time.Second)
	baseline := buf.Snapshot(baselineStart)
	if len(baseline) < 10 {
		return // not enough data for baseline
	}

	if alert := d.detectAnomaly(dp, baseline); alert != nil {
		d.addAlert(*alert)
	}
}

// IngestPoint allows external callers to add data points for analysis.
func (d *Detector) IngestPoint(dp DataPoint) {
	d.ingest(dp)
}

// detectAnomaly performs dual-method anomaly detection on a data point against
// a baseline window. It first checks the Z-score (standard deviations from the
// mean) with sensitivity-adjusted thresholds: High=2.0, Medium=3.0 (default),
// Low=4.0. If the Z-score is below threshold, it falls back to IQR outlier
// detection for robustness against non-normal distributions. Returns an Alert
// with severity "warning" or "critical" (when Z-score exceeds 1.5× threshold),
// or nil if no anomaly is detected.
func (d *Detector) detectAnomaly(current DataPoint, baseline []DataPoint) *Alert {
	values := make([]float64, len(baseline))
	for i, p := range baseline {
		values[i] = p.Value
	}

	mean, stddev := meanStddev(values)
	if stddev == 0 {
		return nil
	}

	zScore := (current.Value - mean) / stddev
	absZ := math.Abs(zScore)

	threshold := d.config.ZScoreThreshold
	switch d.config.Sensitivity {
	case SensitivityHigh:
		threshold = 2.0
	case SensitivityLow:
		threshold = 4.0
	}

	if absZ < threshold {
		// Also check IQR method for robustness
		if !d.iqrOutlier(current.Value, values) {
			return nil
		}
	}

	severity := "warning"
	if absZ > threshold*1.5 {
		severity = "critical"
	}

	return &Alert{
		ID:         fmt.Sprintf("%s-%s-%d", current.Metric, current.Source, current.Timestamp.UnixMilli()),
		Metric:     current.Metric,
		Source:     current.Source,
		Severity:   severity,
		Value:      current.Value,
		Expected:   mean,
		ZScore:     zScore,
		DetectedAt: time.Now(),
		Message:    d.generateMessage(current, mean, zScore),
		Hypothesis: d.generateHypothesis(current, mean, zScore),
	}
}

// iqrOutlier checks whether a value is an outlier using the Interquartile Range
// method. It computes Q1 (25th percentile) and Q3 (75th percentile), then flags
// values outside [Q1 - multiplier×IQR, Q3 + multiplier×IQR] as outliers. The
// IQR multiplier is configured via Detector.config.IQRMultiplier.
func (d *Detector) iqrOutlier(value float64, data []float64) bool {
	sorted := make([]float64, len(data))
	copy(sorted, data)
	sort.Float64s(sorted)

	q1 := percentile(sorted, 25)
	q3 := percentile(sorted, 75)
	iqr := q3 - q1
	if iqr == 0 {
		return false
	}

	lower := q1 - d.config.IQRMultiplier*iqr
	upper := q3 + d.config.IQRMultiplier*iqr
	return value < lower || value > upper
}

func (d *Detector) addAlert(alert Alert) {
	d.alertsMu.Lock()
	defer d.alertsMu.Unlock()

	d.alerts = append(d.alerts, alert)
	if len(d.alerts) > d.config.MaxAlerts {
		d.alerts = d.alerts[len(d.alerts)-d.config.MaxAlerts:]
	}
}

func (d *Detector) generateMessage(dp DataPoint, mean, zScore float64) string {
	direction := "spike"
	if dp.Value < mean {
		direction = "drop"
	}

	switch dp.Metric {
	case MetricConsumerLag:
		return fmt.Sprintf("Consumer lag %s detected for %s: %.0f (expected ~%.0f, z-score: %.2f)",
			direction, dp.Source, dp.Value, mean, zScore)
	case MetricThroughput:
		return fmt.Sprintf("Throughput %s detected for %s: %.0f msg/s (expected ~%.0f, z-score: %.2f)",
			direction, dp.Source, dp.Value, mean, zScore)
	case MetricErrorRate:
		return fmt.Sprintf("Error rate %s detected for %s: %.2f%% (expected ~%.2f%%, z-score: %.2f)",
			direction, dp.Source, dp.Value*100, mean*100, zScore)
	default:
		return fmt.Sprintf("Anomaly detected for %s on %s: %.2f (expected ~%.2f)", dp.Metric, dp.Source, dp.Value, mean)
	}
}

func (d *Detector) generateHypothesis(dp DataPoint, mean, zScore float64) string {
	switch dp.Metric {
	case MetricConsumerLag:
		if dp.Value > mean {
			return "Possible causes: consumer slowdown, processing backlog, partition rebalancing, or upstream producer burst. Check consumer group status and processing latency."
		}
		return "Possible causes: consumer caught up after backlog clearance, or reduced producer throughput."
	case MetricThroughput:
		if dp.Value > mean {
			return "Possible causes: upstream batch job, traffic spike, or replay event. Check producer applications and ingestion pipelines."
		}
		return "Possible causes: producer failure, network partition, upstream service outage, or intentional traffic reduction."
	case MetricErrorRate:
		if dp.Value > mean {
			return "Possible causes: schema incompatibility, downstream service failure, deserialization errors, or poison pill messages. Check DLQ topics and error logs."
		}
		return "Error rate normalized. Previous issue may be resolved."
	default:
		return "Review recent cluster changes and producer/consumer health."
	}
}

// GetAlerts returns recent alerts, optionally filtered.
func (d *Detector) GetAlerts(since time.Time, severity string, metric string, limit int) []Alert {
	d.alertsMu.RLock()
	defer d.alertsMu.RUnlock()

	var filtered []Alert
	for i := len(d.alerts) - 1; i >= 0 && (limit <= 0 || len(filtered) < limit); i-- {
		a := d.alerts[i]
		if !a.DetectedAt.Before(since) {
			if severity != "" && a.Severity != severity {
				continue
			}
			if metric != "" && string(a.Metric) != metric {
				continue
			}
			filtered = append(filtered, a)
		}
	}
	return filtered
}

// GetStatus returns a summary of the detector state.
func (d *Detector) GetStatus() map[string]any {
	d.buffersMu.RLock()
	bufferCount := len(d.buffers)
	d.buffersMu.RUnlock()

	d.alertsMu.RLock()
	alertCount := len(d.alerts)
	var recentCritical int
	cutoff := time.Now().Add(-1 * time.Hour)
	for i := len(d.alerts) - 1; i >= 0; i-- {
		if d.alerts[i].DetectedAt.Before(cutoff) {
			break
		}
		if d.alerts[i].Severity == "critical" {
			recentCritical++
		}
	}
	d.alertsMu.RUnlock()

	return map[string]any{
		"active_metrics":      bufferCount,
		"total_alerts":        alertCount,
		"critical_alerts_1h":  recentCritical,
		"sample_interval_sec": d.config.SampleIntervalSec,
		"baseline_window_sec": d.config.BaselineWindowSec,
		"sensitivity":         d.config.Sensitivity,
	}
}

// --- math helpers ---

func meanStddev(values []float64) (float64, float64) {
	n := float64(len(values))
	if n == 0 {
		return 0, 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / n

	var sqDiffSum float64
	for _, v := range values {
		diff := v - mean
		sqDiffSum += diff * diff
	}
	stddev := math.Sqrt(sqDiffSum / n)
	return mean, stddev
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p / 100.0) * float64(len(sorted)-1)
	lower := int(math.Floor(idx))
	upper := int(math.Ceil(idx))
	if lower == upper || upper >= len(sorted) {
		return sorted[lower]
	}
	frac := idx - float64(lower)
	return sorted[lower]*(1-frac) + sorted[upper]*frac
}
