package timeseries

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
)

const (
	maxDataPoints = 10000
	defaultWindow = time.Hour
)

// MetricType identifies the type of metric being tracked.
type MetricType string

// MetricThroughputIn and related values define constants for this package.
const (
	MetricThroughputIn  MetricType = "throughput_in"
	MetricThroughputOut MetricType = "throughput_out"
	MetricLatencyP50    MetricType = "latency_p50"
	MetricLatencyP95    MetricType = "latency_p95"
	MetricLatencyP99    MetricType = "latency_p99"
	MetricConsumerLag   MetricType = "consumer_lag"
	MetricPartitionSkew MetricType = "partition_skew"
	MetricGroupVelocity MetricType = "group_velocity"
)

// DataPoint represents a single time-series observation.
type DataPoint struct {
	Timestamp time.Time         `json:"timestamp"`
	Value     float64           `json:"value"`
	Metric    MetricType        `json:"metric"`
	Source    string            `json:"source"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// Window represents a time window for analysis.
type Window struct {
	Name     string        `json:"name"`
	Duration time.Duration `json:"duration"`
}

// QueryResult contains the result of a time-series query.
type QueryResult struct {
	Metric MetricType  `json:"metric"`
	Source string      `json:"source"`
	Window string      `json:"window"`
	Points []DataPoint `json:"points"`
	Stats  Stats       `json:"stats"`
	Trend  Trend       `json:"trend"`
}

// Stats contains statistical summary of a time-series.
type Stats struct {
	Count  int     `json:"count"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	Median float64 `json:"median"`
	StdDev float64 `json:"std_dev"`
	P50    float64 `json:"p50"`
	P95    float64 `json:"p95"`
	P99    float64 `json:"p99"`
}

// Trend describes the overall trend in the time-series.
type Trend struct {
	Direction  string  `json:"direction"`
	Slope      float64 `json:"slope"`
	RSquared   float64 `json:"r_squared"`
	ChangeRate float64 `json:"change_rate_pct"`
}

// ComparisonResult contains the result of comparing two time windows.
type ComparisonResult struct {
	Metric      MetricType `json:"metric"`
	Source      string     `json:"source"`
	BaseWindow  string     `json:"base_window"`
	CompWindow  string     `json:"compare_window"`
	BaseStats   Stats      `json:"base_stats"`
	CompStats   Stats      `json:"compare_stats"`
	ChangePct   float64    `json:"change_pct"`
	Significant bool       `json:"significant"`
	Assessment  string     `json:"assessment"`
}

// BaselineResult represents a baseline analysis.
type BaselineResult struct {
	Metric      MetricType  `json:"metric"`
	Source      string      `json:"source"`
	Baseline    Stats       `json:"baseline"`
	Current     Stats       `json:"current"`
	Deviations  []Deviation `json:"deviations,omitempty"`
	HealthScore float64     `json:"health_score"`
	Assessment  string      `json:"assessment"`
}

// Deviation represents a point where metrics deviate from baseline.
type Deviation struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
	Expected  float64   `json:"expected"`
	ZScore    float64   `json:"z_score"`
	Severity  string    `json:"severity"`
}

// Engine manages time-series data collection and analysis.
type Engine struct {
	mu      sync.RWMutex
	series  map[string]*ringBuffer
	windows []Window
	logger  *slog.Logger
}

type ringBuffer struct {
	points []DataPoint
	size   int
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{
		points: make([]DataPoint, 0, size),
		size:   size,
	}
}

func (rb *ringBuffer) add(p DataPoint) {
	if len(rb.points) >= rb.size {
		rb.points = rb.points[1:]
	}
	rb.points = append(rb.points, p)
}

func (rb *ringBuffer) query(since time.Time) []DataPoint {
	var result []DataPoint
	for _, p := range rb.points {
		if !p.Timestamp.Before(since) {
			result = append(result, p)
		}
	}
	return result
}

// NewEngine creates a new time-series analytics engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		series: make(map[string]*ringBuffer),
		windows: []Window{
			{Name: "1h", Duration: time.Hour},
			{Name: "24h", Duration: 24 * time.Hour},
			{Name: "7d", Duration: 7 * 24 * time.Hour},
			{Name: "30d", Duration: 30 * 24 * time.Hour},
		},
		logger: logger,
	}
}

func seriesKey(metric MetricType, source string) string {
	return string(metric) + ":" + source
}

// Record stores a data point in the time-series.
func (e *Engine) Record(p DataPoint) {
	e.mu.Lock()
	defer e.mu.Unlock()

	key := seriesKey(p.Metric, p.Source)
	rb, ok := e.series[key]
	if !ok {
		rb = newRingBuffer(maxDataPoints)
		e.series[key] = rb
	}
	rb.add(p)
}

// Query retrieves time-series data for a metric/source within a window.
func (e *Engine) Query(metric MetricType, source, window string) (*QueryResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	w, err := e.resolveWindow(window)
	if err != nil {
		return nil, err
	}

	key := seriesKey(metric, source)
	rb, ok := e.series[key]
	if !ok {
		return &QueryResult{
			Metric: metric,
			Source: source,
			Window: window,
			Points: []DataPoint{},
			Stats:  Stats{},
		}, nil
	}

	since := time.Now().Add(-w.Duration)
	points := rb.query(since)

	stats := computeStats(points)
	trend := computeTrend(points)

	return &QueryResult{
		Metric: metric,
		Source: source,
		Window: window,
		Points: points,
		Stats:  stats,
		Trend:  trend,
	}, nil
}

// Compare compares metrics between two time windows.
func (e *Engine) Compare(metric MetricType, source, baseWindow, compWindow string) (*ComparisonResult, error) {
	base, err := e.Query(metric, source, baseWindow)
	if err != nil {
		return nil, fmt.Errorf("querying base window: %w", err)
	}
	comp, err := e.Query(metric, source, compWindow)
	if err != nil {
		return nil, fmt.Errorf("querying comparison window: %w", err)
	}

	changePct := 0.0
	if base.Stats.Mean != 0 {
		changePct = ((comp.Stats.Mean - base.Stats.Mean) / base.Stats.Mean) * 100
	}

	significant := math.Abs(changePct) > 10
	assessment := "stable"
	if changePct > 20 {
		assessment = "significant increase"
	} else if changePct > 10 {
		assessment = "moderate increase"
	} else if changePct < -20 {
		assessment = "significant decrease"
	} else if changePct < -10 {
		assessment = "moderate decrease"
	}

	return &ComparisonResult{
		Metric:      metric,
		Source:      source,
		BaseWindow:  baseWindow,
		CompWindow:  compWindow,
		BaseStats:   base.Stats,
		CompStats:   comp.Stats,
		ChangePct:   changePct,
		Significant: significant,
		Assessment:  assessment,
	}, nil
}

// Baseline computes a baseline from historical data and compares current metrics.
func (e *Engine) Baseline(metric MetricType, source, baselineWindow, currentWindow string) (*BaselineResult, error) {
	baseQ, err := e.Query(metric, source, baselineWindow)
	if err != nil {
		return nil, fmt.Errorf("querying baseline: %w", err)
	}
	currentQ, err := e.Query(metric, source, currentWindow)
	if err != nil {
		return nil, fmt.Errorf("querying current: %w", err)
	}

	var deviations []Deviation
	if baseQ.Stats.StdDev > 0 {
		for _, p := range currentQ.Points {
			zScore := (p.Value - baseQ.Stats.Mean) / baseQ.Stats.StdDev
			if math.Abs(zScore) > 2.0 {
				severity := "warning"
				if math.Abs(zScore) > 3.0 {
					severity = "critical"
				}
				deviations = append(deviations, Deviation{
					Timestamp: p.Timestamp,
					Value:     p.Value,
					Expected:  baseQ.Stats.Mean,
					ZScore:    zScore,
					Severity:  severity,
				})
			}
		}
	}

	// Health score: 100 = perfectly aligned, drops with deviations
	healthScore := 100.0
	if currentQ.Stats.Count > 0 {
		deviationRatio := float64(len(deviations)) / float64(currentQ.Stats.Count)
		healthScore = math.Max(0, 100*(1-deviationRatio*2))
	}

	assessment := "healthy"
	if healthScore < 50 {
		assessment = "degraded"
	} else if healthScore < 80 {
		assessment = "warning"
	}

	return &BaselineResult{
		Metric:      metric,
		Source:      source,
		Baseline:    baseQ.Stats,
		Current:     currentQ.Stats,
		Deviations:  deviations,
		HealthScore: healthScore,
		Assessment:  assessment,
	}, nil
}

// ListSeries returns all tracked metric series.
func (e *Engine) ListSeries() []map[string]string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []map[string]string
	for key := range e.series {
		parts := splitKey(key)
		result = append(result, map[string]string{
			"metric": parts[0],
			"source": parts[1],
			"points": fmt.Sprintf("%d", len(e.series[key].points)),
		})
	}
	return result
}

// GetWindows returns available analysis windows.
func (e *Engine) GetWindows() []Window {
	return e.windows
}

func (e *Engine) resolveWindow(name string) (Window, error) {
	for _, w := range e.windows {
		if w.Name == name {
			return w, nil
		}
	}
	return Window{}, fmt.Errorf("unknown window %q; available: 1h, 24h, 7d, 30d", name)
}

func splitKey(key string) [2]string {
	for i, c := range key {
		if c == ':' {
			return [2]string{key[:i], key[i+1:]}
		}
	}
	return [2]string{key, ""}
}

func computeStats(points []DataPoint) Stats {
	n := len(points)
	if n == 0 {
		return Stats{}
	}

	values := make([]float64, n)
	sum := 0.0
	for i, p := range points {
		values[i] = p.Value
		sum += p.Value
	}
	mean := sum / float64(n)

	sort.Float64s(values)
	min := values[0]
	max := values[n-1]
	median := values[n/2]

	varSum := 0.0
	for _, v := range values {
		d := v - mean
		varSum += d * d
	}
	stdDev := math.Sqrt(varSum / float64(n))

	return Stats{
		Count:  n,
		Min:    min,
		Max:    max,
		Mean:   mean,
		Median: median,
		StdDev: stdDev,
		P50:    percentile(values, 50),
		P95:    percentile(values, 95),
		P99:    percentile(values, 99),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func computeTrend(points []DataPoint) Trend {
	n := len(points)
	if n < 2 {
		return Trend{Direction: "insufficient_data"}
	}

	// Linear regression on time vs value
	var sumX, sumY, sumXY, sumX2 float64
	t0 := points[0].Timestamp.Unix()
	for _, p := range points {
		x := float64(p.Timestamp.Unix() - t0)
		y := p.Value
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}
	fn := float64(n)
	denom := fn*sumX2 - sumX*sumX
	if denom == 0 {
		return Trend{Direction: "flat", Slope: 0}
	}

	slope := (fn*sumXY - sumX*sumY) / denom
	intercept := (sumY - slope*sumX) / fn

	// R-squared
	meanY := sumY / fn
	var ssRes, ssTot float64
	for _, p := range points {
		x := float64(p.Timestamp.Unix() - t0)
		predicted := slope*x + intercept
		ssRes += (p.Value - predicted) * (p.Value - predicted)
		ssTot += (p.Value - meanY) * (p.Value - meanY)
	}
	rSquared := 0.0
	if ssTot > 0 {
		rSquared = 1 - ssRes/ssTot
	}

	// Change rate
	first := points[0].Value
	last := points[n-1].Value
	changeRate := 0.0
	if first != 0 {
		changeRate = ((last - first) / first) * 100
	}

	direction := "flat"
	if slope > 0.001 {
		direction = "increasing"
	} else if slope < -0.001 {
		direction = "decreasing"
	}

	return Trend{
		Direction:  direction,
		Slope:      slope,
		RSquared:   rSquared,
		ChangeRate: changeRate,
	}
}
