package perfguard

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
)

// MetricName identifies a performance metric.
type MetricName string

// MetricProduceLatency and related values define constants for this package.
const (
	MetricProduceLatency MetricName = "produce_latency_ms"
	MetricConsumeLatency MetricName = "consume_latency_ms"
	MetricThroughput     MetricName = "throughput_msg_sec"
	MetricErrorRate      MetricName = "error_rate"
)

// Sample is a single performance observation.
type Sample struct {
	AgentID   string     `json:"agent_id"`
	Metric    MetricName `json:"metric"`
	Value     float64    `json:"value"`
	Timestamp time.Time  `json:"timestamp"`
}

// Baseline stores statistical baseline for a metric.
type Baseline struct {
	AgentID     string     `json:"agent_id"`
	Metric      MetricName `json:"metric"`
	Mean        float64    `json:"mean"`
	StdDev      float64    `json:"std_dev"`
	P50         float64    `json:"p50"`
	P95         float64    `json:"p95"`
	P99         float64    `json:"p99"`
	SampleCount int        `json:"sample_count"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// Regression represents a detected performance regression.
type Regression struct {
	ID            string     `json:"id"`
	AgentID       string     `json:"agent_id"`
	Metric        MetricName `json:"metric"`
	Severity      string     `json:"severity"` // warning, critical
	CurrentMean   float64    `json:"current_mean"`
	BaselineMean  float64    `json:"baseline_mean"`
	ChangePercent float64    `json:"change_percent"`
	CUSUMValue    float64    `json:"cusum_value"`
	DetectedAt    time.Time  `json:"detected_at"`
	Message       string     `json:"message"`
	AutoThrottled bool       `json:"auto_throttled"`
}

// ThrottleStatus tracks per-agent throttle state.
type ThrottleStatus struct {
	AgentID       string    `json:"agent_id"`
	Throttled     bool      `json:"throttled"`
	Reason        string    `json:"reason,omitempty"`
	ThrottledAt   time.Time `json:"throttled_at,omitempty"`
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
	OriginalRate  int       `json:"original_rate,omitempty"`
	ThrottledRate int       `json:"throttled_rate,omitempty"`
}

// GuardConfig configures the performance guard.
type GuardConfig struct {
	BaselineWindow    int     `json:"baseline_window" yaml:"baseline_window"`       // samples for baseline
	DetectionWindow   int     `json:"detection_window" yaml:"detection_window"`     // samples for detection
	CUSUMThreshold    float64 `json:"cusum_threshold" yaml:"cusum_threshold"`       // CUSUM alarm threshold
	CUSUMSlack        float64 `json:"cusum_slack" yaml:"cusum_slack"`               // CUSUM slack parameter
	WarningThreshold  float64 `json:"warning_threshold" yaml:"warning_threshold"`   // % change for warning
	CriticalThreshold float64 `json:"critical_threshold" yaml:"critical_threshold"` // % change for critical
	AutoThrottle      bool    `json:"auto_throttle" yaml:"auto_throttle"`
	ThrottleDuration  string  `json:"throttle_duration" yaml:"throttle_duration"`
	MaxRegressions    int     `json:"max_regressions" yaml:"max_regressions"`
}

// DefaultGuardConfig returns sensible defaults.
func DefaultGuardConfig() GuardConfig {
	return GuardConfig{
		BaselineWindow:    100,
		DetectionWindow:   20,
		CUSUMThreshold:    5.0,
		CUSUMSlack:        0.5,
		WarningThreshold:  20.0,
		CriticalThreshold: 50.0,
		AutoThrottle:      true,
		ThrottleDuration:  "5m",
		MaxRegressions:    1000,
	}
}

type agentMetrics struct {
	samples  []Sample
	baseline *Baseline
	cusumPos float64
	cusumNeg float64
}

// Guard manages performance monitoring and regression detection.
type Guard struct {
	config      GuardConfig
	mu          sync.RWMutex
	metrics     map[string]*agentMetrics // key: "agentID:metric"
	regressions []Regression
	throttles   map[string]*ThrottleStatus
	nextID      int64
	logger      *slog.Logger
}

// NewGuard creates a new performance guard.
func NewGuard(cfg GuardConfig, logger *slog.Logger) *Guard {
	return &Guard{
		config:    cfg,
		metrics:   make(map[string]*agentMetrics),
		throttles: make(map[string]*ThrottleStatus),
		logger:    logger,
	}
}

// RecordSample records a performance sample and checks for regressions.
func (g *Guard) RecordSample(sample Sample) *Regression {
	if sample.Timestamp.IsZero() {
		sample.Timestamp = time.Now()
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	key := fmt.Sprintf("%s:%s", sample.AgentID, sample.Metric)
	am, ok := g.metrics[key]
	if !ok {
		am = &agentMetrics{}
		g.metrics[key] = am
	}

	am.samples = append(am.samples, sample)
	// Cap sample history
	if len(am.samples) > g.config.BaselineWindow*3 {
		am.samples = am.samples[len(am.samples)-g.config.BaselineWindow*3:]
	}

	// Compute baseline if we have enough samples
	if len(am.samples) >= g.config.BaselineWindow && am.baseline == nil {
		am.baseline = computeBaseline(sample.AgentID, sample.Metric, am.samples[:g.config.BaselineWindow])
	}

	if am.baseline == nil {
		return nil
	}

	// CUSUM change-point detection
	regression := g.detectCUSUM(am, sample)
	if regression != nil {
		g.regressions = append(g.regressions, *regression)
		if len(g.regressions) > g.config.MaxRegressions {
			g.regressions = g.regressions[len(g.regressions)-g.config.MaxRegressions:]
		}

		// Auto-throttle if enabled
		if g.config.AutoThrottle && regression.Severity == "critical" {
			g.throttleAgent(sample.AgentID, regression)
			regression.AutoThrottled = true
		}
	}

	return regression
}

// detectCUSUM implements the CUSUM algorithm for change-point detection.
func (g *Guard) detectCUSUM(am *agentMetrics, sample Sample) *Regression {
	if am.baseline == nil {
		return nil
	}

	// Use a minimum stddev to avoid division by zero with constant baselines
	stdDev := am.baseline.StdDev
	if stdDev < 0.001 {
		stdDev = am.baseline.Mean * 0.01 // 1% of mean as minimum
		if stdDev < 0.001 {
			stdDev = 0.001
		}
	}

	// Normalize the sample against baseline
	z := (sample.Value - am.baseline.Mean) / stdDev

	// Update CUSUM statistics
	am.cusumPos = math.Max(0, am.cusumPos+z-g.config.CUSUMSlack)
	am.cusumNeg = math.Max(0, am.cusumNeg-z-g.config.CUSUMSlack)

	cusumVal := math.Max(am.cusumPos, am.cusumNeg)

	if cusumVal < g.config.CUSUMThreshold {
		return nil
	}

	// CUSUM alarm triggered - compute regression details
	recentMean := computeRecentMean(am.samples, g.config.DetectionWindow)
	changePercent := ((recentMean - am.baseline.Mean) / am.baseline.Mean) * 100

	severity := "warning"
	if math.Abs(changePercent) >= g.config.CriticalThreshold {
		severity = "critical"
	} else if math.Abs(changePercent) < g.config.WarningThreshold {
		// Reset CUSUM if change is below warning threshold
		am.cusumPos = 0
		am.cusumNeg = 0
		return nil
	}

	g.nextID++
	reg := &Regression{
		ID:            fmt.Sprintf("reg_%d", g.nextID),
		AgentID:       sample.AgentID,
		Metric:        sample.Metric,
		Severity:      severity,
		CurrentMean:   recentMean,
		BaselineMean:  am.baseline.Mean,
		ChangePercent: changePercent,
		CUSUMValue:    cusumVal,
		DetectedAt:    time.Now(),
		Message:       fmt.Sprintf("Performance regression: %s changed %.1f%% for agent %s", sample.Metric, changePercent, sample.AgentID),
	}

	// Reset CUSUM after alarm
	am.cusumPos = 0
	am.cusumNeg = 0

	return reg
}

func (g *Guard) throttleAgent(agentID string, reg *Regression) {
	duration := 5 * time.Minute
	if d, err := time.ParseDuration(g.config.ThrottleDuration); err == nil {
		duration = d
	}

	g.throttles[agentID] = &ThrottleStatus{
		AgentID:       agentID,
		Throttled:     true,
		Reason:        reg.Message,
		ThrottledAt:   time.Now(),
		ExpiresAt:     time.Now().Add(duration),
		ThrottledRate: 10, // reduced rate
	}

	g.logger.Warn("agent auto-throttled", "agent_id", agentID, "reason", reg.Message, "duration", duration)
}

// IsThrottled checks if an agent is currently throttled.
func (g *Guard) IsThrottled(agentID string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ts, ok := g.throttles[agentID]
	if !ok {
		return false
	}
	if time.Now().After(ts.ExpiresAt) {
		return false
	}
	return ts.Throttled
}

// GetBaseline returns the baseline for an agent/metric pair.
func (g *Guard) GetBaseline(agentID string, metric MetricName) *Baseline {
	g.mu.RLock()
	defer g.mu.RUnlock()
	key := fmt.Sprintf("%s:%s", agentID, metric)
	if am, ok := g.metrics[key]; ok {
		return am.baseline
	}
	return nil
}

// ListRegressions returns recent regressions.
func (g *Guard) ListRegressions(agentID string, limit int) []Regression {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if limit <= 0 {
		limit = 50
	}
	var result []Regression
	for i := len(g.regressions) - 1; i >= 0 && len(result) < limit; i-- {
		if agentID == "" || g.regressions[i].AgentID == agentID {
			result = append(result, g.regressions[i])
		}
	}
	return result
}

// GetThrottleStatus returns throttle status for an agent.
func (g *Guard) GetThrottleStatus(agentID string) *ThrottleStatus {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if ts, ok := g.throttles[agentID]; ok {
		return ts
	}
	return nil
}

// UnthrottleAgent removes throttling for an agent.
func (g *Guard) UnthrottleAgent(agentID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.throttles[agentID]; !ok {
		return fmt.Errorf("agent %q is not throttled", agentID)
	}
	delete(g.throttles, agentID)
	return nil
}

// ResetBaseline forces a baseline reset for an agent/metric.
func (g *Guard) ResetBaseline(agentID string, metric MetricName) {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := fmt.Sprintf("%s:%s", agentID, metric)
	if am, ok := g.metrics[key]; ok {
		am.baseline = nil
		am.cusumPos = 0
		am.cusumNeg = 0
	}
}

// Stats returns guard statistics.
func (g *Guard) Stats() map[string]any {
	g.mu.RLock()
	defer g.mu.RUnlock()
	throttled := 0
	for _, ts := range g.throttles {
		if ts.Throttled && time.Now().Before(ts.ExpiresAt) {
			throttled++
		}
	}
	return map[string]any{
		"tracked_metrics":   len(g.metrics),
		"total_regressions": len(g.regressions),
		"active_throttles":  throttled,
		"config":            g.config,
	}
}

// Helper functions

func computeBaseline(agentID string, metric MetricName, samples []Sample) *Baseline {
	if len(samples) == 0 {
		return nil
	}
	values := make([]float64, len(samples))
	for i, s := range samples {
		values[i] = s.Value
	}
	sort.Float64s(values)

	mean := computeMean(values)
	stdDev := computeStdDev(values, mean)

	return &Baseline{
		AgentID:     agentID,
		Metric:      metric,
		Mean:        mean,
		StdDev:      stdDev,
		P50:         percentile(values, 50),
		P95:         percentile(values, 95),
		P99:         percentile(values, 99),
		SampleCount: len(samples),
		UpdatedAt:   time.Now(),
	}
}

func computeMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func computeStdDev(values []float64, mean float64) float64 {
	if len(values) < 2 {
		return 0
	}
	sumSq := 0.0
	for _, v := range values {
		d := v - mean
		sumSq += d * d
	}
	return math.Sqrt(sumSq / float64(len(values)-1))
}

func percentile(sortedValues []float64, p float64) float64 {
	if len(sortedValues) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100.0*float64(len(sortedValues)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sortedValues) {
		idx = len(sortedValues) - 1
	}
	return sortedValues[idx]
}

func computeRecentMean(samples []Sample, window int) float64 {
	if len(samples) == 0 {
		return 0
	}
	start := len(samples) - window
	if start < 0 {
		start = 0
	}
	sum := 0.0
	count := 0
	for i := start; i < len(samples); i++ {
		sum += samples[i].Value
		count++
	}
	if count == 0 {
		return 0
	}
	return sum / float64(count)
}
