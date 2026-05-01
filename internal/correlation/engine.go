// Package correlation provides a unified observability correlation engine that
// links Kafka metrics with external signals for holistic root cause analysis.
package correlation

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// SignalSource identifies the origin of an external signal.
type SignalSource string

const (
	SourcePrometheus SignalSource = "prometheus"
	SourceDatadog    SignalSource = "datadog"
	SourcePagerDuty  SignalSource = "pagerduty"
	SourceKafka      SignalSource = "kafka"
	SourceCustom     SignalSource = "custom"
)

// Signal represents an external observability signal.
type Signal struct {
	ID        string            `json:"id"`
	Source    SignalSource      `json:"source"`
	Metric   string            `json:"metric"`
	Value    float64           `json:"value"`
	Labels   map[string]string `json:"labels,omitempty"`
	Timestamp time.Time        `json:"timestamp"`
	Severity string            `json:"severity,omitempty"`
	Message  string            `json:"message,omitempty"`
}

// CorrelationResult describes correlations found between signals.
type CorrelationResult struct {
	ID              string          `json:"id"`
	TimeWindow      string          `json:"time_window"`
	SignalCount     int             `json:"signal_count"`
	Correlations    []Correlation   `json:"correlations"`
	RootCauses      []RootCause     `json:"root_causes"`
	Timeline        []TimelineEvent `json:"timeline"`
	GeneratedAt     time.Time       `json:"generated_at"`
}

// Correlation links two signals with a strength score.
type Correlation struct {
	SignalA     string  `json:"signal_a"`
	SignalB     string  `json:"signal_b"`
	Strength    float64 `json:"strength"`
	Lag         string  `json:"lag"`
	Direction   string  `json:"direction"`
	Description string  `json:"description"`
}

// RootCause is a ranked potential root cause.
type RootCause struct {
	Rank        int     `json:"rank"`
	Source      string  `json:"source"`
	Metric      string  `json:"metric"`
	Score       float64 `json:"score"`
	Evidence    string  `json:"evidence"`
	Confidence  string  `json:"confidence"`
}

// TimelineEvent represents a signal on a unified timeline.
type TimelineEvent struct {
	Timestamp time.Time    `json:"timestamp"`
	Source    SignalSource `json:"source"`
	Metric   string       `json:"metric"`
	Value    float64      `json:"value"`
	Severity string       `json:"severity,omitempty"`
}

// ExternalEndpoint configures an external data source.
type ExternalEndpoint struct {
	Source  SignalSource `json:"source"`
	URL     string       `json:"url"`
	APIKey  string       `json:"api_key,omitempty"`
	Active  bool         `json:"active"`
}

// Engine manages the observability correlation engine.
type Engine struct {
	mu        sync.RWMutex
	signals   []Signal
	endpoints map[string]ExternalEndpoint
	maxSignals int
	logger    *slog.Logger
}

// NewEngine creates a new correlation engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		signals:    make([]Signal, 0, 10000),
		endpoints:  make(map[string]ExternalEndpoint),
		maxSignals: 10000,
		logger:     logger,
	}
}

// IngestSignal records an external observability signal.
func (e *Engine) IngestSignal(signal Signal) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if signal.ID == "" {
		signal.ID = randid.New("sig")
	}
	if signal.Timestamp.IsZero() {
		signal.Timestamp = time.Now()
	}

	if len(e.signals) >= e.maxSignals {
		e.signals = e.signals[1:]
	}
	e.signals = append(e.signals, signal)
}

// RegisterEndpoint configures an external data source.
func (e *Engine) RegisterEndpoint(endpoint ExternalEndpoint) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.endpoints[string(endpoint.Source)] = endpoint
}

// ListEndpoints returns configured external endpoints.
func (e *Engine) ListEndpoints() []ExternalEndpoint {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]ExternalEndpoint, 0, len(e.endpoints))
	for _, ep := range e.endpoints {
		safe := ep
		safe.APIKey = "***" // redact
		result = append(result, safe)
	}
	return result
}

// Correlate analyzes signals within a time window for correlations.
func (e *Engine) Correlate(windowDuration time.Duration, metricFilter string) *CorrelationResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	since := time.Now().Add(-windowDuration)

	// Filter signals
	var filtered []Signal
	for _, s := range e.signals {
		if !s.Timestamp.Before(since) {
			if metricFilter == "" || s.Metric == metricFilter {
				filtered = append(filtered, s)
			}
		}
	}

	result := &CorrelationResult{
		ID:          randid.New("corr"),
		TimeWindow:  windowDuration.String(),
		SignalCount: len(filtered),
		GeneratedAt: time.Now(),
	}

	// Build timeline
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].Timestamp.Before(filtered[j].Timestamp)
	})
	for _, s := range filtered {
		result.Timeline = append(result.Timeline, TimelineEvent{
			Timestamp: s.Timestamp,
			Source:    s.Source,
			Metric:   s.Metric,
			Value:    s.Value,
			Severity: s.Severity,
		})
	}

	// Find temporal correlations
	result.Correlations = e.findTemporalCorrelations(filtered)

	// Rank root causes
	result.RootCauses = e.rankRootCauses(filtered, result.Correlations)

	return result
}

// QuerySignals returns signals matching criteria.
func (e *Engine) QuerySignals(source SignalSource, metric string, since time.Time, limit int) []Signal {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var result []Signal
	for _, s := range e.signals {
		if !s.Timestamp.Before(since) {
			if source == "" || s.Source == source {
				if metric == "" || s.Metric == metric {
					result = append(result, s)
				}
			}
		}
	}

	if limit > 0 && len(result) > limit {
		result = result[len(result)-limit:]
	}
	return result
}

func (e *Engine) findTemporalCorrelations(signals []Signal) []Correlation {
	// Group signals by metric
	byMetric := make(map[string][]Signal)
	for _, s := range signals {
		key := fmt.Sprintf("%s:%s", s.Source, s.Metric)
		byMetric[key] = append(byMetric[key], s)
	}

	var correlations []Correlation
	keys := make([]string, 0, len(byMetric))
	for k := range byMetric {
		keys = append(keys, k)
	}

	// Compare each pair of metrics
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			a := byMetric[keys[i]]
			b := byMetric[keys[j]]

			strength, lag := computeTemporalCorrelation(a, b)
			if math.Abs(strength) > 0.3 {
				direction := "positive"
				if strength < 0 {
					direction = "negative"
				}

				correlations = append(correlations, Correlation{
					SignalA:     keys[i],
					SignalB:     keys[j],
					Strength:    strength,
					Lag:         lag.String(),
					Direction:   direction,
					Description: fmt.Sprintf("%s and %s show %s correlation (%.2f) with %s lag", keys[i], keys[j], direction, strength, lag),
				})
			}
		}
	}

	// Sort by absolute strength
	sort.Slice(correlations, func(i, j int) bool {
		return math.Abs(correlations[i].Strength) > math.Abs(correlations[j].Strength)
	})

	return correlations
}

func (e *Engine) rankRootCauses(signals []Signal, correlations []Correlation) []RootCause {
	// Score each signal source by: earliness + severity + correlation strength
	scores := make(map[string]float64)
	evidence := make(map[string]string)

	if len(signals) == 0 {
		return nil
	}

	earliest := signals[0].Timestamp
	latest := signals[len(signals)-1].Timestamp
	timeRange := latest.Sub(earliest)
	if timeRange == 0 {
		timeRange = time.Second
	}

	for _, s := range signals {
		key := fmt.Sprintf("%s:%s", s.Source, s.Metric)

		// Earliness score: earlier signals are more likely root causes
		earlScore := 1.0 - float64(s.Timestamp.Sub(earliest))/float64(timeRange)
		scores[key] += earlScore * 0.4

		// Severity score
		switch s.Severity {
		case "critical":
			scores[key] += 0.3
		case "warning":
			scores[key] += 0.2
		case "info":
			scores[key] += 0.1
		}

		evidence[key] = fmt.Sprintf("First seen at %s with value %.2f", s.Timestamp.Format(time.RFC3339), s.Value)
	}

	// Correlation score: signals correlated with many others are more likely root causes
	for _, c := range correlations {
		scores[c.SignalA] += math.Abs(c.Strength) * 0.3
		scores[c.SignalB] += math.Abs(c.Strength) * 0.2
	}

	// Sort by score
	type kv struct {
		Key   string
		Score float64
	}
	var sorted []kv
	for k, v := range scores {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Score > sorted[j].Score })

	var rootCauses []RootCause
	for i, s := range sorted {
		if i >= 5 {
			break
		}
		confidence := "low"
		if s.Score > 0.7 {
			confidence = "high"
		} else if s.Score > 0.4 {
			confidence = "medium"
		}

		rootCauses = append(rootCauses, RootCause{
			Rank:       i + 1,
			Source:     s.Key,
			Metric:     s.Key,
			Score:      s.Score,
			Evidence:   evidence[s.Key],
			Confidence: confidence,
		})
	}

	return rootCauses
}

func computeTemporalCorrelation(a, b []Signal) (float64, time.Duration) {
	if len(a) < 2 || len(b) < 2 {
		return 0, 0
	}

	// Simple Pearson correlation on values with time alignment
	// Bucket into minute-level windows
	bucketA := make(map[int64]float64)
	bucketB := make(map[int64]float64)

	for _, s := range a {
		minute := s.Timestamp.Unix() / 60
		bucketA[minute] = s.Value
	}
	for _, s := range b {
		minute := s.Timestamp.Unix() / 60
		bucketB[minute] = s.Value
	}

	// Find common time buckets
	var valA, valB []float64
	for t, va := range bucketA {
		if vb, ok := bucketB[t]; ok {
			valA = append(valA, va)
			valB = append(valB, vb)
		}
	}

	if len(valA) < 2 {
		return 0, 0
	}

	// Pearson correlation
	n := float64(len(valA))
	var sumA, sumB, sumAB, sumA2, sumB2 float64
	for i := range valA {
		sumA += valA[i]
		sumB += valB[i]
		sumAB += valA[i] * valB[i]
		sumA2 += valA[i] * valA[i]
		sumB2 += valB[i] * valB[i]
	}

	denom := math.Sqrt((n*sumA2 - sumA*sumA) * (n*sumB2 - sumB*sumB))
	if denom == 0 {
		return 0, 0
	}

	r := (n*sumAB - sumA*sumB) / denom

	// Estimate lag from earliest timestamps
	lag := time.Duration(0)
	if len(a) > 0 && len(b) > 0 {
		lag = b[0].Timestamp.Sub(a[0].Timestamp)
		if lag < 0 {
			lag = -lag
		}
	}

	return r, lag
}
