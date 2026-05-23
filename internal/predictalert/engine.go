package predictalert

import (
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"
)

// LagSample records consumer group lag at a point in time.
type LagSample struct {
	GroupID     string    `json:"group_id"`
	TotalLag    int64     `json:"total_lag"`
	MemberCount int       `json:"member_count"`
	Timestamp   time.Time `json:"timestamp"`
}

// Alert represents a predictive alert.
type Alert struct {
	ID           string    `json:"id"`
	GroupID      string    `json:"group_id"`
	Type         string    `json:"type"`     // lag_spike, rebalance_storm, stalled_consumer, uneven_load
	Severity     string    `json:"severity"` // info, warning, critical
	Message      string    `json:"message"`
	Prediction   string    `json:"prediction,omitempty"`
	Confidence   float64   `json:"confidence"`
	CreatedAt    time.Time `json:"created_at"`
	Acknowledged bool      `json:"acknowledged"`
}

// GroupAnalysis provides predictive analysis for a consumer group.
type GroupAnalysis struct {
	GroupID         string   `json:"group_id"`
	CurrentLag      int64    `json:"current_lag"`
	LagTrend        string   `json:"lag_trend"` // increasing, decreasing, stable, volatile
	LagGrowthRate   float64  `json:"lag_growth_rate_per_min"`
	PredictedLag1h  int64    `json:"predicted_lag_1h"`
	PredictedLag6h  int64    `json:"predicted_lag_6h"`
	RebalanceRisk   string   `json:"rebalance_risk"` // low, medium, high
	ConsumptionRate float64  `json:"consumption_rate_per_sec"`
	Alerts          []Alert  `json:"alerts"`
	Recommendations []string `json:"recommendations,omitempty"`
	SampleCount     int      `json:"sample_count"`
}

// Engine performs predictive analysis on consumer group metrics.
type Engine struct {
	mu      sync.RWMutex
	samples map[string][]LagSample // groupID -> time-series
	alerts  map[string]*Alert
	logger  *slog.Logger
	nextID  int64
}

// NewEngine creates a new predictive alerting engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		samples: make(map[string][]LagSample),
		alerts:  make(map[string]*Alert),
		logger:  logger,
	}
}

// RecordSample adds a lag measurement for a consumer group.
func (e *Engine) RecordSample(groupID string, totalLag int64, memberCount int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	sample := LagSample{
		GroupID:     groupID,
		TotalLag:    totalLag,
		MemberCount: memberCount,
		Timestamp:   time.Now(),
	}

	e.samples[groupID] = append(e.samples[groupID], sample)
	// Keep last 1000 samples per group
	if len(e.samples[groupID]) > 1000 {
		e.samples[groupID] = e.samples[groupID][len(e.samples[groupID])-1000:]
	}
}

// Analyze performs predictive analysis on a consumer group.
func (e *Engine) Analyze(groupID string) *GroupAnalysis {
	e.mu.Lock()
	defer e.mu.Unlock()

	samples := e.samples[groupID]
	analysis := &GroupAnalysis{
		GroupID:     groupID,
		SampleCount: len(samples),
	}

	if len(samples) == 0 {
		analysis.LagTrend = "unknown"
		analysis.RebalanceRisk = "unknown"
		return analysis
	}

	latest := samples[len(samples)-1]
	analysis.CurrentLag = latest.TotalLag

	if len(samples) < 2 {
		analysis.LagTrend = "stable"
		analysis.RebalanceRisk = "low"
		analysis.PredictedLag1h = latest.TotalLag
		analysis.PredictedLag6h = latest.TotalLag
		return analysis
	}

	// Compute lag growth rate using linear regression on recent samples
	recentSamples := samples
	if len(recentSamples) > 60 {
		recentSamples = recentSamples[len(recentSamples)-60:]
	}

	growthRate := e.computeGrowthRate(recentSamples)
	analysis.LagGrowthRate = growthRate

	// Determine trend
	switch {
	case growthRate > 100:
		analysis.LagTrend = "increasing"
	case growthRate < -100:
		analysis.LagTrend = "decreasing"
	case e.isVolatile(recentSamples):
		analysis.LagTrend = "volatile"
	default:
		analysis.LagTrend = "stable"
	}

	// Predict future lag
	analysis.PredictedLag1h = int64(math.Max(0, float64(latest.TotalLag)+growthRate*60))
	analysis.PredictedLag6h = int64(math.Max(0, float64(latest.TotalLag)+growthRate*360))

	// Consumption rate (messages cleared per second)
	if len(recentSamples) >= 2 {
		first := recentSamples[0]
		last := recentSamples[len(recentSamples)-1]
		elapsed := last.Timestamp.Sub(first.Timestamp).Seconds()
		if elapsed > 0 {
			lagDiff := float64(first.TotalLag - last.TotalLag)
			if lagDiff > 0 {
				analysis.ConsumptionRate = lagDiff / elapsed
			}
		}
	}

	// Rebalance risk assessment
	analysis.RebalanceRisk = e.assessRebalanceRisk(recentSamples)

	// Generate alerts
	analysis.Alerts = e.generateAlerts(analysis, recentSamples)

	// Generate recommendations
	analysis.Recommendations = e.generateRecommendations(analysis)

	return analysis
}

func (e *Engine) computeGrowthRate(samples []LagSample) float64 {
	if len(samples) < 2 {
		return 0
	}

	n := float64(len(samples))
	var sumX, sumY, sumXY, sumX2 float64
	baseTime := samples[0].Timestamp

	for i, s := range samples {
		x := s.Timestamp.Sub(baseTime).Minutes()
		y := float64(s.TotalLag)
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
		_ = i
	}

	denom := n*sumX2 - sumX*sumX
	if denom == 0 {
		return 0
	}
	return (n*sumXY - sumX*sumY) / denom
}

func (e *Engine) isVolatile(samples []LagSample) bool {
	if len(samples) < 5 {
		return false
	}

	var mean float64
	for _, s := range samples {
		mean += float64(s.TotalLag)
	}
	mean /= float64(len(samples))

	if mean == 0 {
		return false
	}

	var variance float64
	for _, s := range samples {
		diff := float64(s.TotalLag) - mean
		variance += diff * diff
	}
	variance /= float64(len(samples))

	cv := math.Sqrt(variance) / mean
	return cv > 0.5
}

func (e *Engine) assessRebalanceRisk(samples []LagSample) string {
	if len(samples) < 3 {
		return "low"
	}

	memberChanges := 0
	for i := 1; i < len(samples); i++ {
		if samples[i].MemberCount != samples[i-1].MemberCount {
			memberChanges++
		}
	}

	changeRate := float64(memberChanges) / float64(len(samples))
	switch {
	case changeRate > 0.3:
		return "high"
	case changeRate > 0.1:
		return "medium"
	default:
		return "low"
	}
}

func (e *Engine) generateAlerts(analysis *GroupAnalysis, samples []LagSample) []Alert {
	var alerts []Alert

	if analysis.LagTrend == "increasing" && analysis.PredictedLag1h > analysis.CurrentLag*2 {
		e.nextID++
		alerts = append(alerts, Alert{
			ID:         fmt.Sprintf("alert_%d", e.nextID),
			GroupID:    analysis.GroupID,
			Type:       "lag_spike",
			Severity:   "warning",
			Message:    fmt.Sprintf("Consumer lag is increasing at %.0f msgs/min. Predicted to reach %d in 1 hour.", analysis.LagGrowthRate, analysis.PredictedLag1h),
			Prediction: fmt.Sprintf("Lag will grow from %d to ~%d within 1 hour", analysis.CurrentLag, analysis.PredictedLag1h),
			Confidence: 0.75,
			CreatedAt:  time.Now(),
		})
	}

	if analysis.CurrentLag > 0 && analysis.ConsumptionRate == 0 && len(samples) > 5 {
		e.nextID++
		alerts = append(alerts, Alert{
			ID:         fmt.Sprintf("alert_%d", e.nextID),
			GroupID:    analysis.GroupID,
			Type:       "stalled_consumer",
			Severity:   "critical",
			Message:    fmt.Sprintf("Consumer group appears stalled with %d lag and zero consumption rate", analysis.CurrentLag),
			Confidence: 0.85,
			CreatedAt:  time.Now(),
		})
	}

	if analysis.RebalanceRisk == "high" {
		e.nextID++
		alerts = append(alerts, Alert{
			ID:         fmt.Sprintf("alert_%d", e.nextID),
			GroupID:    analysis.GroupID,
			Type:       "rebalance_storm",
			Severity:   "warning",
			Message:    "Frequent membership changes detected. Risk of rebalance storm.",
			Prediction: "Continued instability may cause processing delays",
			Confidence: 0.70,
			CreatedAt:  time.Now(),
		})
	}

	for _, a := range alerts {
		e.alerts[a.ID] = &a
	}

	return alerts
}

func (e *Engine) generateRecommendations(analysis *GroupAnalysis) []string {
	var recs []string

	switch analysis.LagTrend {
	case "increasing":
		recs = append(recs, "Consider scaling consumer instances to handle increased load")
	case "volatile":
		recs = append(recs, "Investigate source of lag volatility - check for batch processing patterns")
	}

	if analysis.RebalanceRisk == "high" {
		recs = append(recs, "Increase session.timeout.ms or max.poll.interval.ms to reduce rebalance frequency")
	}

	if analysis.CurrentLag > 100000 {
		recs = append(recs, "Consider increasing max.poll.records or adding more partitions for parallelism")
	}

	return recs
}

// ListAlerts returns recent alerts, optionally filtered by group.
func (e *Engine) ListAlerts(groupID string, limit int) []Alert {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var result []Alert
	for _, a := range e.alerts {
		if groupID == "" || a.GroupID == groupID {
			result = append(result, *a)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.After(result[j].CreatedAt) })
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}

// AcknowledgeAlert marks an alert as acknowledged.
func (e *Engine) AcknowledgeAlert(alertID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	a, ok := e.alerts[alertID]
	if !ok {
		return fmt.Errorf("alert %q not found", alertID)
	}
	a.Acknowledged = true
	return nil
}
