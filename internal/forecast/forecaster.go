package forecast

import (
	"fmt"
	"math"
	"sync"
	"time"
)

const maxMetricsPerKey = 1000

// Metric represents a single data point for a topic metric.
type Metric struct {
	Topic      string
	MetricName string
	Value      float64
	Timestamp  time.Time
}

// PredictedPoint represents a single forecasted value with confidence bounds.
type PredictedPoint struct {
	Timestamp      string  `json:"timestamp"`
	Value          float64 `json:"value"`
	ConfidenceLow  float64 `json:"confidence_low"`
	ConfidenceHigh float64 `json:"confidence_high"`
}

// Forecast contains the result of a topic metric forecast.
type Forecast struct {
	Topic           string           `json:"topic"`
	MetricName      string           `json:"metric_name"`
	CurrentValue    float64          `json:"current_value"`
	PredictedValues []PredictedPoint `json:"predicted_values"`
	Trend           string           `json:"trend"`
	GrowthRate      float64          `json:"growth_rate"`
	SeasonalPattern string           `json:"seasonal_pattern"`
	Recommendations []string         `json:"recommendations"`
	Timestamp       string           `json:"timestamp"`
}

// TopicCapacity describes current and predicted capacity for a topic.
type TopicCapacity struct {
	Topic               string  `json:"topic"`
	CurrentThroughput   float64 `json:"current_throughput"`
	PredictedThroughput float64 `json:"predicted_throughput"`
	CurrentStorage      float64 `json:"current_storage"`
	PredictedStorage    float64 `json:"predicted_storage"`
	DaysUntilLimit      int     `json:"days_until_limit"`
	NeedsScaling        bool    `json:"needs_scaling"`
}

// CostEstimate provides monthly cost projections.
type CostEstimate struct {
	CurrentMonthlyCost   float64 `json:"current_monthly_cost"`
	PredictedMonthlyCost float64 `json:"predicted_monthly_cost"`
	Currency             string  `json:"currency"`
}

// CapacityReport aggregates capacity information across topics.
type CapacityReport struct {
	Cluster                string          `json:"cluster"`
	Topics                 []TopicCapacity `json:"topics"`
	ScalingRecommendations []string        `json:"scaling_recommendations"`
	CostEstimate           *CostEstimate   `json:"cost_estimate,omitempty"`
	Timestamp              string          `json:"timestamp"`
}

// Forecaster stores metric history and produces forecasts.
type Forecaster struct {
	mu            sync.RWMutex
	metrics       map[string][]Metric
	retentionDays int
}

// NewForecaster creates a Forecaster with the given retention window.
func NewForecaster(retentionDays int) *Forecaster {
	if retentionDays <= 0 {
		retentionDays = 30
	}
	return &Forecaster{
		metrics:       make(map[string][]Metric),
		retentionDays: retentionDays,
	}
}

func metricKey(topic, metricName string) string {
	return topic + ":" + metricName
}

// RecordMetric stores a metric data point, maintaining a ring buffer of maxMetricsPerKey entries.
func (f *Forecaster) RecordMetric(m Metric) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := metricKey(m.Topic, m.MetricName)
	f.metrics[key] = append(f.metrics[key], m)

	if len(f.metrics[key]) > maxMetricsPerKey {
		f.metrics[key] = f.metrics[key][len(f.metrics[key])-maxMetricsPerKey:]
	}
}

// ForecastTopic produces a forecast for the given topic and metric over horizonDays using linear regression.
func (f *Forecaster) ForecastTopic(topic, metricName string, horizonDays int) (*Forecast, error) {
	f.mu.RLock()
	key := metricKey(topic, metricName)
	points := f.metrics[key]
	f.mu.RUnlock()

	if len(points) < 2 {
		return nil, fmt.Errorf("insufficient data for topic %q metric %q: need at least 2 points, have %d", topic, metricName, len(points))
	}

	slope, intercept := linearRegression(points)

	// Current value is the last recorded point.
	currentValue := points[len(points)-1].Value

	// Mean value for trend detection.
	var sum float64
	for _, p := range points {
		sum += p.Value
	}
	meanValue := sum / float64(len(points))

	trend := detectTrend(slope, meanValue)
	seasonalPattern := detectSeasonality(points)

	// Growth rate as percentage change per day relative to mean.
	var growthRate float64
	if meanValue != 0 {
		growthRate = (slope / meanValue) * 100
	}

	// Compute residual standard error for confidence intervals.
	baseTime := points[0].Timestamp
	var sse float64
	for _, p := range points {
		x := p.Timestamp.Sub(baseTime).Hours() / 24.0
		predicted := slope*x + intercept
		diff := p.Value - predicted
		sse += diff * diff
	}
	residualSE := math.Sqrt(sse / float64(len(points)))

	// Generate predicted points.
	lastTime := points[len(points)-1].Timestamp
	predicted := make([]PredictedPoint, 0, horizonDays)
	for d := 1; d <= horizonDays; d++ {
		futureTime := lastTime.AddDate(0, 0, d)
		x := futureTime.Sub(baseTime).Hours() / 24.0
		val := slope*x + intercept
		margin := 1.96 * residualSE * math.Sqrt(1+1/float64(len(points)))
		predicted = append(predicted, PredictedPoint{
			Timestamp:      futureTime.UTC().Format(time.RFC3339),
			Value:          val,
			ConfidenceLow:  val - margin,
			ConfidenceHigh: val + margin,
		})
	}

	recommendations := generateForecastRecommendations(topic, trend, growthRate)

	return &Forecast{
		Topic:           topic,
		MetricName:      metricName,
		CurrentValue:    currentValue,
		PredictedValues: predicted,
		Trend:           trend,
		GrowthRate:      growthRate,
		SeasonalPattern: seasonalPattern,
		Recommendations: recommendations,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	}, nil
}

// linearRegression computes slope and intercept using ordinary least squares.
// The x-axis is days since the first point's timestamp.
func linearRegression(points []Metric) (slope, intercept float64) {
	n := float64(len(points))
	if n == 0 {
		return 0, 0
	}

	baseTime := points[0].Timestamp

	var sumX, sumY, sumXY, sumX2 float64
	for _, p := range points {
		x := p.Timestamp.Sub(baseTime).Hours() / 24.0
		y := p.Value
		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
	}

	denom := n*sumX2 - sumX*sumX
	if denom == 0 {
		return 0, sumY / n
	}

	slope = (n*sumXY - sumX*sumY) / denom
	intercept = (sumY - slope*sumX) / n
	return slope, intercept
}

// detectTrend classifies the trend based on slope relative to the mean value.
func detectTrend(slope, meanValue float64) string {
	if meanValue == 0 {
		if slope > 0 {
			return "growing"
		}
		if slope < 0 {
			return "shrinking"
		}
		return "stable"
	}
	ratio := math.Abs(slope) / math.Abs(meanValue)
	switch {
	case ratio < 0.01:
		return "stable"
	case slope > 0:
		return "growing"
	default:
		return "shrinking"
	}
}

// detectSeasonality performs basic periodicity detection for daily or weekly patterns.
func detectSeasonality(points []Metric) string {
	if len(points) < 14 {
		return "none"
	}

	// Compute mean.
	var sum float64
	for _, p := range points {
		sum += p.Value
	}
	mean := sum / float64(len(points))

	// Compute autocorrelation at lag 1 (daily) and lag 7 (weekly) using index-based lags.
	dailyCorr := autocorrelation(points, mean, 1)
	weeklyCorr := autocorrelation(points, mean, 7)

	const threshold = 0.3
	hasDaily := dailyCorr > threshold
	hasWeekly := weeklyCorr > threshold

	switch {
	case hasWeekly && hasDaily:
		return "weekly"
	case hasWeekly:
		return "weekly"
	case hasDaily:
		return "daily"
	default:
		return "none"
	}
}

func autocorrelation(points []Metric, mean float64, lag int) float64 {
	if lag >= len(points) {
		return 0
	}

	var num, denom float64
	for i := 0; i < len(points); i++ {
		denom += (points[i].Value - mean) * (points[i].Value - mean)
	}
	if denom == 0 {
		return 0
	}
	for i := lag; i < len(points); i++ {
		num += (points[i].Value - mean) * (points[i-lag].Value - mean)
	}
	return num / denom
}

// generateForecastRecommendations produces actionable recommendations.
func generateForecastRecommendations(topic, trend string, growthRate float64) []string {
	var recs []string
	switch trend {
	case "growing":
		recs = append(recs, fmt.Sprintf("Topic %q is growing at %.1f%%/day — consider adding partitions", topic, growthRate))
		if growthRate > 5 {
			recs = append(recs, fmt.Sprintf("Topic %q has rapid growth — evaluate consumer scaling", topic))
		}
	case "shrinking":
		recs = append(recs, fmt.Sprintf("Topic %q is shrinking — review producer health", topic))
	case "stable":
		recs = append(recs, fmt.Sprintf("Topic %q is stable — no immediate action needed", topic))
	}
	return recs
}

// GetMetricHistory returns the stored metrics for a topic and metric name.
func (f *Forecaster) GetMetricHistory(topic, metricName string) []Metric {
	f.mu.RLock()
	defer f.mu.RUnlock()

	key := metricKey(topic, metricName)
	src := f.metrics[key]
	out := make([]Metric, len(src))
	copy(out, src)
	return out
}
