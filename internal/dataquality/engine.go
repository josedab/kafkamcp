// Package dataquality provides real-time data quality scoring and SLA monitoring
// for Kafka topics, evaluating freshness, completeness, schema conformance,
// anomaly rates, and volume consistency.
package dataquality

import (
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"
)

// Dimension represents a quality dimension.
type Dimension string

const (
	DimensionFreshness    Dimension = "freshness"
	DimensionCompleteness Dimension = "completeness"
	DimensionConformance  Dimension = "conformance"
	DimensionAnomaly      Dimension = "anomaly"
	DimensionVolume       Dimension = "volume"
)

// DimensionScore represents the score for a single quality dimension.
type DimensionScore struct {
	Dimension Dimension `json:"dimension"`
	Score     float64   `json:"score"`
	Details   string    `json:"details,omitempty"`
	SampleSize int     `json:"sample_size"`
}

// QualityScore is the aggregate quality score for a topic.
type QualityScore struct {
	Topic          string           `json:"topic"`
	OverallScore   float64          `json:"overall_score"`
	Grade          string           `json:"grade"`
	Dimensions     []DimensionScore `json:"dimensions"`
	SampleSize     int              `json:"sample_size"`
	EvaluatedAt    time.Time        `json:"evaluated_at"`
}

// SLAConfig defines quality thresholds for a topic.
type SLAConfig struct {
	Topic               string        `json:"topic" yaml:"topic"`
	MinFreshness        float64       `json:"min_freshness" yaml:"min_freshness"`
	MinCompleteness     float64       `json:"min_completeness" yaml:"min_completeness"`
	MinConformance      float64       `json:"min_conformance" yaml:"min_conformance"`
	MaxAnomalyRate      float64       `json:"max_anomaly_rate" yaml:"max_anomaly_rate"`
	MinVolumeConsistency float64      `json:"min_volume_consistency" yaml:"min_volume_consistency"`
	MinOverallScore     float64       `json:"min_overall_score" yaml:"min_overall_score"`
	MaxLatencyMs        int64         `json:"max_latency_ms" yaml:"max_latency_ms"`
	CheckInterval       time.Duration `json:"check_interval" yaml:"check_interval"`
	AlertWebhook        string        `json:"alert_webhook,omitempty" yaml:"alert_webhook,omitempty"`
}

// SLAStatus represents the current SLA compliance status.
type SLAStatus struct {
	Topic        string     `json:"topic"`
	Compliant    bool       `json:"compliant"`
	Breaches     []SLABreach `json:"breaches,omitempty"`
	LastChecked  time.Time  `json:"last_checked"`
	Score        float64    `json:"score"`
}

// SLABreach records a specific SLA violation.
type SLABreach struct {
	Dimension  Dimension `json:"dimension"`
	Threshold  float64   `json:"threshold"`
	Actual     float64   `json:"actual"`
	BreachedAt time.Time `json:"breached_at"`
}

// QualityReport is a detailed quality analysis of a topic.
type QualityReport struct {
	Topic           string                `json:"topic"`
	Score           QualityScore          `json:"score"`
	SLAStatus       *SLAStatus            `json:"sla_status,omitempty"`
	Trends          map[Dimension][]float64 `json:"trends,omitempty"`
	Recommendations []string              `json:"recommendations,omitempty"`
	GeneratedAt     time.Time             `json:"generated_at"`
}

// TopicMetrics holds raw metrics used for quality scoring.
type TopicMetrics struct {
	Topic              string    `json:"topic"`
	MessageCount       int64     `json:"message_count"`
	NullFieldRate      float64   `json:"null_field_rate"`
	SchemaErrorRate    float64   `json:"schema_error_rate"`
	AnomalyRate        float64   `json:"anomaly_rate"`
	LastMessageTime    time.Time `json:"last_message_time"`
	AvgMessageRate     float64   `json:"avg_message_rate"`
	CurrentMessageRate float64   `json:"current_message_rate"`
	SampleSize         int       `json:"sample_size"`
}

// Engine is the data quality scoring engine.
type Engine struct {
	mu          sync.RWMutex
	scores      map[string]*QualityScore
	slaConfigs  map[string]*SLAConfig
	slaStatuses map[string]*SLAStatus
	history     map[string][]QualityScore
	logger      *slog.Logger
	maxHistory  int
}

// NewEngine creates a new data quality engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		scores:      make(map[string]*QualityScore),
		slaConfigs:  make(map[string]*SLAConfig),
		slaStatuses: make(map[string]*SLAStatus),
		history:     make(map[string][]QualityScore),
		logger:      logger,
		maxHistory:  100,
	}
}

// Evaluate computes the quality score for a topic given its metrics.
func (e *Engine) Evaluate(metrics TopicMetrics) *QualityScore {
	dimensions := []DimensionScore{
		e.scoreFreshness(metrics),
		e.scoreCompleteness(metrics),
		e.scoreConformance(metrics),
		e.scoreAnomaly(metrics),
		e.scoreVolume(metrics),
	}

	total := 0.0
	for _, d := range dimensions {
		total += d.Score
	}
	overall := total / float64(len(dimensions))

	score := &QualityScore{
		Topic:        metrics.Topic,
		OverallScore: math.Round(overall*100) / 100,
		Grade:        grade(overall),
		Dimensions:   dimensions,
		SampleSize:   metrics.SampleSize,
		EvaluatedAt:  time.Now(),
	}

	e.mu.Lock()
	e.scores[metrics.Topic] = score
	hist := e.history[metrics.Topic]
	hist = append(hist, *score)
	if len(hist) > e.maxHistory {
		hist = hist[1:]
	}
	e.history[metrics.Topic] = hist

	// Check SLA
	if cfg, ok := e.slaConfigs[metrics.Topic]; ok {
		e.checkSLA(score, cfg)
	}
	e.mu.Unlock()

	return score
}

// GetScore returns the latest quality score for a topic.
func (e *Engine) GetScore(topic string) (*QualityScore, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s, ok := e.scores[topic]
	return s, ok
}

// GetReport generates a detailed quality report for a topic.
func (e *Engine) GetReport(topic string) (*QualityReport, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	score, ok := e.scores[topic]
	if !ok {
		return nil, fmt.Errorf("no quality data for topic %q", topic)
	}

	report := &QualityReport{
		Topic:       topic,
		Score:       *score,
		Trends:      e.buildTrends(topic),
		GeneratedAt: time.Now(),
	}

	if status, ok := e.slaStatuses[topic]; ok {
		report.SLAStatus = status
	}

	report.Recommendations = e.generateRecommendations(score)
	return report, nil
}

// SetSLA configures an SLA for a topic.
func (e *Engine) SetSLA(config SLAConfig) error {
	if config.Topic == "" {
		return fmt.Errorf("topic is required")
	}
	if config.MinOverallScore == 0 {
		config.MinOverallScore = 0.7
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.slaConfigs[config.Topic] = &config
	return nil
}

// GetSLAStatus returns the current SLA status for a topic.
func (e *Engine) GetSLAStatus(topic string) (*SLAStatus, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s, ok := e.slaStatuses[topic]
	return s, ok
}

// ListSLAs returns all configured SLAs.
func (e *Engine) ListSLAs() []SLAConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]SLAConfig, 0, len(e.slaConfigs))
	for _, cfg := range e.slaConfigs {
		result = append(result, *cfg)
	}
	return result
}

func (e *Engine) scoreFreshness(m TopicMetrics) DimensionScore {
	score := 1.0
	if !m.LastMessageTime.IsZero() {
		age := time.Since(m.LastMessageTime)
		switch {
		case age < 1*time.Minute:
			score = 1.0
		case age < 5*time.Minute:
			score = 0.9
		case age < 30*time.Minute:
			score = 0.7
		case age < 1*time.Hour:
			score = 0.5
		case age < 24*time.Hour:
			score = 0.3
		default:
			score = 0.1
		}
	}
	return DimensionScore{Dimension: DimensionFreshness, Score: score, SampleSize: m.SampleSize}
}

func (e *Engine) scoreCompleteness(m TopicMetrics) DimensionScore {
	score := 1.0 - m.NullFieldRate
	if score < 0 {
		score = 0
	}
	return DimensionScore{
		Dimension:  DimensionCompleteness,
		Score:      math.Round(score*100) / 100,
		Details:    fmt.Sprintf("null field rate: %.2f%%", m.NullFieldRate*100),
		SampleSize: m.SampleSize,
	}
}

func (e *Engine) scoreConformance(m TopicMetrics) DimensionScore {
	score := 1.0 - m.SchemaErrorRate
	if score < 0 {
		score = 0
	}
	return DimensionScore{
		Dimension:  DimensionConformance,
		Score:      math.Round(score*100) / 100,
		Details:    fmt.Sprintf("schema error rate: %.2f%%", m.SchemaErrorRate*100),
		SampleSize: m.SampleSize,
	}
}

func (e *Engine) scoreAnomaly(m TopicMetrics) DimensionScore {
	score := 1.0 - m.AnomalyRate
	if score < 0 {
		score = 0
	}
	return DimensionScore{
		Dimension:  DimensionAnomaly,
		Score:      math.Round(score*100) / 100,
		SampleSize: m.SampleSize,
	}
}

func (e *Engine) scoreVolume(m TopicMetrics) DimensionScore {
	score := 1.0
	if m.AvgMessageRate > 0 && m.CurrentMessageRate > 0 {
		ratio := m.CurrentMessageRate / m.AvgMessageRate
		deviation := math.Abs(1.0 - ratio)
		if deviation > 0.5 {
			score = math.Max(0, 1.0-deviation)
		}
	}
	return DimensionScore{
		Dimension:  DimensionVolume,
		Score:      math.Round(score*100) / 100,
		SampleSize: m.SampleSize,
	}
}

func (e *Engine) checkSLA(score *QualityScore, cfg *SLAConfig) {
	status := &SLAStatus{
		Topic:       score.Topic,
		Compliant:   true,
		LastChecked: time.Now(),
		Score:       score.OverallScore,
	}

	for _, dim := range score.Dimensions {
		var threshold float64
		switch dim.Dimension {
		case DimensionFreshness:
			threshold = cfg.MinFreshness
		case DimensionCompleteness:
			threshold = cfg.MinCompleteness
		case DimensionConformance:
			threshold = cfg.MinConformance
		case DimensionVolume:
			threshold = cfg.MinVolumeConsistency
		case DimensionAnomaly:
			if cfg.MaxAnomalyRate > 0 {
				threshold = 1.0 - cfg.MaxAnomalyRate
			}
		}
		if threshold > 0 && dim.Score < threshold {
			status.Compliant = false
			status.Breaches = append(status.Breaches, SLABreach{
				Dimension:  dim.Dimension,
				Threshold:  threshold,
				Actual:     dim.Score,
				BreachedAt: time.Now(),
			})
		}
	}

	if cfg.MinOverallScore > 0 && score.OverallScore < cfg.MinOverallScore {
		status.Compliant = false
	}

	e.slaStatuses[score.Topic] = status
}

func (e *Engine) buildTrends(topic string) map[Dimension][]float64 {
	trends := make(map[Dimension][]float64)
	hist := e.history[topic]
	for _, s := range hist {
		for _, d := range s.Dimensions {
			trends[d.Dimension] = append(trends[d.Dimension], d.Score)
		}
	}
	return trends
}

func (e *Engine) generateRecommendations(score *QualityScore) []string {
	var recs []string
	for _, d := range score.Dimensions {
		if d.Score < 0.5 {
			switch d.Dimension {
			case DimensionFreshness:
				recs = append(recs, "Topic data is stale - check producer health and connectivity")
			case DimensionCompleteness:
				recs = append(recs, "High null field rate - review producer serialization and data pipeline")
			case DimensionConformance:
				recs = append(recs, "Schema conformance issues - verify producers use correct schema version")
			case DimensionAnomaly:
				recs = append(recs, "High anomaly rate - investigate data patterns and outlier sources")
			case DimensionVolume:
				recs = append(recs, "Volume inconsistency - check for dropped messages or traffic spikes")
			}
		}
	}
	return recs
}

func grade(score float64) string {
	switch {
	case score >= 0.95:
		return "A+"
	case score >= 0.90:
		return "A"
	case score >= 0.85:
		return "B+"
	case score >= 0.80:
		return "B"
	case score >= 0.70:
		return "C"
	case score >= 0.60:
		return "D"
	default:
		return "F"
	}
}
