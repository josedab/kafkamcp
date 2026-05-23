package healthscore

import (
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"
)

// ComponentScore represents the health score for a single cluster component.
type ComponentScore struct {
	Name    string  `json:"name"`
	Score   float64 `json:"score"`
	Status  string  `json:"status"`
	Details string  `json:"details,omitempty"`
	Weight  float64 `json:"weight"`
}

// HealthReport is the unified health report for a cluster.
type HealthReport struct {
	Cluster         string           `json:"cluster"`
	OverallScore    float64          `json:"overall_score"`
	Status          string           `json:"status"`
	Components      []ComponentScore `json:"components"`
	Recommendations []string         `json:"recommendations,omitempty"`
	EvaluatedAt     time.Time        `json:"evaluated_at"`
	TrendDirection  string           `json:"trend_direction,omitempty"`
	PreviousScore   float64          `json:"previous_score,omitempty"`
}

// ClusterMetricsInput provides the raw metrics to compute health.
type ClusterMetricsInput struct {
	TotalBrokers              int     `json:"total_brokers"`
	OnlineBrokers             int     `json:"online_brokers"`
	UnderReplicatedPartitions int     `json:"under_replicated_partitions"`
	TotalPartitions           int     `json:"total_partitions"`
	ISRShrinkRate             float64 `json:"isr_shrink_rate"`
	TotalConsumerLag          int64   `json:"total_consumer_lag"`
	MaxConsumerLag            int64   `json:"max_consumer_lag"`
	ActiveControllerCount     int     `json:"active_controller_count"`
	OfflinePartitions         int     `json:"offline_partitions"`
}

// Engine computes and tracks cluster health scores.
type Engine struct {
	mu      sync.RWMutex
	history map[string][]HealthReport
	logger  *slog.Logger
}

// NewEngine creates a new health score engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		history: make(map[string][]HealthReport),
		logger:  logger,
	}
}

// Evaluate computes a unified health score from raw cluster metrics.
func (e *Engine) Evaluate(cluster string, input ClusterMetricsInput) *HealthReport {
	components := e.computeComponents(input)

	var totalWeight, weightedSum float64
	for _, c := range components {
		totalWeight += c.Weight
		weightedSum += c.Score * c.Weight
	}

	overall := 0.0
	if totalWeight > 0 {
		overall = math.Round(weightedSum/totalWeight*100) / 100
	}

	report := &HealthReport{
		Cluster:      cluster,
		OverallScore: overall,
		Status:       scoreToStatus(overall),
		Components:   components,
		EvaluatedAt:  time.Now(),
	}

	report.Recommendations = e.generateRecommendations(input, components)

	e.mu.Lock()
	defer e.mu.Unlock()

	history := e.history[cluster]
	if len(history) > 0 {
		prev := history[len(history)-1]
		report.PreviousScore = prev.OverallScore
		if overall > prev.OverallScore {
			report.TrendDirection = "improving"
		} else if overall < prev.OverallScore {
			report.TrendDirection = "degrading"
		} else {
			report.TrendDirection = "stable"
		}
	}

	e.history[cluster] = append(history, *report)
	if len(e.history[cluster]) > 100 {
		e.history[cluster] = e.history[cluster][len(e.history[cluster])-100:]
	}

	return report
}

func (e *Engine) computeComponents(input ClusterMetricsInput) []ComponentScore {
	var components []ComponentScore

	// Broker availability (weight: 3)
	brokerScore := 100.0
	brokerDetails := ""
	if input.TotalBrokers > 0 {
		brokerScore = float64(input.OnlineBrokers) / float64(input.TotalBrokers) * 100
		brokerDetails = fmt.Sprintf("%d/%d brokers online", input.OnlineBrokers, input.TotalBrokers)
	}
	components = append(components, ComponentScore{
		Name: "broker_availability", Score: brokerScore,
		Status: scoreToStatus(brokerScore), Details: brokerDetails, Weight: 3,
	})

	// Under-replicated partitions (weight: 2.5)
	urpScore := 100.0
	urpDetails := ""
	if input.TotalPartitions > 0 && input.UnderReplicatedPartitions > 0 {
		ratio := float64(input.UnderReplicatedPartitions) / float64(input.TotalPartitions)
		urpScore = math.Max(0, 100-ratio*500)
		urpDetails = fmt.Sprintf("%d under-replicated of %d total", input.UnderReplicatedPartitions, input.TotalPartitions)
	}
	components = append(components, ComponentScore{
		Name: "replication_health", Score: urpScore,
		Status: scoreToStatus(urpScore), Details: urpDetails, Weight: 2.5,
	})

	// ISR shrink rate (weight: 2)
	isrScore := 100.0
	isrDetails := ""
	if input.ISRShrinkRate > 0 {
		isrScore = math.Max(0, 100-input.ISRShrinkRate*20)
		isrDetails = fmt.Sprintf("ISR shrink rate: %.2f/sec", input.ISRShrinkRate)
	}
	components = append(components, ComponentScore{
		Name: "isr_stability", Score: isrScore,
		Status: scoreToStatus(isrScore), Details: isrDetails, Weight: 2,
	})

	// Consumer lag (weight: 1.5)
	lagScore := 100.0
	lagDetails := ""
	if input.MaxConsumerLag > 0 {
		switch {
		case input.MaxConsumerLag > 1000000:
			lagScore = 20
		case input.MaxConsumerLag > 100000:
			lagScore = 50
		case input.MaxConsumerLag > 10000:
			lagScore = 75
		case input.MaxConsumerLag > 1000:
			lagScore = 90
		}
		lagDetails = fmt.Sprintf("max lag: %d, total lag: %d", input.MaxConsumerLag, input.TotalConsumerLag)
	}
	components = append(components, ComponentScore{
		Name: "consumer_lag", Score: lagScore,
		Status: scoreToStatus(lagScore), Details: lagDetails, Weight: 1.5,
	})

	// Controller health (weight: 3)
	controllerScore := 100.0
	controllerDetails := ""
	if input.ActiveControllerCount != 1 {
		controllerScore = 0
		controllerDetails = fmt.Sprintf("active controllers: %d (expected 1)", input.ActiveControllerCount)
	}
	components = append(components, ComponentScore{
		Name: "controller_health", Score: controllerScore,
		Status: scoreToStatus(controllerScore), Details: controllerDetails, Weight: 3,
	})

	// Offline partitions (weight: 3)
	offlineScore := 100.0
	offlineDetails := ""
	if input.OfflinePartitions > 0 {
		offlineScore = 0
		offlineDetails = fmt.Sprintf("%d offline partitions", input.OfflinePartitions)
	}
	components = append(components, ComponentScore{
		Name: "partition_availability", Score: offlineScore,
		Status: scoreToStatus(offlineScore), Details: offlineDetails, Weight: 3,
	})

	return components
}

func (e *Engine) generateRecommendations(input ClusterMetricsInput, components []ComponentScore) []string {
	var recs []string
	for _, c := range components {
		if c.Score < 80 {
			switch c.Name {
			case "broker_availability":
				recs = append(recs, "Investigate offline brokers and restore availability")
			case "replication_health":
				recs = append(recs, "Run partition reassignment to fix under-replicated partitions")
			case "isr_stability":
				recs = append(recs, "Check broker disk I/O and network connectivity for ISR stability")
			case "consumer_lag":
				recs = append(recs, "Scale consumers or investigate slow processing to reduce lag")
			case "controller_health":
				recs = append(recs, "Investigate controller election issues immediately")
			case "partition_availability":
				recs = append(recs, "Critical: offline partitions detected, investigate immediately")
			}
		}
	}
	return recs
}

// GetHistory returns recent health scores for a cluster.
func (e *Engine) GetHistory(cluster string, limit int) []HealthReport {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if limit <= 0 {
		limit = 20
	}
	history := e.history[cluster]
	if len(history) > limit {
		history = history[len(history)-limit:]
	}
	result := make([]HealthReport, len(history))
	copy(result, history)
	return result
}

func scoreToStatus(score float64) string {
	switch {
	case score >= 90:
		return "healthy"
	case score >= 70:
		return "warning"
	case score >= 50:
		return "degraded"
	default:
		return "critical"
	}
}
