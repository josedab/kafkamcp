package lagdiag

import (
	"fmt"
	"math"
	"sync"
	"time"
)

const maxHistorySize = 100

// LagDiagnosis contains the result of a consumer lag root cause analysis.
type LagDiagnosis struct {
	GroupID         string       `json:"group_id"`
	Topic           string       `json:"topic"`
	TotalLag        int64        `json:"total_lag"`
	RootCause       string       `json:"root_cause"`
	Confidence      float64      `json:"confidence"`
	Factors         []DiagFactor `json:"factors"`
	Recommendations []string     `json:"recommendations"`
	Timestamp       string       `json:"timestamp"`
}

// DiagFactor represents a single contributing factor to consumer lag.
type DiagFactor struct {
	Name     string  `json:"name"`
	Category string  `json:"category"` // slow_consumer, producer_spike, partition_imbalance, network, gc_pressure
	Score    float64 `json:"score"`
	Evidence string  `json:"evidence"`
}

// LagSnapshot captures lag metrics for a single partition at a point in time.
type LagSnapshot struct {
	GroupID        string `json:"group_id"`
	Topic          string `json:"topic"`
	Partition      int32  `json:"partition"`
	Lag            int64  `json:"lag"`
	ConsumerOffset int64  `json:"consumer_offset"`
	LogEndOffset   int64  `json:"log_end_offset"`
	Timestamp      string `json:"timestamp"`
}

// Analyzer performs consumer lag root cause analysis using historical snapshots.
type Analyzer struct {
	mu      sync.RWMutex
	history map[string][]LagSnapshot
}

// NewAnalyzer creates a new Analyzer instance.
func NewAnalyzer() *Analyzer {
	return &Analyzer{
		history: make(map[string][]LagSnapshot),
	}
}

// RecordSnapshot stores a lag snapshot in the history ring buffer for the given group.
func (a *Analyzer) RecordSnapshot(snap LagSnapshot) {
	a.mu.Lock()
	defer a.mu.Unlock()

	key := snap.GroupID
	a.history[key] = append(a.history[key], snap)
	if len(a.history[key]) > maxHistorySize {
		a.history[key] = a.history[key][len(a.history[key])-maxHistorySize:]
	}
}

// Diagnose analyzes lag patterns and produces a root cause diagnosis.
func (a *Analyzer) Diagnose(groupID string, currentLag int64, partitionLags []LagSnapshot) *LagDiagnosis {
	a.mu.RLock()
	snapshots := make([]LagSnapshot, len(a.history[groupID]))
	copy(snapshots, a.history[groupID])
	a.mu.RUnlock()

	var factors []DiagFactor

	trendFactor := diagnoseLagTrend(snapshots)
	if trendFactor.Score > 0 {
		factors = append(factors, trendFactor)
	}

	imbalanceFactor := diagnosePartitionImbalance(partitionLags)
	if imbalanceFactor.Score > 0 {
		factors = append(factors, imbalanceFactor)
	}

	rootCause, confidence := determineRootCause(factors)
	recommendations := generateRecommendations(rootCause, factors)

	topic := ""
	if len(partitionLags) > 0 {
		topic = partitionLags[0].Topic
	}

	return &LagDiagnosis{
		GroupID:         groupID,
		Topic:           topic,
		TotalLag:        currentLag,
		RootCause:       rootCause,
		Confidence:      confidence,
		Factors:         factors,
		Recommendations: recommendations,
		Timestamp:       time.Now().UTC().Format(time.RFC3339),
	}
}

// diagnoseLagTrend checks whether lag is growing, stable, or shrinking over recent history.
func diagnoseLagTrend(snapshots []LagSnapshot) DiagFactor {
	if len(snapshots) < 2 {
		return DiagFactor{}
	}

	// Aggregate lag per snapshot timestamp to detect trend.
	type tsLag struct {
		ts  string
		lag int64
	}
	seen := make(map[string]int64)
	var order []string
	for _, s := range snapshots {
		if _, ok := seen[s.Timestamp]; !ok {
			order = append(order, s.Timestamp)
		}
		seen[s.Timestamp] += s.Lag
	}

	if len(order) < 2 {
		return DiagFactor{}
	}

	// Compare first half average to second half average.
	mid := len(order) / 2
	var firstSum, secondSum int64
	var firstCount, secondCount int
	for i, ts := range order {
		if i < mid {
			firstSum += seen[ts]
			firstCount++
		} else {
			secondSum += seen[ts]
			secondCount++
		}
	}

	firstAvg := float64(firstSum) / float64(firstCount)
	secondAvg := float64(secondSum) / float64(secondCount)

	if firstAvg == 0 && secondAvg == 0 {
		return DiagFactor{}
	}

	var ratio float64
	if firstAvg > 0 {
		ratio = (secondAvg - firstAvg) / firstAvg
	} else {
		ratio = 1.0
	}

	if ratio > 0.2 {
		score := math.Min(ratio, 1.0)
		return DiagFactor{
			Name:     "growing_lag",
			Category: "slow_consumer",
			Score:    score,
			Evidence: fmt.Sprintf("lag increased by %.0f%% (avg %.0f -> %.0f)", ratio*100, firstAvg, secondAvg),
		}
	}

	if ratio < -0.2 {
		return DiagFactor{
			Name:     "shrinking_lag",
			Category: "slow_consumer",
			Score:    0.1,
			Evidence: fmt.Sprintf("lag decreased by %.0f%% (avg %.0f -> %.0f)", -ratio*100, firstAvg, secondAvg),
		}
	}

	return DiagFactor{}
}

// diagnosePartitionImbalance checks if lag is skewed across partitions.
func diagnosePartitionImbalance(lags []LagSnapshot) DiagFactor {
	if len(lags) < 2 {
		return DiagFactor{}
	}

	var totalLag int64
	var maxLag int64
	for _, l := range lags {
		totalLag += l.Lag
		if l.Lag > maxLag {
			maxLag = l.Lag
		}
	}

	if totalLag == 0 {
		return DiagFactor{}
	}

	avgLag := float64(totalLag) / float64(len(lags))
	if avgLag == 0 {
		return DiagFactor{}
	}

	// Compute coefficient of variation.
	var sumSqDiff float64
	for _, l := range lags {
		diff := float64(l.Lag) - avgLag
		sumSqDiff += diff * diff
	}
	stddev := math.Sqrt(sumSqDiff / float64(len(lags)))
	cv := stddev / avgLag

	if cv > 0.5 {
		score := math.Min(cv/2.0, 1.0)
		return DiagFactor{
			Name:     "partition_skew",
			Category: "partition_imbalance",
			Score:    score,
			Evidence: fmt.Sprintf("partition lag CV=%.2f, max=%d, avg=%.0f", cv, maxLag, avgLag),
		}
	}

	return DiagFactor{}
}

// determineRootCause picks the factor with the highest score as root cause.
func determineRootCause(factors []DiagFactor) (string, float64) {
	if len(factors) == 0 {
		return "unknown", 0.0
	}

	best := factors[0]
	for _, f := range factors[1:] {
		if f.Score > best.Score {
			best = f
		}
	}

	return best.Category, best.Score
}

// generateRecommendations produces actionable recommendations based on the diagnosis.
func generateRecommendations(rootCause string, factors []DiagFactor) []string {
	var recs []string

	switch rootCause {
	case "slow_consumer":
		recs = append(recs, "Consider increasing consumer instances or parallelism")
		recs = append(recs, "Check consumer processing time per message")
		for _, f := range factors {
			if f.Name == "growing_lag" && f.Score > 0.5 {
				recs = append(recs, "Lag is growing rapidly; investigate consumer bottlenecks urgently")
			}
		}
	case "producer_spike":
		recs = append(recs, "Investigate recent producer throughput changes")
		recs = append(recs, "Consider adding consumer capacity to handle increased load")
	case "partition_imbalance":
		recs = append(recs, "Review partition assignment strategy")
		recs = append(recs, "Check for hot partitions caused by skewed keys")
	case "network":
		recs = append(recs, "Check network latency between consumers and brokers")
		recs = append(recs, "Verify broker health and replication status")
	case "gc_pressure":
		recs = append(recs, "Review consumer JVM/runtime garbage collection settings")
		recs = append(recs, "Consider increasing consumer memory allocation")
	default:
		recs = append(recs, "Collect more lag snapshots for better diagnosis")
	}

	return recs
}
