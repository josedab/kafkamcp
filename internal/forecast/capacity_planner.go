package forecast

import (
	"fmt"
	"math"
	"time"
)

// PartitionRecommendation recommends partition count for a topic.
type PartitionRecommendation struct {
	Topic              string  `json:"topic"`
	CurrentPartitions  int     `json:"current_partitions"`
	RecommendedParts   int     `json:"recommended_partitions"`
	Reason             string  `json:"reason"`
	ExpectedThroughput float64 `json:"expected_throughput_mbps"`
	ConfidenceLevel    string  `json:"confidence_level"` // high, medium, low
}

// BrokerRecommendation recommends broker configuration.
type BrokerRecommendation struct {
	CurrentBrokers     int     `json:"current_brokers"`
	RecommendedBrokers int     `json:"recommended_brokers"`
	RecommendedDiskGB  float64 `json:"recommended_disk_gb"`
	Reason             string  `json:"reason"`
	ScalingTrigger     string  `json:"scaling_trigger"` // storage, throughput, partitions
	TimelineDays       int     `json:"timeline_days"`
}

// CapacityForecast is a comprehensive capacity forecast with recommendations.
type CapacityForecast struct {
	Cluster              string                    `json:"cluster"`
	HorizonDays          int                       `json:"horizon_days"`
	TopicForecasts       []TopicForecast           `json:"topic_forecasts"`
	PartitionRecommends  []PartitionRecommendation `json:"partition_recommendations"`
	BrokerRecommendation *BrokerRecommendation     `json:"broker_recommendation,omitempty"`
	StorageForecast      StorageForecast           `json:"storage_forecast"`
	ThroughputForecast   ThroughputForecast        `json:"throughput_forecast"`
	CostProjection       CostProjection            `json:"cost_projection"`
	RiskAssessment       RiskAssessment            `json:"risk_assessment"`
	GeneratedAt          string                    `json:"generated_at"`
}

// TopicForecast is a per-topic growth forecast.
type TopicForecast struct {
	Topic           string  `json:"topic"`
	CurrentSizeMB   float64 `json:"current_size_mb"`
	PredictedSizeMB float64 `json:"predicted_size_mb"`
	GrowthRatePct   float64 `json:"growth_rate_pct"`
	Trend           string  `json:"trend"`
}

// StorageForecast predicts storage needs.
type StorageForecast struct {
	CurrentGB         float64 `json:"current_gb"`
	PredictedGB       float64 `json:"predicted_gb"`
	DaysUntilCapacity int     `json:"days_until_capacity"`
	MaxCapacityGB     float64 `json:"max_capacity_gb"`
}

// ThroughputForecast predicts throughput needs.
type ThroughputForecast struct {
	CurrentMBps   float64 `json:"current_mbps"`
	PredictedMBps float64 `json:"predicted_mbps"`
	PeakMBps      float64 `json:"peak_mbps"`
}

// CostProjection estimates infrastructure costs.
type CostProjection struct {
	CurrentMonthlyCost   float64 `json:"current_monthly_cost_usd"`
	ProjectedMonthlyCost float64 `json:"projected_monthly_cost_usd"`
	CostPerGB            float64 `json:"cost_per_gb_usd"`
	CostPerBroker        float64 `json:"cost_per_broker_usd"`
	Currency             string  `json:"currency"`
}

// RiskAssessment evaluates capacity-related risks.
type RiskAssessment struct {
	OverallRisk string       `json:"overall_risk"` // low, medium, high, critical
	RiskFactors []RiskFactor `json:"risk_factors"`
}

// RiskFactor is a single capacity risk.
type RiskFactor struct {
	Category    string `json:"category"` // storage, throughput, partitions, availability
	Risk        string `json:"risk"`
	Severity    string `json:"severity"`
	Description string `json:"description"`
	Mitigation  string `json:"mitigation"`
}

// PredictiveCapacityPlan generates a full capacity forecast.
func (f *Forecaster) PredictiveCapacityPlan(cluster string, horizonDays int, currentBrokers int, maxStorageGB, costPerGB, costPerBroker float64) *CapacityForecast {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if horizonDays <= 0 {
		horizonDays = 30
	}
	if maxStorageGB <= 0 {
		maxStorageGB = 1000
	}
	if costPerGB <= 0 {
		costPerGB = 0.10
	}
	if costPerBroker <= 0 {
		costPerBroker = 500
	}
	if currentBrokers <= 0 {
		currentBrokers = 3
	}

	forecast := &CapacityForecast{
		Cluster:     cluster,
		HorizonDays: horizonDays,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	topicSet := make(map[string]bool)
	for key := range f.metrics {
		parts := splitMetricKey(key)
		if parts[0] != "" {
			topicSet[parts[0]] = true
		}
	}

	var totalCurrentStorage, totalPredictedStorage float64
	var totalCurrentThroughput, totalPredictedThroughput, peakThroughput float64
	totalPartitions := 0

	for topic := range topicSet {
		tf := TopicForecast{Topic: topic}

		// Storage forecast
		storageKey := metricKey(topic, "storage")
		if points, ok := f.metrics[storageKey]; ok && len(points) > 0 {
			current := points[len(points)-1].Value
			tf.CurrentSizeMB = current / 1024 / 1024

			values := extractValues(points)
			predicted := ExponentialSmoothing(values, 0.3, horizonDays)
			if len(predicted) > 0 {
				tf.PredictedSizeMB = predicted[len(predicted)-1] / 1024 / 1024
				totalPredictedStorage += predicted[len(predicted)-1]
			}

			if tf.CurrentSizeMB > 0 {
				tf.GrowthRatePct = ((tf.PredictedSizeMB - tf.CurrentSizeMB) / tf.CurrentSizeMB) * 100
			}
			totalCurrentStorage += current
		}

		// Throughput forecast
		throughputKey := metricKey(topic, "throughput")
		if points, ok := f.metrics[throughputKey]; ok && len(points) > 0 {
			current := points[len(points)-1].Value
			totalCurrentThroughput += current

			values := extractValues(points)
			predicted := ExponentialSmoothing(values, 0.3, horizonDays)
			if len(predicted) > 0 {
				totalPredictedThroughput += predicted[len(predicted)-1]
			}

			for _, p := range points {
				if p.Value > peakThroughput {
					peakThroughput = p.Value
				}
			}
		}

		// Determine trend
		if tf.GrowthRatePct > 10 {
			tf.Trend = "rapid_growth"
		} else if tf.GrowthRatePct > 2 {
			tf.Trend = "growing"
		} else if tf.GrowthRatePct < -2 {
			tf.Trend = "shrinking"
		} else {
			tf.Trend = "stable"
		}

		forecast.TopicForecasts = append(forecast.TopicForecasts, tf)
		totalPartitions++
	}

	// Storage forecast
	currentStorageGB := totalCurrentStorage / 1024 / 1024 / 1024
	predictedStorageGB := totalPredictedStorage / 1024 / 1024 / 1024
	daysUntilCapacity := 0
	if predictedStorageGB > currentStorageGB {
		dailyGrowth := (predictedStorageGB - currentStorageGB) / float64(horizonDays)
		if dailyGrowth > 0 {
			remaining := maxStorageGB - currentStorageGB
			daysUntilCapacity = int(remaining / dailyGrowth)
		}
	}

	forecast.StorageForecast = StorageForecast{
		CurrentGB:         currentStorageGB,
		PredictedGB:       predictedStorageGB,
		DaysUntilCapacity: daysUntilCapacity,
		MaxCapacityGB:     maxStorageGB,
	}

	forecast.ThroughputForecast = ThroughputForecast{
		CurrentMBps:   totalCurrentThroughput / 1024 / 1024,
		PredictedMBps: totalPredictedThroughput / 1024 / 1024,
		PeakMBps:      peakThroughput / 1024 / 1024,
	}

	// Partition recommendations
	for _, tf := range forecast.TopicForecasts {
		if tf.Trend == "rapid_growth" {
			current := 3 // default assumption
			recommended := int(math.Ceil(float64(current) * (1 + tf.GrowthRatePct/100)))
			if recommended < current {
				recommended = current
			}
			forecast.PartitionRecommends = append(forecast.PartitionRecommends, PartitionRecommendation{
				Topic:              tf.Topic,
				CurrentPartitions:  current,
				RecommendedParts:   recommended,
				Reason:             fmt.Sprintf("%.1f%% growth rate requires more partitions for parallelism", tf.GrowthRatePct),
				ExpectedThroughput: tf.PredictedSizeMB / float64(horizonDays),
				ConfidenceLevel:    "medium",
			})
		}
	}

	// Broker recommendation
	recommendedBrokers := currentBrokers
	trigger := ""
	if predictedStorageGB > maxStorageGB*0.8 {
		recommendedBrokers = int(math.Ceil(float64(currentBrokers) * (predictedStorageGB / (maxStorageGB * 0.6))))
		trigger = "storage"
	}
	if totalPartitions > currentBrokers*2000 {
		needed := int(math.Ceil(float64(totalPartitions) / 2000))
		if needed > recommendedBrokers {
			recommendedBrokers = needed
			trigger = "partitions"
		}
	}

	if recommendedBrokers > currentBrokers {
		forecast.BrokerRecommendation = &BrokerRecommendation{
			CurrentBrokers:     currentBrokers,
			RecommendedBrokers: recommendedBrokers,
			RecommendedDiskGB:  predictedStorageGB / float64(recommendedBrokers) * 1.5,
			Reason:             fmt.Sprintf("Scale from %d to %d brokers based on %s growth", currentBrokers, recommendedBrokers, trigger),
			ScalingTrigger:     trigger,
			TimelineDays:       daysUntilCapacity,
		}
	}

	// Cost projection
	forecast.CostProjection = CostProjection{
		CurrentMonthlyCost:   float64(currentBrokers)*costPerBroker + currentStorageGB*costPerGB,
		ProjectedMonthlyCost: float64(recommendedBrokers)*costPerBroker + predictedStorageGB*costPerGB,
		CostPerGB:            costPerGB,
		CostPerBroker:        costPerBroker,
		Currency:             "USD",
	}

	// Risk assessment
	forecast.RiskAssessment = assessCapacityRisks(forecast)

	return forecast
}

func assessCapacityRisks(fc *CapacityForecast) RiskAssessment {
	ra := RiskAssessment{OverallRisk: "low"}

	if fc.StorageForecast.DaysUntilCapacity > 0 && fc.StorageForecast.DaysUntilCapacity < 7 {
		ra.RiskFactors = append(ra.RiskFactors, RiskFactor{
			Category: "storage", Risk: "imminent_exhaustion", Severity: "critical",
			Description: fmt.Sprintf("Storage will reach capacity in %d days", fc.StorageForecast.DaysUntilCapacity),
			Mitigation:  "Add brokers or increase disk capacity immediately",
		})
		ra.OverallRisk = "critical"
	} else if fc.StorageForecast.DaysUntilCapacity > 0 && fc.StorageForecast.DaysUntilCapacity < 30 {
		ra.RiskFactors = append(ra.RiskFactors, RiskFactor{
			Category: "storage", Risk: "approaching_capacity", Severity: "high",
			Description: fmt.Sprintf("Storage will reach capacity in %d days", fc.StorageForecast.DaysUntilCapacity),
			Mitigation:  "Plan scaling within the next 2 weeks",
		})
		if ra.OverallRisk == "low" {
			ra.OverallRisk = "high"
		}
	}

	for _, tf := range fc.TopicForecasts {
		if tf.GrowthRatePct > 50 {
			ra.RiskFactors = append(ra.RiskFactors, RiskFactor{
				Category: "throughput", Risk: "explosive_growth", Severity: "high",
				Description: fmt.Sprintf("Topic %q growing at %.0f%%", tf.Topic, tf.GrowthRatePct),
				Mitigation:  "Review producer patterns and add partitions proactively",
			})
			if ra.OverallRisk == "low" {
				ra.OverallRisk = "medium"
			}
		}
	}

	if len(ra.RiskFactors) == 0 {
		ra.RiskFactors = append(ra.RiskFactors, RiskFactor{
			Category: "general", Risk: "none", Severity: "low",
			Description: "No capacity risks detected in the forecast period",
			Mitigation:  "Continue monitoring",
		})
	}

	return ra
}
