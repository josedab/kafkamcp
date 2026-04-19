package cost

import (
	"fmt"
	"log/slog"
	"math"
	"time"
)

// CostModel defines pricing parameters for a cloud provider.
type CostModel struct {
	Provider          string  `json:"provider" yaml:"provider"`
	StorageCostPerGB  float64 `json:"storage_cost_per_gb" yaml:"storage_cost_per_gb"`   // per month
	NetworkCostPerGB  float64 `json:"network_cost_per_gb" yaml:"network_cost_per_gb"`
	PartitionCost     float64 `json:"partition_cost" yaml:"partition_cost"`              // per partition per month
	BrokerCostPerHour float64 `json:"broker_cost_per_hour" yaml:"broker_cost_per_hour"`
}

// DefaultCostModels returns cost models for common cloud providers.
func DefaultCostModels() map[string]CostModel {
	return map[string]CostModel{
		"aws-msk": {
			Provider:          "AWS MSK",
			StorageCostPerGB:  0.10,
			NetworkCostPerGB:  0.02,
			PartitionCost:     0.0015,
			BrokerCostPerHour: 0.21,
		},
		"confluent-cloud": {
			Provider:          "Confluent Cloud",
			StorageCostPerGB:  0.10,
			NetworkCostPerGB:  0.05,
			PartitionCost:     0.004,
			BrokerCostPerHour: 0.0,
		},
		"self-hosted": {
			Provider:          "Self-Hosted",
			StorageCostPerGB:  0.05,
			NetworkCostPerGB:  0.01,
			PartitionCost:     0.0,
			BrokerCostPerHour: 0.15,
		},
	}
}

// TopicCostInput describes a topic for cost estimation.
type TopicCostInput struct {
	Name              string  `json:"name"`
	Partitions        int     `json:"partitions"`
	ReplicationFactor int     `json:"replication_factor"`
	RetentionMs       int64   `json:"retention_ms"`
	AvgMsgSizeBytes   int64   `json:"avg_msg_size_bytes"`
	MsgsPerSecond     float64 `json:"msgs_per_second"`
	CompressionType   string  `json:"compression_type"`
}

// TopicCostResult is the cost analysis for a single topic.
type TopicCostResult struct {
	Topic             string  `json:"topic"`
	StorageGB         float64 `json:"storage_gb"`
	StorageCostMonth  float64 `json:"storage_cost_month"`
	NetworkCostMonth  float64 `json:"network_cost_month"`
	PartitionCost     float64 `json:"partition_cost_month"`
	TotalCostMonth    float64 `json:"total_cost_month"`
	TotalCostYear     float64 `json:"total_cost_year"`
	RetentionDays     float64 `json:"retention_days"`
	DataRateGBPerDay  float64 `json:"data_rate_gb_per_day"`
}

// WasteReport identifies cost optimization opportunities.
type WasteReport struct {
	TotalMonthlyWaste   float64         `json:"total_monthly_waste"`
	IdleTopics          []IdleTopic     `json:"idle_topics"`
	OverReplicated      []OverReplica   `json:"over_replicated"`
	CompressionOpps     []CompressionOpp `json:"compression_opportunities"`
	RetentionOpps       []RetentionOpp  `json:"retention_opportunities"`
	Recommendations     []string        `json:"recommendations"`
}

// IdleTopic is a topic with no active consumers.
type IdleTopic struct {
	Name           string  `json:"name"`
	DaysIdle       int     `json:"days_idle"`
	StorageGB      float64 `json:"storage_gb"`
	MonthlyCost    float64 `json:"monthly_cost"`
}

// OverReplica is a topic with more replicas than needed.
type OverReplica struct {
	Name              string  `json:"name"`
	CurrentRF         int     `json:"current_rf"`
	RecommendedRF     int     `json:"recommended_rf"`
	SavingsMonth      float64 `json:"savings_month"`
}

// CompressionOpp is a compression optimization opportunity.
type CompressionOpp struct {
	Name              string  `json:"name"`
	CurrentCompression string `json:"current_compression"`
	Recommended       string  `json:"recommended"`
	EstimatedSavings  float64 `json:"estimated_savings_pct"`
}

// RetentionOpp is a retention reduction opportunity.
type RetentionOpp struct {
	Name              string  `json:"name"`
	CurrentRetention  string  `json:"current_retention"`
	RecommendedDays   int     `json:"recommended_days"`
	SavingsMonth      float64 `json:"savings_month"`
}

// WhatIfScenario defines a hypothetical configuration change.
type WhatIfScenario struct {
	Topic           string `json:"topic"`
	NewRetentionMs  *int64 `json:"new_retention_ms,omitempty"`
	NewCompression  string `json:"new_compression,omitempty"`
	NewPartitions   *int   `json:"new_partitions,omitempty"`
	NewReplication  *int   `json:"new_replication,omitempty"`
}

// WhatIfResult shows the cost impact of a scenario.
type WhatIfResult struct {
	Scenario      WhatIfScenario `json:"scenario"`
	CurrentCost   float64        `json:"current_cost_month"`
	ProjectedCost float64        `json:"projected_cost_month"`
	Savings       float64        `json:"savings_month"`
	SavingsPct    float64        `json:"savings_pct"`
}

// Analyzer performs Kafka cost analysis.
type Analyzer struct {
	logger     *slog.Logger
	costModels map[string]CostModel
}

// NewAnalyzer creates a new cost analyzer.
func NewAnalyzer(logger *slog.Logger) *Analyzer {
	return &Analyzer{
		logger:     logger,
		costModels: DefaultCostModels(),
	}
}

// AnalyzeTopic calculates the cost for a single topic.
func (a *Analyzer) AnalyzeTopic(input TopicCostInput, provider string) (*TopicCostResult, error) {
	model, ok := a.costModels[provider]
	if !ok {
		model = a.costModels["aws-msk"]
	}

	retentionDays := float64(input.RetentionMs) / (1000 * 60 * 60 * 24)
	if retentionDays <= 0 {
		retentionDays = 7 // default
	}

	avgMsgSize := float64(input.AvgMsgSizeBytes)
	if avgMsgSize <= 0 {
		avgMsgSize = 500 // default 500 bytes
	}

	compressionRatio := compressionFactor(input.CompressionType)
	dataRateGBPerDay := (input.MsgsPerSecond * avgMsgSize * 86400 * compressionRatio) / (1024 * 1024 * 1024)
	storageGB := dataRateGBPerDay * retentionDays * float64(input.ReplicationFactor)

	storageCost := storageGB * model.StorageCostPerGB
	networkCost := dataRateGBPerDay * 30 * model.NetworkCostPerGB
	partitionCost := float64(input.Partitions) * model.PartitionCost
	total := storageCost + networkCost + partitionCost

	return &TopicCostResult{
		Topic:            input.Name,
		StorageGB:        math.Round(storageGB*100) / 100,
		StorageCostMonth: math.Round(storageCost*100) / 100,
		NetworkCostMonth: math.Round(networkCost*100) / 100,
		PartitionCost:    math.Round(partitionCost*100) / 100,
		TotalCostMonth:   math.Round(total*100) / 100,
		TotalCostYear:    math.Round(total*12*100) / 100,
		RetentionDays:    retentionDays,
		DataRateGBPerDay: math.Round(dataRateGBPerDay*100) / 100,
	}, nil
}

// DetectWaste identifies cost optimization opportunities across topics.
func (a *Analyzer) DetectWaste(topics []TopicCostInput, consumerGroups map[string]time.Time, provider string) (*WasteReport, error) {
	report := &WasteReport{}

	for _, topic := range topics {
		// Idle topic detection (no consumers for >7 days)
		if lastActive, ok := consumerGroups[topic.Name]; ok {
			idleDays := int(time.Since(lastActive).Hours() / 24)
			if idleDays > 7 {
				costResult, err := a.AnalyzeTopic(topic, provider)
				if err != nil {
					slog.Warn("cost analysis failed for idle topic", "topic", topic.Name, "error", err)
					continue
				}
				report.IdleTopics = append(report.IdleTopics, IdleTopic{
					Name:        topic.Name,
					DaysIdle:    idleDays,
					StorageGB:   costResult.StorageGB,
					MonthlyCost: costResult.TotalCostMonth,
				})
				report.TotalMonthlyWaste += costResult.TotalCostMonth
			}
		}

		// Over-replication detection
		if topic.ReplicationFactor > 3 {
			currentCost, err := a.AnalyzeTopic(topic, provider)
			if err != nil {
				slog.Warn("cost analysis failed for over-replicated topic", "topic", topic.Name, "error", err)
				continue
			}
			reduced := topic
			reduced.ReplicationFactor = 3
			reducedCost, err := a.AnalyzeTopic(reduced, provider)
			if err != nil {
				slog.Warn("cost analysis failed for reduced replication", "topic", topic.Name, "error", err)
				continue
			}
			savings := currentCost.TotalCostMonth - reducedCost.TotalCostMonth
			report.OverReplicated = append(report.OverReplicated, OverReplica{
				Name:          topic.Name,
				CurrentRF:     topic.ReplicationFactor,
				RecommendedRF: 3,
				SavingsMonth:  math.Round(savings*100) / 100,
			})
			report.TotalMonthlyWaste += savings
		}

		// Compression opportunities
		if topic.CompressionType == "" || topic.CompressionType == "none" {
			savings := 0.5 // ~50% savings with compression
			report.CompressionOpps = append(report.CompressionOpps, CompressionOpp{
				Name:               topic.Name,
				CurrentCompression: "none",
				Recommended:        "lz4",
				EstimatedSavings:   savings * 100,
			})
		}

		// Excessive retention
		retDays := float64(topic.RetentionMs) / (1000 * 60 * 60 * 24)
		if retDays > 30 && topic.MsgsPerSecond > 100 {
			currentCost, err := a.AnalyzeTopic(topic, provider)
			if err != nil {
				slog.Warn("cost analysis failed for retention check", "topic", topic.Name, "error", err)
				continue
			}
			reduced := topic
			reduced.RetentionMs = 7 * 24 * 60 * 60 * 1000 // 7 days
			reducedCost, err := a.AnalyzeTopic(reduced, provider)
			if err != nil {
				slog.Warn("cost analysis failed for reduced retention", "topic", topic.Name, "error", err)
				continue
			}
			savings := currentCost.TotalCostMonth - reducedCost.TotalCostMonth
			if savings > 0 {
				report.RetentionOpps = append(report.RetentionOpps, RetentionOpp{
					Name:             topic.Name,
					CurrentRetention: fmt.Sprintf("%.0f days", retDays),
					RecommendedDays:  7,
					SavingsMonth:     math.Round(savings*100) / 100,
				})
				report.TotalMonthlyWaste += savings
			}
		}
	}

	report.TotalMonthlyWaste = math.Round(report.TotalMonthlyWaste*100) / 100

	// Generate recommendations
	if len(report.IdleTopics) > 0 {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("Delete or archive %d idle topics to save $%.2f/month", len(report.IdleTopics), sumIdleCost(report.IdleTopics)))
	}
	if len(report.CompressionOpps) > 0 {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("Enable compression on %d topics for ~50%% storage reduction", len(report.CompressionOpps)))
	}
	if len(report.OverReplicated) > 0 {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("Reduce replication factor on %d over-replicated topics", len(report.OverReplicated)))
	}
	if len(report.RetentionOpps) > 0 {
		report.Recommendations = append(report.Recommendations,
			fmt.Sprintf("Review retention settings for %d high-throughput topics", len(report.RetentionOpps)))
	}

	return report, nil
}

// WhatIf simulates a configuration change and returns cost impact.
func (a *Analyzer) WhatIf(current TopicCostInput, scenario WhatIfScenario, provider string) (*WhatIfResult, error) {
	currentCost, err := a.AnalyzeTopic(current, provider)
	if err != nil {
		return nil, err
	}

	projected := current
	if scenario.NewRetentionMs != nil {
		projected.RetentionMs = *scenario.NewRetentionMs
	}
	if scenario.NewCompression != "" {
		projected.CompressionType = scenario.NewCompression
	}
	if scenario.NewPartitions != nil {
		projected.Partitions = *scenario.NewPartitions
	}
	if scenario.NewReplication != nil {
		projected.ReplicationFactor = *scenario.NewReplication
	}

	projectedCost, err := a.AnalyzeTopic(projected, provider)
	if err != nil {
		return nil, err
	}

	savings := currentCost.TotalCostMonth - projectedCost.TotalCostMonth
	savingsPct := 0.0
	if currentCost.TotalCostMonth > 0 {
		savingsPct = (savings / currentCost.TotalCostMonth) * 100
	}

	return &WhatIfResult{
		Scenario:      scenario,
		CurrentCost:   currentCost.TotalCostMonth,
		ProjectedCost: projectedCost.TotalCostMonth,
		Savings:       math.Round(savings*100) / 100,
		SavingsPct:    math.Round(savingsPct*100) / 100,
	}, nil
}

func compressionFactor(compressionType string) float64 {
	switch compressionType {
	case "gzip":
		return 0.3
	case "snappy":
		return 0.5
	case "lz4":
		return 0.5
	case "zstd":
		return 0.25
	default:
		return 1.0
	}
}

func sumIdleCost(topics []IdleTopic) float64 {
	var total float64
	for _, t := range topics {
		total += t.MonthlyCost
	}
	return total
}
