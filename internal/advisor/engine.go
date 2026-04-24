package advisor

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// TopicMetrics holds topic info for analysis.
type TopicMetrics struct {
	Name              string            `json:"name"`
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replication_factor"`
	RetentionMs       int64             `json:"retention_ms"`
	MessageCount      int64             `json:"message_count"`
	IsInternal        bool              `json:"is_internal"`
	Configs           map[string]string `json:"configs,omitempty"`
	ConsumerGroups    []string          `json:"consumer_groups,omitempty"`
}

// Recommendation is a single actionable recommendation.
type Recommendation struct {
	ID          string `json:"id"`
	Category    string `json:"category"`
	Severity    string `json:"severity"` // info, warning, critical
	Title       string `json:"title"`
	Description string `json:"description"`
	Topic       string `json:"topic,omitempty"`
	Impact      string `json:"impact"`
	Effort      string `json:"effort"` // low, medium, high
	AutoFixable bool   `json:"auto_fixable"`
}

// AntiPattern represents a detected anti-pattern.
type AntiPattern struct {
	Type        string   `json:"type"`
	Severity    string   `json:"severity"`
	Description string   `json:"description"`
	Topics      []string `json:"topics"`
	Suggestion  string   `json:"suggestion"`
}

// ScanResult holds the results of a topic advisory scan.
type ScanResult struct {
	ClusterName     string           `json:"cluster_name"`
	TopicsScanned   int              `json:"topics_scanned"`
	Recommendations []Recommendation `json:"recommendations"`
	AntiPatterns    []AntiPattern    `json:"anti_patterns"`
	Score           int              `json:"score"` // 0-100 health score
	ScannedAt       string           `json:"scanned_at"`
}

// Report is a detailed advisory report.
type Report struct {
	ClusterName       string           `json:"cluster_name"`
	Summary           string           `json:"summary"`
	OverallScore      int              `json:"overall_score"`
	CategoryScores    map[string]int   `json:"category_scores"`
	Recommendations   []Recommendation `json:"recommendations"`
	AntiPatterns      []AntiPattern    `json:"anti_patterns"`
	TopicCount        int              `json:"topic_count"`
	PartitionCount    int              `json:"partition_count"`
	GeneratedAt       string           `json:"generated_at"`
}

// RuleConfig holds configurable thresholds for best-practice checks.
type RuleConfig struct {
	MinReplicationFactor    int   `json:"min_replication_factor"`
	MaxPartitionsPerTopic   int   `json:"max_partitions_per_topic"`
	MinPartitionsPerTopic   int   `json:"min_partitions_per_topic"`
	MaxRetentionDays        int   `json:"max_retention_days"`
	MinRetentionHours       int   `json:"min_retention_hours"`
	OrphanedThresholdDays   int   `json:"orphaned_threshold_days"`
	MaxTopicNameLength      int   `json:"max_topic_name_length"`
	TopicNamingConvention   string `json:"topic_naming_convention"` // dot-separated, kebab-case, snake_case
}

// DefaultRuleConfig returns reasonable defaults.
func DefaultRuleConfig() RuleConfig {
	return RuleConfig{
		MinReplicationFactor:  3,
		MaxPartitionsPerTopic: 100,
		MinPartitionsPerTopic: 1,
		MaxRetentionDays:      30,
		MinRetentionHours:     1,
		OrphanedThresholdDays: 7,
		MaxTopicNameLength:    249,
		TopicNamingConvention: "dot-separated",
	}
}

// Engine is the topic advisor engine.
type Engine struct {
	config    RuleConfig
	lastScan  *ScanResult
	logger    *slog.Logger
	mu        sync.RWMutex
}

// NewEngine creates a new advisor engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		config: DefaultRuleConfig(),
		logger: logger,
	}
}

// SetConfig updates the rule configuration.
func (e *Engine) SetConfig(cfg RuleConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.config = cfg
}

// Scan analyzes topics and returns recommendations.
func (e *Engine) Scan(clusterName string, topics []TopicMetrics) *ScanResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	result := &ScanResult{
		ClusterName: clusterName,
		TopicsScanned: len(topics),
		ScannedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	// Run best-practice checks
	for _, t := range topics {
		if t.IsInternal {
			continue
		}
		result.Recommendations = append(result.Recommendations, e.checkBestPractices(t)...)
	}

	// Detect anti-patterns
	result.AntiPatterns = e.detectAntiPatterns(topics)

	// Calculate health score
	result.Score = e.calculateScore(result)

	e.lastScan = result
	return result
}

// GenerateReport creates a detailed report from the last scan.
func (e *Engine) GenerateReport(clusterName string, topics []TopicMetrics) *Report {
	scan := e.Scan(clusterName, topics)

	totalPartitions := 0
	for _, t := range topics {
		totalPartitions += t.Partitions
	}

	categoryScores := map[string]int{
		"replication":  100,
		"partitioning": 100,
		"retention":    100,
		"naming":       100,
		"hygiene":      100,
	}

	for _, r := range scan.Recommendations {
		deduction := 5
		if r.Severity == "critical" {
			deduction = 15
		} else if r.Severity == "warning" {
			deduction = 10
		}
		if score, ok := categoryScores[r.Category]; ok {
			categoryScores[r.Category] = max(0, score-deduction)
		}
	}

	return &Report{
		ClusterName:     clusterName,
		Summary:         fmt.Sprintf("Scanned %d topics with %d partitions. Found %d recommendations and %d anti-patterns.", len(topics), totalPartitions, len(scan.Recommendations), len(scan.AntiPatterns)),
		OverallScore:    scan.Score,
		CategoryScores:  categoryScores,
		Recommendations: scan.Recommendations,
		AntiPatterns:    scan.AntiPatterns,
		TopicCount:      len(topics),
		PartitionCount:  totalPartitions,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
	}
}

func (e *Engine) checkBestPractices(t TopicMetrics) []Recommendation {
	var recs []Recommendation

	// Replication factor check
	if t.ReplicationFactor < e.config.MinReplicationFactor {
		recs = append(recs, Recommendation{
			ID:          fmt.Sprintf("rf-%s", t.Name),
			Category:    "replication",
			Severity:    "critical",
			Title:       "Low replication factor",
			Description: fmt.Sprintf("Topic %q has replication factor %d (recommended: >= %d)", t.Name, t.ReplicationFactor, e.config.MinReplicationFactor),
			Topic:       t.Name,
			Impact:      "Data loss risk if broker fails",
			Effort:      "medium",
			AutoFixable: true,
		})
	}

	// Partition count checks
	if t.Partitions > e.config.MaxPartitionsPerTopic {
		recs = append(recs, Recommendation{
			ID:          fmt.Sprintf("part-high-%s", t.Name),
			Category:    "partitioning",
			Severity:    "warning",
			Title:       "Excessive partition count",
			Description: fmt.Sprintf("Topic %q has %d partitions (max recommended: %d)", t.Name, t.Partitions, e.config.MaxPartitionsPerTopic),
			Topic:       t.Name,
			Impact:      "Increased metadata overhead and rebalance time",
			Effort:      "high",
			AutoFixable: false,
		})
	}

	// Retention checks
	if t.RetentionMs > 0 {
		retentionDays := t.RetentionMs / (24 * 60 * 60 * 1000)
		if retentionDays > int64(e.config.MaxRetentionDays) {
			recs = append(recs, Recommendation{
				ID:          fmt.Sprintf("ret-high-%s", t.Name),
				Category:    "retention",
				Severity:    "info",
				Title:       "High retention period",
				Description: fmt.Sprintf("Topic %q retains data for %d days (recommended max: %d)", t.Name, retentionDays, e.config.MaxRetentionDays),
				Topic:       t.Name,
				Impact:      "Increased storage costs",
				Effort:      "low",
				AutoFixable: true,
			})
		}
		retentionHours := t.RetentionMs / (60 * 60 * 1000)
		if retentionHours < int64(e.config.MinRetentionHours) {
			recs = append(recs, Recommendation{
				ID:          fmt.Sprintf("ret-low-%s", t.Name),
				Category:    "retention",
				Severity:    "warning",
				Title:       "Very short retention",
				Description: fmt.Sprintf("Topic %q retains data for less than %d hours", t.Name, e.config.MinRetentionHours),
				Topic:       t.Name,
				Impact:      "May lose messages before consumers process them",
				Effort:      "low",
				AutoFixable: true,
			})
		}
	}

	// Naming convention
	if !e.checkNamingConvention(t.Name) {
		recs = append(recs, Recommendation{
			ID:          fmt.Sprintf("name-%s", t.Name),
			Category:    "naming",
			Severity:    "info",
			Title:       "Non-standard naming",
			Description: fmt.Sprintf("Topic %q doesn't follow %s convention", t.Name, e.config.TopicNamingConvention),
			Topic:       t.Name,
			Impact:      "Harder to discover and manage topics",
			Effort:      "high",
			AutoFixable: false,
		})
	}

	return recs
}

func (e *Engine) checkNamingConvention(name string) bool {
	switch e.config.TopicNamingConvention {
	case "dot-separated":
		return !strings.Contains(name, " ") && !strings.Contains(name, "_") || strings.Contains(name, ".")
	case "kebab-case":
		return !strings.Contains(name, " ") && !strings.Contains(name, "_") && !strings.Contains(name, ".")
	case "snake_case":
		return !strings.Contains(name, " ") && !strings.Contains(name, "-") && !strings.Contains(name, ".")
	default:
		return true
	}
}

func (e *Engine) detectAntiPatterns(topics []TopicMetrics) []AntiPattern {
	var patterns []AntiPattern

	// Topic sprawl detection
	if len(topics) > 100 {
		patterns = append(patterns, AntiPattern{
			Type:        "topic_sprawl",
			Severity:    "warning",
			Description: fmt.Sprintf("Cluster has %d topics which may indicate topic sprawl", len(topics)),
			Suggestion:  "Consider consolidating related topics or implementing topic lifecycle management",
		})
	}

	// Orphaned topic detection (no consumer groups)
	var orphaned []string
	for _, t := range topics {
		if !t.IsInternal && len(t.ConsumerGroups) == 0 && t.MessageCount > 0 {
			orphaned = append(orphaned, t.Name)
		}
	}
	if len(orphaned) > 0 {
		patterns = append(patterns, AntiPattern{
			Type:        "orphaned_topics",
			Severity:    "warning",
			Description: fmt.Sprintf("%d topics have messages but no active consumers", len(orphaned)),
			Topics:      orphaned,
			Suggestion:  "Review orphaned topics and either attach consumers or delete them",
		})
	}

	// Naming inconsistency detection
	conventions := make(map[string]int)
	for _, t := range topics {
		if t.IsInternal {
			continue
		}
		if strings.Contains(t.Name, ".") {
			conventions["dot-separated"]++
		} else if strings.Contains(t.Name, "-") {
			conventions["kebab-case"]++
		} else if strings.Contains(t.Name, "_") {
			conventions["snake_case"]++
		} else {
			conventions["flat"]++
		}
	}
	if len(conventions) > 2 {
		patterns = append(patterns, AntiPattern{
			Type:        "naming_inconsistency",
			Severity:    "info",
			Description: fmt.Sprintf("Multiple naming conventions detected: %v", conventions),
			Suggestion:  "Standardize on a single naming convention for all topics",
		})
	}

	// Single partition topics
	var singlePartition []string
	for _, t := range topics {
		if !t.IsInternal && t.Partitions == 1 && t.MessageCount > 10000 {
			singlePartition = append(singlePartition, t.Name)
		}
	}
	if len(singlePartition) > 0 {
		patterns = append(patterns, AntiPattern{
			Type:        "single_partition_high_volume",
			Severity:    "warning",
			Description: fmt.Sprintf("%d high-volume topics have only 1 partition", len(singlePartition)),
			Topics:      singlePartition,
			Suggestion:  "Increase partition count for better parallelism and throughput",
		})
	}

	return patterns
}

func (e *Engine) calculateScore(result *ScanResult) int {
	score := 100
	for _, r := range result.Recommendations {
		switch r.Severity {
		case "critical":
			score -= 10
		case "warning":
			score -= 5
		case "info":
			score -= 2
		}
	}
	for _, p := range result.AntiPatterns {
		switch p.Severity {
		case "critical":
			score -= 15
		case "warning":
			score -= 8
		case "info":
			score -= 3
		}
	}
	if score < 0 {
		score = 0
	}
	return score
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
