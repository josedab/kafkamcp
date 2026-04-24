package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// ReplicationLink represents a monitored replication link between clusters.
type ReplicationLink struct {
	Source      string            `json:"source_cluster"`
	Target      string            `json:"target_cluster"`
	Type        string            `json:"type"` // "mirrormaker2", "cluster_linking"
	Topics      []TopicReplication `json:"topics"`
	Status      string            `json:"status"` // "active", "lagging", "error", "unknown"
	LastChecked string            `json:"last_checked"`
}

// TopicReplication tracks replication state for a single topic.
type TopicReplication struct {
	Topic      string               `json:"topic"`
	Partitions []PartitionReplication `json:"partitions"`
	TotalLag   int64                `json:"total_lag"`
	Status     string               `json:"status"`
}

// PartitionReplication tracks replication lag for a single partition.
type PartitionReplication struct {
	Partition    int32  `json:"partition"`
	SourceOffset int64  `json:"source_offset"`
	TargetOffset int64  `json:"target_offset"`
	Lag          int64  `json:"lag"`
	Status       string `json:"status"`
}

// Alert represents a replication lag alert.
type Alert struct {
	Source    string `json:"source_cluster"`
	Target   string `json:"target_cluster"`
	Topic    string `json:"topic"`
	Partition int32 `json:"partition"`
	Lag      int64  `json:"lag"`
	Threshold int64 `json:"threshold"`
	Severity string `json:"severity"` // "warning", "critical"
	Time     string `json:"time"`
}

// ReplicationStatus is the overall replication status across all links.
type ReplicationStatus struct {
	Links       []ReplicationLink `json:"links"`
	TotalLinks  int               `json:"total_links"`
	ActiveLinks int               `json:"active_links"`
	Alerts      []Alert           `json:"alerts"`
}

// MonitorConfig configures replication monitoring thresholds.
type MonitorConfig struct {
	WarningLagThreshold  int64 `json:"warning_lag_threshold"`
	CriticalLagThreshold int64 `json:"critical_lag_threshold"`
	CheckIntervalSeconds int   `json:"check_interval_seconds"`
}

// Monitor tracks cross-cluster replication links and lag.
type Monitor struct {
	mu     sync.RWMutex
	links  map[string]*ReplicationLink // key: "source->target"
	alerts []Alert
	config MonitorConfig
	logger *slog.Logger
}

// NewMonitor creates a new replication monitor.
func NewMonitor(logger *slog.Logger) *Monitor {
	return &Monitor{
		links: make(map[string]*ReplicationLink),
		config: MonitorConfig{
			WarningLagThreshold:  1000,
			CriticalLagThreshold: 10000,
			CheckIntervalSeconds: 60,
		},
		logger: logger,
	}
}

// RegisterLink adds a replication link to monitor.
func (m *Monitor) RegisterLink(source, target, linkType string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s->%s", source, target)
	m.links[key] = &ReplicationLink{
		Source:      source,
		Target:      target,
		Type:        linkType,
		Status:      "unknown",
		LastChecked: time.Now().UTC().Format(time.RFC3339),
	}
	if m.logger != nil {
		m.logger.Info("replication link registered", "source", source, "target", target, "type", linkType)
	}
}

// UpdateTopicLag updates the lag for a topic-partition on a replication link.
func (m *Monitor) UpdateTopicLag(source, target, topic string, partition int32, sourceOffset, targetOffset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s->%s", source, target)
	link, ok := m.links[key]
	if !ok {
		return
	}

	lag := sourceOffset - targetOffset
	if lag < 0 {
		lag = 0
	}

	partRepl := PartitionReplication{
		Partition:    partition,
		SourceOffset: sourceOffset,
		TargetOffset: targetOffset,
		Lag:          lag,
		Status:       "active",
	}

	if lag >= m.config.CriticalLagThreshold {
		partRepl.Status = "critical"
	} else if lag >= m.config.WarningLagThreshold {
		partRepl.Status = "warning"
	}

	// Find or create topic entry
	var topicRepl *TopicReplication
	for i := range link.Topics {
		if link.Topics[i].Topic == topic {
			topicRepl = &link.Topics[i]
			break
		}
	}
	if topicRepl == nil {
		link.Topics = append(link.Topics, TopicReplication{Topic: topic})
		topicRepl = &link.Topics[len(link.Topics)-1]
	}

	// Update partition
	found := false
	for i := range topicRepl.Partitions {
		if topicRepl.Partitions[i].Partition == partition {
			topicRepl.Partitions[i] = partRepl
			found = true
			break
		}
	}
	if !found {
		topicRepl.Partitions = append(topicRepl.Partitions, partRepl)
	}

	// Recalculate total lag
	var totalLag int64
	worstStatus := "active"
	for _, p := range topicRepl.Partitions {
		totalLag += p.Lag
		if p.Status == "critical" {
			worstStatus = "critical"
		} else if p.Status == "warning" && worstStatus != "critical" {
			worstStatus = "warning"
		}
	}
	topicRepl.TotalLag = totalLag
	topicRepl.Status = worstStatus

	// Update link status
	link.LastChecked = time.Now().UTC().Format(time.RFC3339)
	linkStatus := "active"
	for _, t := range link.Topics {
		if t.Status == "critical" {
			linkStatus = "lagging"
			break
		}
	}
	link.Status = linkStatus

	// Generate alerts for threshold breaches
	if lag >= m.config.WarningLagThreshold {
		severity := "warning"
		if lag >= m.config.CriticalLagThreshold {
			severity = "critical"
		}
		alert := Alert{
			Source:    source,
			Target:    target,
			Topic:    topic,
			Partition: partition,
			Lag:      lag,
			Threshold: m.config.WarningLagThreshold,
			Severity: severity,
			Time:     time.Now().UTC().Format(time.RFC3339),
		}
		m.alerts = append(m.alerts, alert)
		// Keep last 1000 alerts
		if len(m.alerts) > 1000 {
			m.alerts = m.alerts[len(m.alerts)-1000:]
		}
	}
}

// GetStatus returns the overall replication status.
func (m *Monitor) GetStatus(_ context.Context) *ReplicationStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := &ReplicationStatus{
		TotalLinks: len(m.links),
	}

	for _, link := range m.links {
		status.Links = append(status.Links, *link)
		if link.Status == "active" {
			status.ActiveLinks++
		}
	}

	// Return recent alerts (last 100)
	alertCount := len(m.alerts)
	if alertCount > 100 {
		status.Alerts = m.alerts[alertCount-100:]
	} else {
		status.Alerts = m.alerts
	}

	return status
}

// SetThresholds updates the alert thresholds.
func (m *Monitor) SetThresholds(warning, critical int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.config.WarningLagThreshold = warning
	m.config.CriticalLagThreshold = critical
}

// FailoverReadiness evaluates how ready a target cluster is to take over from a source.
type FailoverReadiness struct {
	Source          string   `json:"source_cluster"`
	Target          string   `json:"target_cluster"`
	ReadinessScore  int      `json:"readiness_score"`
	Status          string   `json:"status"`
	MaxLag          int64    `json:"max_lag"`
	AvgLag          float64  `json:"avg_lag"`
	TopicsInSync    int      `json:"topics_in_sync"`
	TopicsOutOfSync int      `json:"topics_out_of_sync"`
	EstimatedRPO    string   `json:"estimated_rpo"`
	EstimatedRTO    string   `json:"estimated_rto"`
	Recommendations []string `json:"recommendations"`
}

// GetLagDetails returns per-topic lag details for a replication link.
func (m *Monitor) GetLagDetails(source, target string) ([]TopicReplication, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s->%s", source, target)
	link, ok := m.links[key]
	if !ok {
		return nil, fmt.Errorf("replication link %s not found", key)
	}

	result := make([]TopicReplication, len(link.Topics))
	copy(result, link.Topics)
	return result, nil
}

// EvaluateFailoverReadiness computes a failover readiness score for a replication link.
func (m *Monitor) EvaluateFailoverReadiness(source, target string) (*FailoverReadiness, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s->%s", source, target)
	link, ok := m.links[key]
	if !ok {
		return nil, fmt.Errorf("replication link %s not found", key)
	}

	fr := &FailoverReadiness{
		Source: source,
		Target: target,
	}

	if len(link.Topics) == 0 {
		fr.ReadinessScore = 0
		fr.Status = "not_ready"
		fr.Recommendations = []string{"No topics are being replicated"}
		fr.EstimatedRPO = "unknown"
		fr.EstimatedRTO = "unknown"
		return fr, nil
	}

	var totalLag int64
	var maxLag int64
	var partitionCount int64
	for _, t := range link.Topics {
		if t.TotalLag == 0 {
			fr.TopicsInSync++
		} else {
			fr.TopicsOutOfSync++
		}
		totalLag += t.TotalLag
		for _, p := range t.Partitions {
			partitionCount++
			if p.Lag > maxLag {
				maxLag = p.Lag
			}
		}
	}

	fr.MaxLag = maxLag
	if partitionCount > 0 {
		fr.AvgLag = float64(totalLag) / float64(partitionCount)
	}

	// Score calculation
	score := 100
	if maxLag > m.config.CriticalLagThreshold {
		score -= 50
	} else if maxLag > m.config.WarningLagThreshold {
		score -= 25
	}
	if fr.TopicsOutOfSync > 0 {
		outOfSyncPct := float64(fr.TopicsOutOfSync) / float64(len(link.Topics)) * 100
		if outOfSyncPct > 50 {
			score -= 30
		} else if outOfSyncPct > 20 {
			score -= 15
		} else {
			score -= 5
		}
	}
	if score < 0 {
		score = 0
	}
	fr.ReadinessScore = score

	switch {
	case score >= 80:
		fr.Status = "ready"
	case score >= 50:
		fr.Status = "at_risk"
	default:
		fr.Status = "not_ready"
	}

	// Estimate RPO based on lag
	if maxLag == 0 {
		fr.EstimatedRPO = "0s (fully synced)"
	} else if maxLag < 100 {
		fr.EstimatedRPO = "<1s"
	} else if maxLag < 10000 {
		fr.EstimatedRPO = "<10s"
	} else {
		fr.EstimatedRPO = fmt.Sprintf("~%ds (estimated)", maxLag/1000)
	}
	fr.EstimatedRTO = "30-60s (manual failover)"

	// Recommendations
	if fr.TopicsOutOfSync > 0 {
		fr.Recommendations = append(fr.Recommendations,
			fmt.Sprintf("%d topics are out of sync — investigate replication lag", fr.TopicsOutOfSync))
	}
	if maxLag > m.config.CriticalLagThreshold {
		fr.Recommendations = append(fr.Recommendations,
			"Critical lag detected — check network bandwidth and replicator health")
	}
	if score >= 80 && fr.TopicsOutOfSync == 0 {
		fr.Recommendations = append(fr.Recommendations, "Cluster is ready for failover")
	}

	return fr, nil
}

// GetRecentAlerts returns alerts for a specific replication link.
func (m *Monitor) GetRecentAlerts(source, target string, limit int) []Alert {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 {
		limit = 50
	}

	var filtered []Alert
	for i := len(m.alerts) - 1; i >= 0 && len(filtered) < limit; i-- {
		a := m.alerts[i]
		if (source == "" || a.Source == source) && (target == "" || a.Target == target) {
			filtered = append(filtered, a)
		}
	}
	return filtered
}
