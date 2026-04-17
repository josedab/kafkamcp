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
