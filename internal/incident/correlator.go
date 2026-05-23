// Package incident provides AI-assisted cross-cluster incident correlation,
// using temporal proximity, topic dependency graphs, and schema lineage to
// identify root causes and generate structured incident reports.
package incident

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// Severity represents incident severity.
type Severity string

// SeverityInfo and related values define constants for this package.
const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// AnomalyEvent represents an anomaly detected on a cluster.
type AnomalyEvent struct {
	ID        string            `json:"id"`
	Cluster   string            `json:"cluster"`
	Topic     string            `json:"topic,omitempty"`
	Group     string            `json:"group,omitempty"`
	Type      string            `json:"type"` // consumer_lag, throughput_drop, error_spike, broker_down
	Severity  Severity          `json:"severity"`
	Value     float64           `json:"value,omitempty"`
	Threshold float64           `json:"threshold,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Details   map[string]string `json:"details,omitempty"`
}

// IncidentCluster groups correlated anomaly events.
type IncidentCluster struct {
	ID          string         `json:"id"`
	Events      []AnomalyEvent `json:"events"`
	Severity    Severity       `json:"severity"`
	Clusters    []string       `json:"affected_clusters"`
	Topics      []string       `json:"affected_topics"`
	Groups      []string       `json:"affected_groups"`
	RootCause   *RootCause     `json:"root_cause,omitempty"`
	BlastRadius int            `json:"blast_radius"`
	StartTime   time.Time      `json:"start_time"`
	EndTime     time.Time      `json:"end_time"`
	Duration    time.Duration  `json:"duration"`
	Status      string         `json:"status"` // active, resolved, investigating
}

// RootCause is the identified probable root cause.
type RootCause struct {
	Description string   `json:"description"`
	Confidence  float64  `json:"confidence"`
	Component   string   `json:"component"` // broker, producer, consumer, network, schema
	Evidence    []string `json:"evidence"`
}

// IncidentReport is a structured report of an incident.
type IncidentReport struct {
	Incident    IncidentCluster `json:"incident"`
	Timeline    []TimelineEntry `json:"timeline"`
	BlastRadius BlastRadius     `json:"blast_radius"`
	Remediation []string        `json:"remediation_steps"`
	GeneratedAt time.Time       `json:"generated_at"`
}

// TimelineEntry is a point on the incident timeline.
type TimelineEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Event     string    `json:"event"`
	Cluster   string    `json:"cluster"`
	Component string    `json:"component"`
}

// BlastRadius quantifies the impact of an incident.
type BlastRadius struct {
	ClustersAffected int      `json:"clusters_affected"`
	TopicsAffected   int      `json:"topics_affected"`
	GroupsAffected   int      `json:"groups_affected"`
	EstimatedImpact  string   `json:"estimated_impact"` // low, medium, high, critical
	DownstreamTopics []string `json:"downstream_topics,omitempty"`
}

// CorrelatorConfig configures the correlation engine.
type CorrelatorConfig struct {
	TemporalWindowMs int64   `json:"temporal_window_ms" yaml:"temporal_window_ms"`
	MinCorrelation   float64 `json:"min_correlation" yaml:"min_correlation"`
	MaxIncidents     int     `json:"max_incidents" yaml:"max_incidents"`
}

// DefaultCorrelatorConfig returns sensible defaults.
func DefaultCorrelatorConfig() CorrelatorConfig {
	return CorrelatorConfig{
		TemporalWindowMs: 60000, // 60 seconds
		MinCorrelation:   0.5,
		MaxIncidents:     100,
	}
}

// TopicDependency defines a dependency between topics.
type TopicDependency struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"` // produces_to, consumes_from, mirrors
}

// Correlator correlates anomalies across clusters.
type Correlator struct {
	mu           sync.RWMutex
	config       CorrelatorConfig
	events       []AnomalyEvent
	incidents    []IncidentCluster
	incidentMap  map[string]*IncidentCluster
	runbooks     map[string]*RemediationRunbook
	dependencies []TopicDependency
	logger       *slog.Logger
}

// NewCorrelator creates a new incident correlator.
func NewCorrelator(cfg CorrelatorConfig, logger *slog.Logger) *Correlator {
	return &Correlator{
		config:      cfg,
		events:      make([]AnomalyEvent, 0),
		incidents:   make([]IncidentCluster, 0),
		incidentMap: make(map[string]*IncidentCluster),
		runbooks:    make(map[string]*RemediationRunbook),
		logger:      logger,
	}
}

// SetDependencies configures the topic dependency graph.
func (c *Correlator) SetDependencies(deps []TopicDependency) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dependencies = deps
}

// IngestEvent adds an anomaly event and triggers correlation.
func (c *Correlator) IngestEvent(event AnomalyEvent) *IncidentCluster {
	c.mu.Lock()
	defer c.mu.Unlock()

	if event.ID == "" {
		event.ID = randid.New("evt")
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	c.events = append(c.events, event)

	// Find correlated events within temporal window
	windowStart := event.Timestamp.Add(-time.Duration(c.config.TemporalWindowMs) * time.Millisecond)
	windowEnd := event.Timestamp.Add(time.Duration(c.config.TemporalWindowMs) * time.Millisecond)

	var correlated []AnomalyEvent
	for _, e := range c.events {
		if e.Timestamp.After(windowStart) && e.Timestamp.Before(windowEnd) {
			if c.isCorrelated(event, e) {
				correlated = append(correlated, e)
			}
		}
	}

	if len(correlated) < 2 {
		return nil
	}

	// Check if these events belong to an existing incident
	for i := range c.incidents {
		if c.incidents[i].Status == "active" && c.overlaps(&c.incidents[i], correlated) {
			c.mergeIntoIncident(&c.incidents[i], correlated)
			return &c.incidents[i]
		}
	}

	// Create new incident
	incident := c.createIncident(correlated)
	if len(c.incidents) >= c.config.MaxIncidents {
		c.incidents = c.incidents[1:]
	}
	c.incidents = append(c.incidents, incident)
	c.incidentMap[incident.ID] = &c.incidents[len(c.incidents)-1]
	return &incident
}

// GetIncidents returns recent incidents.
func (c *Correlator) GetIncidents(limit int) []IncidentCluster {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if limit <= 0 || limit > len(c.incidents) {
		limit = len(c.incidents)
	}
	start := len(c.incidents) - limit
	result := make([]IncidentCluster, limit)
	copy(result, c.incidents[start:])
	return result
}

// GenerateReport creates a detailed incident report.
func (c *Correlator) GenerateReport(incidentID string) (*IncidentReport, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var incident *IncidentCluster
	for i := range c.incidents {
		if c.incidents[i].ID == incidentID {
			incident = &c.incidents[i]
			break
		}
	}
	if incident == nil {
		return nil, fmt.Errorf("incident %q not found", incidentID)
	}

	report := &IncidentReport{
		Incident:    *incident,
		Timeline:    c.buildTimeline(incident),
		BlastRadius: c.calculateBlastRadius(incident),
		Remediation: c.generateRemediation(incident),
		GeneratedAt: time.Now(),
	}
	return report, nil
}

// Stats returns correlator statistics.
func (c *Correlator) Stats() map[string]any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	active := 0
	for _, inc := range c.incidents {
		if inc.Status == "active" {
			active++
		}
	}
	return map[string]any{
		"total_events":     len(c.events),
		"total_incidents":  len(c.incidents),
		"active_incidents": active,
		"dependencies":     len(c.dependencies),
	}
}

func (c *Correlator) isCorrelated(a, b AnomalyEvent) bool {
	// Same cluster + related topics
	if a.Cluster == b.Cluster {
		return true
	}
	// Check topic dependency graph
	for _, dep := range c.dependencies {
		if (dep.Source == a.Topic && dep.Target == b.Topic) ||
			(dep.Source == b.Topic && dep.Target == a.Topic) {
			return true
		}
	}
	// Same topic across clusters (replication issues)
	if a.Topic != "" && a.Topic == b.Topic {
		return true
	}
	return false
}

func (c *Correlator) overlaps(incident *IncidentCluster, events []AnomalyEvent) bool {
	existingTopics := make(map[string]bool)
	for _, e := range incident.Events {
		existingTopics[e.Topic] = true
		existingTopics[e.Cluster] = true
	}
	for _, e := range events {
		if existingTopics[e.Topic] || existingTopics[e.Cluster] {
			return true
		}
	}
	return false
}

func (c *Correlator) mergeIntoIncident(incident *IncidentCluster, events []AnomalyEvent) {
	existingIDs := make(map[string]bool)
	for _, e := range incident.Events {
		existingIDs[e.ID] = true
	}
	for _, e := range events {
		if !existingIDs[e.ID] {
			incident.Events = append(incident.Events, e)
			existingIDs[e.ID] = true
		}
	}
	c.refreshIncidentMeta(incident)
}

func (c *Correlator) createIncident(events []AnomalyEvent) IncidentCluster {
	incident := IncidentCluster{
		ID:     randid.New("inc"),
		Events: events,
		Status: "active",
	}
	c.refreshIncidentMeta(&incident)
	incident.RootCause = c.identifyRootCause(&incident)
	return incident
}

func (c *Correlator) refreshIncidentMeta(incident *IncidentCluster) {
	clusters := make(map[string]bool)
	topics := make(map[string]bool)
	groups := make(map[string]bool)
	maxSeverity := SeverityInfo

	for _, e := range incident.Events {
		clusters[e.Cluster] = true
		if e.Topic != "" {
			topics[e.Topic] = true
		}
		if e.Group != "" {
			groups[e.Group] = true
		}
		if severityRank(e.Severity) > severityRank(maxSeverity) {
			maxSeverity = e.Severity
		}
		if incident.StartTime.IsZero() || e.Timestamp.Before(incident.StartTime) {
			incident.StartTime = e.Timestamp
		}
		if e.Timestamp.After(incident.EndTime) {
			incident.EndTime = e.Timestamp
		}
	}

	incident.Severity = maxSeverity
	incident.Clusters = mapKeys(clusters)
	incident.Topics = mapKeys(topics)
	incident.Groups = mapKeys(groups)
	incident.BlastRadius = len(clusters) + len(topics) + len(groups)
	incident.Duration = incident.EndTime.Sub(incident.StartTime)
}

func (c *Correlator) identifyRootCause(incident *IncidentCluster) *RootCause {
	if len(incident.Events) == 0 {
		return nil
	}

	// Sort by timestamp to find earliest event
	sort.Slice(incident.Events, func(i, j int) bool {
		return incident.Events[i].Timestamp.Before(incident.Events[j].Timestamp)
	})
	first := incident.Events[0]

	rc := &RootCause{
		Confidence: 0.7,
		Evidence:   []string{fmt.Sprintf("Earliest anomaly: %s on %s at %s", first.Type, first.Cluster, first.Timestamp.Format(time.RFC3339))},
	}

	switch first.Type {
	case "broker_down":
		rc.Description = fmt.Sprintf("Broker failure on cluster %s triggered cascading anomalies", first.Cluster)
		rc.Component = "broker"
		rc.Confidence = 0.85
	case "throughput_drop":
		rc.Description = fmt.Sprintf("Throughput degradation on %s propagated to dependent topics", first.Cluster)
		rc.Component = "producer"
	case "consumer_lag":
		rc.Description = fmt.Sprintf("Consumer lag spike on %s/%s indicates consumer-side issue", first.Cluster, first.Group)
		rc.Component = "consumer"
	case "error_spike":
		rc.Description = fmt.Sprintf("Error rate increase on %s suggests application or schema issue", first.Cluster)
		rc.Component = "schema"
	default:
		rc.Description = fmt.Sprintf("Anomaly cluster originating from %s on %s", first.Type, first.Cluster)
		rc.Component = "unknown"
		rc.Confidence = 0.5
	}

	if len(incident.Clusters) > 1 {
		rc.Evidence = append(rc.Evidence, fmt.Sprintf("Cross-cluster impact: %d clusters affected", len(incident.Clusters)))
	}

	return rc
}

func (c *Correlator) buildTimeline(incident *IncidentCluster) []TimelineEntry {
	var timeline []TimelineEntry
	for _, e := range incident.Events {
		timeline = append(timeline, TimelineEntry{
			Timestamp: e.Timestamp,
			Event:     fmt.Sprintf("%s: %s (value: %.2f)", e.Type, e.Topic, e.Value),
			Cluster:   e.Cluster,
			Component: e.Type,
		})
	}
	sort.Slice(timeline, func(i, j int) bool {
		return timeline[i].Timestamp.Before(timeline[j].Timestamp)
	})
	return timeline
}

func (c *Correlator) calculateBlastRadius(incident *IncidentCluster) BlastRadius {
	impact := "low"
	switch {
	case len(incident.Clusters) > 2 || len(incident.Topics) > 10:
		impact = "critical"
	case len(incident.Clusters) > 1 || len(incident.Topics) > 5:
		impact = "high"
	case len(incident.Topics) > 2:
		impact = "medium"
	}

	// Find downstream topics
	var downstream []string
	for _, dep := range c.dependencies {
		for _, topic := range incident.Topics {
			if dep.Source == topic {
				downstream = append(downstream, dep.Target)
			}
		}
	}

	return BlastRadius{
		ClustersAffected: len(incident.Clusters),
		TopicsAffected:   len(incident.Topics),
		GroupsAffected:   len(incident.Groups),
		EstimatedImpact:  impact,
		DownstreamTopics: downstream,
	}
}

func (c *Correlator) generateRemediation(incident *IncidentCluster) []string {
	var steps []string
	if incident.RootCause == nil {
		return []string{"Investigate anomaly events manually"}
	}
	switch incident.RootCause.Component {
	case "broker":
		steps = append(steps,
			"Check broker health and disk utilization on affected cluster",
			"Verify ISR (in-sync replicas) for affected partitions",
			"Consider rolling restart of degraded brokers",
		)
	case "consumer":
		steps = append(steps,
			"Check consumer group health and rebalance status",
			"Verify consumer application logs for errors",
			"Consider resetting consumer offsets if stuck",
		)
	case "producer":
		steps = append(steps,
			"Check producer application health and error logs",
			"Verify network connectivity between producers and brokers",
			"Monitor producer ack latency and retry rates",
		)
	case "schema":
		steps = append(steps,
			"Check Schema Registry for recent schema changes",
			"Verify producer/consumer schema compatibility",
			"Review recent deployments that may have changed message formats",
		)
	default:
		steps = append(steps, "Investigate anomaly events and check system health")
	}
	return steps
}

func severityRank(s Severity) int {
	switch s {
	case SeverityCritical:
		return 3
	case SeverityWarning:
		return 2
	case SeverityInfo:
		return 1
	default:
		return 0
	}
}

func mapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
