package observability

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Dashboard represents an auto-provisioned observability dashboard.
type Dashboard struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Provider  string            `json:"provider"` // "grafana", "datadog", "custom"
	Type      string            `json:"type"`     // "cluster", "topic", "consumer_group"
	Panels    []Panel           `json:"panels"`
	Target    string            `json:"target,omitempty"` // topic name, group ID, etc.
	CreatedAt string            `json:"created_at"`
	CreatedBy string            `json:"created_by"`
	URL       string            `json:"url,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// Panel represents a single panel/widget in a dashboard.
type Panel struct {
	Title      string `json:"title"`
	Type       string `json:"type"` // "graph", "stat", "table", "heatmap"
	MetricExpr string `json:"metric_expr"`
	Threshold  string `json:"threshold,omitempty"`
}

// AlertRule represents an alerting rule managed through MCP.
type AlertRule struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Metric      string   `json:"metric"`
	Condition   string   `json:"condition"` // "gt", "lt", "eq", "ne"
	Threshold   float64  `json:"threshold"`
	Duration    string   `json:"duration"` // e.g., "5m", "15m"
	Severity    string   `json:"severity"` // "info", "warning", "critical"
	Channels    []string `json:"channels"` // notification channels
	Topic       string   `json:"topic,omitempty"`
	GroupID     string   `json:"group_id,omitempty"`
	Enabled     bool     `json:"enabled"`
	CreatedAt   string   `json:"created_at"`
	CreatedBy   string   `json:"created_by"`
	LastFiredAt string   `json:"last_fired_at,omitempty"`
	FireCount   int      `json:"fire_count"`
}

// Incident represents an auto-created incident from anomaly detection.
type Incident struct {
	ID         string         `json:"id"`
	Title      string         `json:"title"`
	Severity   string         `json:"severity"`
	Source     string         `json:"source"` // "anomaly_detector", "alert_rule", "manual"
	Status     string         `json:"status"` // "open", "acknowledged", "resolved"
	Context    map[string]any `json:"context"`
	CreatedAt  string         `json:"created_at"`
	ResolvedAt string         `json:"resolved_at,omitempty"`
}

// Manager manages observability dashboards, alerts, and incidents.
type Manager struct {
	mu         sync.RWMutex
	dashboards map[string]*Dashboard
	alerts     map[string]*AlertRule
	incidents  map[string]*Incident
}

// NewManager creates a new observability manager.
func NewManager() *Manager {
	return &Manager{
		dashboards: make(map[string]*Dashboard),
		alerts:     make(map[string]*AlertRule),
		incidents:  make(map[string]*Incident),
	}
}

// CreateDashboard creates a pre-configured dashboard for a Kafka resource.
func (m *Manager) CreateDashboard(name, provider, dashType, target, createdBy string) (*Dashboard, error) {
	if name == "" {
		return nil, fmt.Errorf("dashboard name is required")
	}
	if provider == "" {
		provider = "grafana"
	}
	if dashType == "" {
		dashType = "cluster"
	}

	rid, err := randomID()
	if err != nil {
		return nil, fmt.Errorf("generating dashboard ID: %w", err)
	}
	id := fmt.Sprintf("dash_%s", rid)

	panels := m.generatePanels(dashType, target)

	dash := &Dashboard{
		ID:        id,
		Name:      name,
		Provider:  provider,
		Type:      dashType,
		Panels:    panels,
		Target:    target,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		CreatedBy: createdBy,
	}

	m.mu.Lock()
	m.dashboards[id] = dash
	m.mu.Unlock()

	return dash, nil
}

func (m *Manager) generatePanels(dashType, target string) []Panel {
	switch dashType {
	case "topic":
		return []Panel{
			{Title: "Messages In/Out Rate", Type: "graph", MetricExpr: fmt.Sprintf("kafka_topic_messages_in_total{topic=\"%s\"}", target)},
			{Title: "Bytes In/Out", Type: "graph", MetricExpr: fmt.Sprintf("kafka_topic_bytes_in_total{topic=\"%s\"}", target)},
			{Title: "Partition Count", Type: "stat", MetricExpr: fmt.Sprintf("kafka_topic_partitions{topic=\"%s\"}", target)},
			{Title: "Under-Replicated Partitions", Type: "stat", MetricExpr: fmt.Sprintf("kafka_topic_partition_under_replicated{topic=\"%s\"}", target), Threshold: "> 0"},
			{Title: "Consumer Lag by Group", Type: "graph", MetricExpr: fmt.Sprintf("kafka_consumer_group_lag{topic=\"%s\"}", target)},
		}
	case "consumer_group":
		return []Panel{
			{Title: "Consumer Lag", Type: "graph", MetricExpr: fmt.Sprintf("kafka_consumer_group_lag{group=\"%s\"}", target)},
			{Title: "Commit Rate", Type: "graph", MetricExpr: fmt.Sprintf("kafka_consumer_group_commits_total{group=\"%s\"}", target)},
			{Title: "Member Count", Type: "stat", MetricExpr: fmt.Sprintf("kafka_consumer_group_members{group=\"%s\"}", target)},
			{Title: "Rebalance Rate", Type: "graph", MetricExpr: fmt.Sprintf("kafka_consumer_group_rebalances_total{group=\"%s\"}", target)},
		}
	default: // cluster
		return []Panel{
			{Title: "Broker Count", Type: "stat", MetricExpr: "kafka_brokers_count"},
			{Title: "Messages/sec", Type: "graph", MetricExpr: "sum(rate(kafka_topic_messages_in_total[5m]))"},
			{Title: "Bytes In/Out", Type: "graph", MetricExpr: "sum(rate(kafka_topic_bytes_in_total[5m]))"},
			{Title: "Active Consumer Groups", Type: "stat", MetricExpr: "kafka_consumer_groups_active"},
			{Title: "Under-Replicated Partitions", Type: "stat", MetricExpr: "kafka_cluster_partition_under_replicated", Threshold: "> 0"},
			{Title: "Total Consumer Lag", Type: "graph", MetricExpr: "sum(kafka_consumer_group_lag)"},
		}
	}
}

// ListDashboards returns all dashboards.
func (m *Manager) ListDashboards() []Dashboard {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]Dashboard, 0, len(m.dashboards))
	for _, d := range m.dashboards {
		result = append(result, *d)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt > result[j].CreatedAt
	})
	return result
}

// CreateAlertRule creates a new alerting rule.
func (m *Manager) CreateAlertRule(name, metric, condition string, threshold float64, duration, severity, createdBy string, channels []string, topic, groupID string) (*AlertRule, error) {
	if name == "" {
		return nil, fmt.Errorf("alert name is required")
	}
	if metric == "" {
		return nil, fmt.Errorf("metric is required")
	}
	if condition == "" {
		condition = "gt"
	}
	if severity == "" {
		severity = "warning"
	}
	if duration == "" {
		duration = "5m"
	}

	rid, err := randomID()
	if err != nil {
		return nil, fmt.Errorf("generating alert rule ID: %w", err)
	}
	id := fmt.Sprintf("alert_%s", rid)

	rule := &AlertRule{
		ID:        id,
		Name:      name,
		Metric:    metric,
		Condition: condition,
		Threshold: threshold,
		Duration:  duration,
		Severity:  severity,
		Channels:  channels,
		Topic:     topic,
		GroupID:   groupID,
		Enabled:   true,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		CreatedBy: createdBy,
	}

	m.mu.Lock()
	m.alerts[id] = rule
	m.mu.Unlock()

	return rule, nil
}

// ListAlertRules returns all alert rules.
func (m *Manager) ListAlertRules() []AlertRule {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]AlertRule, 0, len(m.alerts))
	for _, r := range m.alerts {
		result = append(result, *r)
	}
	return result
}

// DeleteAlertRule removes an alert rule.
func (m *Manager) DeleteAlertRule(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.alerts[id]; !ok {
		return fmt.Errorf("alert rule %q not found", id)
	}
	delete(m.alerts, id)
	return nil
}

// CreateIncident creates an incident from an anomaly detection.
func (m *Manager) CreateIncident(title, severity, source string, context map[string]any) (*Incident, error) {
	rid, err := randomID()
	if err != nil {
		return nil, fmt.Errorf("generating incident ID: %w", err)
	}
	id := fmt.Sprintf("inc_%s", rid)

	incident := &Incident{
		ID:        id,
		Title:     title,
		Severity:  severity,
		Source:    source,
		Status:    "open",
		Context:   context,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	m.mu.Lock()
	m.incidents[id] = incident
	m.mu.Unlock()

	return incident, nil
}

// ListIncidents returns incidents, optionally filtered by status.
func (m *Manager) ListIncidents(status string) []Incident {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []Incident
	for _, inc := range m.incidents {
		if status == "" || inc.Status == status {
			result = append(result, *inc)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt > result[j].CreatedAt
	})
	return result
}

func randomID() (string, error) {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b), nil
}
