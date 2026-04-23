package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/urlutil"
)

// Topology represents the discovered replication topology across clusters.
type Topology struct {
	Clusters []ClusterNode      `json:"clusters"`
	Links    []TopologyLink     `json:"links"`
	Type     string             `json:"type"` // "hub-spoke", "mesh", "chain", "single"
	DetectedAt string           `json:"detected_at"`
}

// ClusterNode represents a cluster in the topology.
type ClusterNode struct {
	Name     string   `json:"name"`
	Role     string   `json:"role"` // "source", "target", "bidirectional"
	Topics   int      `json:"topics"`
	IsOnline bool     `json:"is_online"`
}

// TopologyLink represents a replication link in the topology.
type TopologyLink struct {
	Source    string `json:"source"`
	Target   string `json:"target"`
	Type     string `json:"type"` // "mirrormaker2", "cluster_linking", "custom"
	Topics   int    `json:"topics"`
	TotalLag int64  `json:"total_lag"`
	Status   string `json:"status"`
}

// AlertNotification represents a threshold alert for MCP notification.
type AlertNotification struct {
	ID        string `json:"id"`
	Severity  string `json:"severity"`
	Source    string `json:"source_cluster"`
	Target   string `json:"target_cluster"`
	Topic    string `json:"topic"`
	Partition int32 `json:"partition"`
	CurrentLag int64 `json:"current_lag"`
	Threshold int64 `json:"threshold"`
	Message  string `json:"message"`
	Time     string `json:"time"`
	Resolved bool   `json:"resolved"`
}

// NotificationConfig configures how alerts are delivered.
type NotificationConfig struct {
	WebhookURL    string        `json:"webhook_url,omitempty" yaml:"webhook_url,omitempty"`
	MCPChannel    bool          `json:"mcp_channel" yaml:"mcp_channel"`
	CooldownMins  int           `json:"cooldown_mins" yaml:"cooldown_mins"`
}

// TopologyDetector auto-discovers replication topologies.
type TopologyDetector struct {
	mu       sync.RWMutex
	topology *Topology
	monitor  *Monitor
	logger   interface{ Info(string, ...any) }
}

// NewTopologyDetector creates a topology detector.
func NewTopologyDetector(monitor *Monitor) *TopologyDetector {
	return &TopologyDetector{
		monitor: monitor,
		logger:  monitor.logger,
	}
}

// DetectTopology analyzes registered links to determine the replication topology.
func (td *TopologyDetector) DetectTopology() *Topology {
	td.monitor.mu.RLock()
	defer td.monitor.mu.RUnlock()

	topology := &Topology{
		DetectedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Build cluster set and link list
	clusterSet := make(map[string]*ClusterNode)
	sourceCount := make(map[string]int)
	targetCount := make(map[string]int)

	for _, link := range td.monitor.links {
		// Track source/target roles
		sourceCount[link.Source]++
		targetCount[link.Target]++

		if _, ok := clusterSet[link.Source]; !ok {
			clusterSet[link.Source] = &ClusterNode{
				Name:     link.Source,
				IsOnline: link.Status != "error",
			}
		}
		if _, ok := clusterSet[link.Target]; !ok {
			clusterSet[link.Target] = &ClusterNode{
				Name:     link.Target,
				IsOnline: link.Status != "error",
			}
		}

		var totalLag int64
		for _, t := range link.Topics {
			totalLag += t.TotalLag
		}

		topology.Links = append(topology.Links, TopologyLink{
			Source:   link.Source,
			Target:   link.Target,
			Type:     link.Type,
			Topics:   len(link.Topics),
			TotalLag: totalLag,
			Status:   link.Status,
		})
	}

	// Determine cluster roles
	for name, node := range clusterSet {
		sc := sourceCount[name]
		tc := targetCount[name]
		if sc > 0 && tc > 0 {
			node.Role = "bidirectional"
		} else if sc > 0 {
			node.Role = "source"
		} else if tc > 0 {
			node.Role = "target"
		}
		topology.Clusters = append(topology.Clusters, *node)
	}

	// Determine topology type
	topology.Type = td.classifyTopology(sourceCount, targetCount)

	td.mu.Lock()
	td.topology = topology
	td.mu.Unlock()

	return topology
}

func (td *TopologyDetector) classifyTopology(sourceCount, targetCount map[string]int) string {
	if len(sourceCount) == 0 && len(targetCount) == 0 {
		return "single"
	}

	// Check for mesh: multiple clusters are both source and target
	bidirectional := 0
	for name := range sourceCount {
		if targetCount[name] > 0 {
			bidirectional++
		}
	}
	if bidirectional > 1 {
		return "mesh"
	}

	// Check for hub-spoke: one source, multiple targets
	sourceOnlyCount := 0
	for name := range sourceCount {
		if targetCount[name] == 0 {
			sourceOnlyCount++
		}
	}
	if sourceOnlyCount == 1 && len(targetCount) > 1 {
		return "hub-spoke"
	}

	// Check for chain: A->B->C
	if len(sourceCount) > 1 && bidirectional == 0 {
		return "chain"
	}

	return "hub-spoke"
}

// GetTopology returns the last detected topology.
func (td *TopologyDetector) GetTopology() *Topology {
	td.mu.RLock()
	defer td.mu.RUnlock()
	return td.topology
}

// AlertManager manages alert notifications for replication lag.
type AlertManager struct {
	mu            sync.RWMutex
	notifications []AlertNotification
	config        NotificationConfig
	cooldowns     map[string]time.Time // key -> last alert time
	alertCounter  int
	httpClient    *http.Client
	webhookSem    chan struct{} // bounds concurrent webhook goroutines
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewAlertManager creates a new alert manager.
func NewAlertManager(ctx context.Context, config NotificationConfig) (*AlertManager, error) {
	if config.CooldownMins <= 0 {
		config.CooldownMins = 5
	}
	if config.WebhookURL != "" {
		if err := urlutil.ValidateWebhookURL(config.WebhookURL); err != nil {
			return nil, fmt.Errorf("invalid alert webhook: %w", err)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	return &AlertManager{
		config:     config,
		cooldowns:  make(map[string]time.Time),
		httpClient: &http.Client{Timeout: 10 * time.Second},
		webhookSem: make(chan struct{}, 10),
		ctx:        ctx,
		cancel:     cancel,
	}, nil
}

// ProcessAlert evaluates a replication alert and sends notifications if needed.
func (am *AlertManager) ProcessAlert(alert Alert) *AlertNotification {
	am.mu.Lock()
	defer am.mu.Unlock()

	// Check cooldown
	key := fmt.Sprintf("%s->%s:%s:%d", alert.Source, alert.Target, alert.Topic, alert.Partition)
	if lastAlert, ok := am.cooldowns[key]; ok {
		if time.Since(lastAlert) < time.Duration(am.config.CooldownMins)*time.Minute {
			return nil
		}
	}

	am.alertCounter++
	notif := &AlertNotification{
		ID:         fmt.Sprintf("repl-alert-%d", am.alertCounter),
		Severity:   alert.Severity,
		Source:     alert.Source,
		Target:     alert.Target,
		Topic:      alert.Topic,
		Partition:  alert.Partition,
		CurrentLag: alert.Lag,
		Threshold:  alert.Threshold,
		Message:    fmt.Sprintf("Replication lag %s on %s->%s topic %s partition %d: %d messages (threshold: %d)", alert.Severity, alert.Source, alert.Target, alert.Topic, alert.Partition, alert.Lag, alert.Threshold),
		Time:       time.Now().UTC().Format(time.RFC3339),
	}

	am.notifications = append(am.notifications, *notif)
	if len(am.notifications) > 1000 {
		am.notifications = am.notifications[len(am.notifications)-1000:]
	}

	am.cooldowns[key] = time.Now()

	// Send webhook if configured (bounded concurrency)
	if am.config.WebhookURL != "" {
		select {
		case am.webhookSem <- struct{}{}:
			go func() {
				defer func() { <-am.webhookSem }()
				am.sendWebhook(am.ctx, notif)
			}()
		default:
			// Drop webhook if all slots are busy
		}
	}

	return notif
}

// Stop cancels the alert manager context, terminating any in-flight webhook requests.
func (am *AlertManager) Stop() {
	am.cancel()
}

// GetRecentNotifications returns recent alert notifications.
func (am *AlertManager) GetRecentNotifications(limit int) []AlertNotification {
	am.mu.RLock()
	defer am.mu.RUnlock()

	if limit <= 0 || limit > len(am.notifications) {
		limit = len(am.notifications)
	}

	start := len(am.notifications) - limit
	if start < 0 {
		start = 0
	}

	result := make([]AlertNotification, limit)
	copy(result, am.notifications[start:])
	return result
}

func (am *AlertManager) sendWebhook(ctx context.Context, notif *AlertNotification) {
	body, err := json.Marshal(notif)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, am.config.WebhookURL, strings.NewReader(string(body)))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "KafkaMCP-Replication-Monitor/1.0")

	resp, err := am.httpClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// GetReplicationResource returns data for the kafka://replication/{source}/{target} MCP resource.
func (m *Monitor) GetReplicationResource(source, target string) (*ReplicationLink, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := fmt.Sprintf("%s->%s", source, target)
	link, ok := m.links[key]
	if !ok {
		return nil, fmt.Errorf("replication link %s not found", key)
	}
	return link, nil
}

// GetCrossClusterLinks returns all links for use in extending kafka://topic_graph.
func (m *Monitor) GetCrossClusterLinks() []TopologyLink {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var links []TopologyLink
	for _, link := range m.links {
		var totalLag int64
		for _, t := range link.Topics {
			totalLag += t.TotalLag
		}
		links = append(links, TopologyLink{
			Source:   link.Source,
			Target:   link.Target,
			Type:     link.Type,
			Topics:   len(link.Topics),
			TotalLag: totalLag,
			Status:   link.Status,
		})
	}
	return links
}
