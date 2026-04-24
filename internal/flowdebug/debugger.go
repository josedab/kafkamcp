package flowdebug

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// FlowGraph represents the real-time message flow across topics.
type FlowGraph struct {
	Nodes      []FlowNode `json:"nodes"`
	Edges      []FlowEdge `json:"edges"`
	UpdatedAt  time.Time  `json:"updated_at"`
	ClusterID  string     `json:"cluster_id"`
}

// FlowNode is a topic or consumer group in the flow graph.
type FlowNode struct {
	ID             string         `json:"id"`
	Type           string         `json:"type"` // topic, consumer_group, producer
	Name           string         `json:"name"`
	Throughput     float64        `json:"throughput_msg_sec"`
	ErrorRate      float64        `json:"error_rate"`
	Partitions     int            `json:"partitions,omitempty"`
	ConsumerLag    int64          `json:"consumer_lag,omitempty"`
	HealthStatus   string         `json:"health_status"` // healthy, warning, critical
	Latency        *LatencyStats  `json:"latency,omitempty"`
}

// FlowEdge represents message flow between nodes.
type FlowEdge struct {
	Source          string  `json:"source"`
	Target          string  `json:"target"`
	Throughput      float64 `json:"throughput_msg_sec"`
	AvgLatencyMs    float64 `json:"avg_latency_ms"`
	ErrorRate       float64 `json:"error_rate"`
	Status          string  `json:"status"` // active, slow, error, idle
}

// LatencyStats contains latency distribution info.
type LatencyStats struct {
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
	MaxMs  float64 `json:"max_ms"`
}

// Bottleneck identifies a detected bottleneck in the flow.
type Bottleneck struct {
	NodeID      string  `json:"node_id"`
	NodeName    string  `json:"node_name"`
	Type        string  `json:"type"` // high_lag, low_throughput, high_error_rate, high_latency
	Severity    string  `json:"severity"` // warning, critical
	Description string  `json:"description"`
	Value       float64 `json:"value"`
	Threshold   float64 `json:"threshold"`
}

// FlowSnapshot is a point-in-time snapshot for time-range analysis.
type FlowSnapshot struct {
	Timestamp    time.Time     `json:"timestamp"`
	Nodes        []FlowNode    `json:"nodes"`
	Edges        []FlowEdge    `json:"edges"`
	Bottlenecks  []Bottleneck  `json:"bottlenecks"`
}

// MessageDetail is a single message for drill-down.
type MessageDetail struct {
	Topic       string            `json:"topic"`
	Partition   int32             `json:"partition"`
	Offset      int64             `json:"offset"`
	Timestamp   time.Time         `json:"timestamp"`
	Key         string            `json:"key"`
	ValueSize   int               `json:"value_size_bytes"`
	Headers     map[string]string `json:"headers"`
	Latency     float64           `json:"latency_ms,omitempty"`
}

// WebSocketMessage is the format for push updates.
type WebSocketMessage struct {
	Type    string `json:"type"` // flow_update, bottleneck_alert, node_update
	Payload any    `json:"payload"`
}

// BottleneckConfig defines configurable thresholds for bottleneck detection.
type BottleneckConfig struct {
	LagWarning       int64   `json:"lag_warning"`
	LagCritical      int64   `json:"lag_critical"`
	ErrorRateWarning  float64 `json:"error_rate_warning"`
	ErrorRateCritical float64 `json:"error_rate_critical"`
	LatencyWarningMs  float64 `json:"latency_warning_ms"`
	LatencyCriticalMs float64 `json:"latency_critical_ms"`
	ThroughputMinMsg  float64 `json:"throughput_min_msg_sec"`
}

// DefaultBottleneckConfig returns reasonable default thresholds.
func DefaultBottleneckConfig() BottleneckConfig {
	return BottleneckConfig{
		LagWarning:        10000,
		LagCritical:       100000,
		ErrorRateWarning:  0.01,
		ErrorRateCritical: 0.05,
		LatencyWarningMs:  1000,
		LatencyCriticalMs: 5000,
		ThroughputMinMsg:  1.0,
	}
}

// Debugger manages the visual message flow debugging.
type Debugger struct {
	currentGraph    *FlowGraph
	snapshots       []FlowSnapshot
	subscribers     map[string]chan WebSocketMessage
	mu              sync.RWMutex
	logger          *slog.Logger
	maxSnapshots    int
	bottleneckCfg   BottleneckConfig
}

// NewDebugger creates a new flow debugger.
func NewDebugger(logger *slog.Logger) *Debugger {
	return &Debugger{
		currentGraph:  &FlowGraph{},
		subscribers:   make(map[string]chan WebSocketMessage),
		logger:        logger,
		maxSnapshots:  360,
		bottleneckCfg: DefaultBottleneckConfig(),
	}
}

// SetBottleneckConfig updates the bottleneck detection thresholds.
func (d *Debugger) SetBottleneckConfig(cfg BottleneckConfig) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.bottleneckCfg = cfg
}

// GetBottleneckConfig returns the current bottleneck thresholds.
func (d *Debugger) GetBottleneckConfig() BottleneckConfig {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.bottleneckCfg
}

// UpdateGraph updates the current flow graph and detects bottlenecks.
func (d *Debugger) UpdateGraph(graph *FlowGraph) {
	d.mu.Lock()
	defer d.mu.Unlock()

	graph.UpdatedAt = time.Now()
	d.currentGraph = graph

	bottlenecks := d.detectBottlenecks(graph)

	snapshot := FlowSnapshot{
		Timestamp:   time.Now(),
		Nodes:       graph.Nodes,
		Edges:       graph.Edges,
		Bottlenecks: bottlenecks,
	}
	d.snapshots = append(d.snapshots, snapshot)
	if len(d.snapshots) > d.maxSnapshots {
		d.snapshots = d.snapshots[len(d.snapshots)-d.maxSnapshots:]
	}

	// Push to subscribers
	msg := WebSocketMessage{Type: "flow_update", Payload: snapshot}
	for _, ch := range d.subscribers {
		select {
		case ch <- msg:
		default:
			// subscriber too slow, skip
		}
	}

	if len(bottlenecks) > 0 {
		alertMsg := WebSocketMessage{Type: "bottleneck_alert", Payload: bottlenecks}
		for _, ch := range d.subscribers {
			select {
			case ch <- alertMsg:
			default:
			}
		}
	}
}

// GetCurrentGraph returns the latest flow graph.
func (d *Debugger) GetCurrentGraph() *FlowGraph {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.currentGraph
}

// GetSnapshots returns flow snapshots within a time range.
func (d *Debugger) GetSnapshots(since, until time.Time, limit int) []FlowSnapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var result []FlowSnapshot
	for _, s := range d.snapshots {
		if !s.Timestamp.Before(since) && (until.IsZero() || !s.Timestamp.After(until)) {
			result = append(result, s)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result
}

// Subscribe creates a channel for receiving real-time updates.
func (d *Debugger) Subscribe(clientID string) chan WebSocketMessage {
	d.mu.Lock()
	defer d.mu.Unlock()

	ch := make(chan WebSocketMessage, 100)
	d.subscribers[clientID] = ch
	return ch
}

// Unsubscribe removes a subscriber.
func (d *Debugger) Unsubscribe(clientID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if ch, ok := d.subscribers[clientID]; ok {
		close(ch)
		delete(d.subscribers, clientID)
	}
}

func (d *Debugger) detectBottlenecks(graph *FlowGraph) []Bottleneck {
	var bottlenecks []Bottleneck
	cfg := d.bottleneckCfg

	for _, node := range graph.Nodes {
		// High consumer lag
		if node.ConsumerLag > cfg.LagWarning {
			severity := "warning"
			if node.ConsumerLag > cfg.LagCritical {
				severity = "critical"
			}
			bottlenecks = append(bottlenecks, Bottleneck{
				NodeID:      node.ID,
				NodeName:    node.Name,
				Type:        "high_lag",
				Severity:    severity,
				Description: fmt.Sprintf("Consumer lag of %d messages on %s", node.ConsumerLag, node.Name),
				Value:       float64(node.ConsumerLag),
				Threshold:   float64(cfg.LagWarning),
			})
		}

		// High error rate
		if node.ErrorRate > cfg.ErrorRateWarning {
			severity := "warning"
			if node.ErrorRate > cfg.ErrorRateCritical {
				severity = "critical"
			}
			bottlenecks = append(bottlenecks, Bottleneck{
				NodeID:      node.ID,
				NodeName:    node.Name,
				Type:        "high_error_rate",
				Severity:    severity,
				Description: fmt.Sprintf("Error rate %.1f%% on %s", node.ErrorRate*100, node.Name),
				Value:       node.ErrorRate,
				Threshold:   cfg.ErrorRateWarning,
			})
		}

		// Low throughput
		if node.Throughput > 0 && node.Throughput < cfg.ThroughputMinMsg {
			bottlenecks = append(bottlenecks, Bottleneck{
				NodeID:      node.ID,
				NodeName:    node.Name,
				Type:        "low_throughput",
				Severity:    "warning",
				Description: fmt.Sprintf("Low throughput %.1f msg/s on %s", node.Throughput, node.Name),
				Value:       node.Throughput,
				Threshold:   cfg.ThroughputMinMsg,
			})
		}
	}

	for _, edge := range graph.Edges {
		// High latency
		if edge.AvgLatencyMs > cfg.LatencyWarningMs {
			severity := "warning"
			if edge.AvgLatencyMs > cfg.LatencyCriticalMs {
				severity = "critical"
			}
			bottlenecks = append(bottlenecks, Bottleneck{
				NodeID:      edge.Source,
				NodeName:    fmt.Sprintf("%s → %s", edge.Source, edge.Target),
				Type:        "high_latency",
				Severity:    severity,
				Description: fmt.Sprintf("Avg latency %.0fms between %s and %s", edge.AvgLatencyMs, edge.Source, edge.Target),
				Value:       edge.AvgLatencyMs,
				Threshold:   cfg.LatencyWarningMs,
			})
		}
	}

	return bottlenecks
}

// GetBottlenecks returns currently detected bottlenecks.
func (d *Debugger) GetBottlenecks() []Bottleneck {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.currentGraph == nil {
		return nil
	}
	return d.detectBottlenecks(d.currentGraph)
}

// SerializeUpdate marshals a WebSocket message to JSON.
func SerializeUpdate(msg WebSocketMessage) ([]byte, error) {
	return json.Marshal(msg)
}
