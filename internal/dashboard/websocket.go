package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// WebSocketHub manages WebSocket connections for real-time updates.
type WebSocketHub struct {
	mu         sync.RWMutex
	clients    map[string]*WSClient
	clientID   atomic.Int64
	broadcast  chan []byte
	register   chan *WSClient
	unregister chan *WSClient
}

// WSClient represents a single WebSocket-like connection (using SSE as transport).
type WSClient struct {
	ID       string
	Ch       chan []byte
	Filters  map[string]string
	JoinedAt time.Time
}

// NewWebSocketHub creates a new WebSocket hub.
func NewWebSocketHub() *WebSocketHub {
	return &WebSocketHub{
		clients:    make(map[string]*WSClient),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *WSClient, 16),
		unregister: make(chan *WSClient, 16),
	}
}

// Run starts the hub event loop.
func (h *WebSocketHub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client.ID] = client
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			delete(h.clients, client.ID)
			h.mu.Unlock()
		case msg := <-h.broadcast:
			h.mu.RLock()
			for _, client := range h.clients {
				select {
				case client.Ch <- msg:
				default:
					// Drop if client is slow
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Broadcast sends a message to all connected clients.
func (h *WebSocketHub) Broadcast(eventType string, data any) {
	payload, err := json.Marshal(map[string]any{
		"type":      eventType,
		"data":      data,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return
	}
	select {
	case h.broadcast <- payload:
	default:
	}
}

// ClientCount returns the number of connected clients.
func (h *WebSocketHub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// ConsumerLagSnapshot captures lag for all consumer groups at a point in time.
type ConsumerLagSnapshot struct {
	Timestamp string                `json:"timestamp"`
	Groups    []ConsumerGroupLag    `json:"groups"`
	TotalLag  int64                 `json:"total_lag"`
}

// ConsumerGroupLag captures lag for a single consumer group.
type ConsumerGroupLag struct {
	GroupID    string          `json:"group_id"`
	State     string          `json:"state"`
	TotalLag  int64           `json:"total_lag"`
	Topics    []TopicLag      `json:"topics"`
}

// TopicLag captures lag for a single topic within a consumer group.
type TopicLag struct {
	Topic      string `json:"topic"`
	Partitions int    `json:"partitions"`
	TotalLag   int64  `json:"total_lag"`
}

// MessageFlowRate tracks message production/consumption rates.
type MessageFlowRate struct {
	Topic          string  `json:"topic"`
	ProduceRate    float64 `json:"produce_rate_per_sec"`
	ConsumeRate    float64 `json:"consume_rate_per_sec"`
	NetRate        float64 `json:"net_rate_per_sec"`
	LastUpdated    string  `json:"last_updated"`
}

// AgentActivityEntry represents a single agent's activity summary.
type AgentActivityEntry struct {
	AgentID        string `json:"agent_id"`
	TotalCalls     int64  `json:"total_calls"`
	ActiveSince    string `json:"active_since"`
	LastActivity   string `json:"last_activity"`
	TopTools       []ToolUsage `json:"top_tools"`
	ErrorRate      float64 `json:"error_rate"`
}

// ToolUsage tracks usage of a specific tool by an agent.
type ToolUsage struct {
	Tool    string `json:"tool"`
	Count   int64  `json:"count"`
	AvgMs   int64  `json:"avg_latency_ms"`
}

// DashboardMetrics holds aggregated dashboard metrics.
type DashboardMetrics struct {
	mu              sync.RWMutex
	agentActivity   map[string]*agentStats
	flowRates       map[string]*flowStats
}

type agentStats struct {
	totalCalls   int64
	errors       int64
	activeSince  time.Time
	lastActivity time.Time
	toolCounts   map[string]int64
	toolLatency  map[string]int64
}

type flowStats struct {
	produced  int64
	consumed  int64
	lastCheck time.Time
}

// NewDashboardMetrics creates a new metrics tracker.
func NewDashboardMetrics() *DashboardMetrics {
	return &DashboardMetrics{
		agentActivity: make(map[string]*agentStats),
		flowRates:     make(map[string]*flowStats),
	}
}

// RecordToolCall records an agent's tool invocation.
func (dm *DashboardMetrics) RecordToolCall(agentID, tool string, latencyMs int64, isError bool) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	stats, ok := dm.agentActivity[agentID]
	if !ok {
		stats = &agentStats{
			activeSince: time.Now(),
			toolCounts:  make(map[string]int64),
			toolLatency: make(map[string]int64),
		}
		dm.agentActivity[agentID] = stats
	}
	stats.totalCalls++
	stats.lastActivity = time.Now()
	stats.toolCounts[tool]++
	stats.toolLatency[tool] += latencyMs
	if isError {
		stats.errors++
	}
}

// GetAgentActivity returns agent activity summaries.
func (dm *DashboardMetrics) GetAgentActivity() []AgentActivityEntry {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	entries := make([]AgentActivityEntry, 0, len(dm.agentActivity))
	for id, stats := range dm.agentActivity {
		entry := AgentActivityEntry{
			AgentID:      id,
			TotalCalls:   stats.totalCalls,
			ActiveSince:  stats.activeSince.UTC().Format(time.RFC3339),
			LastActivity: stats.lastActivity.UTC().Format(time.RFC3339),
		}
		if stats.totalCalls > 0 {
			entry.ErrorRate = float64(stats.errors) / float64(stats.totalCalls)
		}

		// Top tools
		type kv struct {
			tool  string
			count int64
		}
		var tools []kv
		for t, c := range stats.toolCounts {
			tools = append(tools, kv{t, c})
		}
		sort.Slice(tools, func(i, j int) bool { return tools[i].count > tools[j].count })
		if len(tools) > 5 {
			tools = tools[:5]
		}
		for _, t := range tools {
			avgMs := int64(0)
			if t.count > 0 {
				avgMs = stats.toolLatency[t.tool] / t.count
			}
			entry.TopTools = append(entry.TopTools, ToolUsage{
				Tool: t.tool, Count: t.count, AvgMs: avgMs,
			})
		}
		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].TotalCalls > entries[j].TotalCalls })
	return entries
}

// RegisterDashboardRoutes adds the enhanced API routes to the dashboard handler.
func (d *Dashboard) RegisterDashboardRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/consumer-lag", d.handleConsumerLag)
	mux.HandleFunc("/api/v1/agent-activity", d.handleAgentActivity)
	mux.HandleFunc("/api/v1/flow-rates", d.handleFlowRates)
	mux.HandleFunc("/api/v1/dashboard-summary", d.handleDashboardSummary)
}

func (d *Dashboard) handleConsumerLag(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	groups, err := d.kafkaManager.ListConsumerGroups(ctx, "")
	if err != nil {
		d.logger.Error("failed to list consumer groups", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	snapshot := ConsumerLagSnapshot{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	for _, g := range groups {
		groupLag := ConsumerGroupLag{
			GroupID:  g.GroupID,
			State:   g.State,
			TotalLag: g.TotalLag,
		}
		snapshot.TotalLag += g.TotalLag
		snapshot.Groups = append(snapshot.Groups, groupLag)
	}

	w.Header().Set("X-Frame-Options", "DENY")
	writeJSON(w, snapshot)
}

func (d *Dashboard) handleAgentActivity(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	entries := d.auditLogger.Recent(500)
	agentMap := make(map[string]*AgentActivityEntry)

	for _, e := range entries {
		agent, ok := agentMap[e.AgentID]
		if !ok {
			agent = &AgentActivityEntry{
				AgentID:      e.AgentID,
				ActiveSince:  e.Timestamp,
				LastActivity: e.Timestamp,
			}
			agentMap[e.AgentID] = agent
		}
		agent.TotalCalls++
		agent.LastActivity = e.Timestamp
		if e.Result.Status == "error" {
			agent.ErrorRate++
		}
	}

	// Normalize error rates
	var result []AgentActivityEntry
	for _, a := range agentMap {
		if a.TotalCalls > 0 {
			a.ErrorRate = a.ErrorRate / float64(a.TotalCalls)
		}
		result = append(result, *a)
	}

	writeJSON(w, map[string]any{"agents": result, "total": len(result)})
}

func (d *Dashboard) handleFlowRates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	topics, err := d.kafkaManager.ListTopics(ctx, "", false)
	if err != nil {
		d.logger.Error("failed to list topics", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	var rates []MessageFlowRate
	for _, t := range topics {
		rate := MessageFlowRate{
			Topic:       t.Name,
			LastUpdated: time.Now().UTC().Format(time.RFC3339),
		}
		rates = append(rates, rate)
	}

	writeJSON(w, map[string]any{"flow_rates": rates, "total": len(rates)})
}

func (d *Dashboard) handleDashboardSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("X-Frame-Options", "DENY")

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	summary := map[string]any{
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
		"connected_clients": d.ConnectedClients(),
	}

	if topics, err := d.kafkaManager.ListTopics(ctx, "", false); err == nil {
		summary["topic_count"] = len(topics)
	}

	if groups, err := d.kafkaManager.ListConsumerGroups(ctx, ""); err == nil {
		summary["consumer_group_count"] = len(groups)
		var totalLag int64
		for _, g := range groups {
			totalLag += g.TotalLag
		}
		summary["total_consumer_lag"] = totalLag
	}

	auditEntries := d.auditLogger.Recent(100)
	summary["recent_activity_count"] = len(auditEntries)

	if err := d.kafkaManager.Ping(ctx); err != nil {
		summary["cluster_status"] = "unhealthy"
	} else {
		summary["cluster_status"] = "healthy"
	}

	writeJSON(w, summary)
}

// ListTopicsFunc provides topic listing for flow rate calculation.
type ListTopicsFunc func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error)
