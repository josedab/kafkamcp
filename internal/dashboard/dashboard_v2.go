package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// DashboardV2 extends the base dashboard with advanced visualization APIs.
type DashboardV2 struct {
	kafkaManager kafka.KafkaManager
	auditLogger  *audit.Logger
	logger       interface{ Error(string, ...any) }

	mu       sync.RWMutex
	heatmap  map[string]*AgentHeatmapData
	tailers  map[string]*MessageTailer
	tailerID int
}

// AgentHeatmapData tracks per-agent tool call frequency across time buckets.
type AgentHeatmapData struct {
	AgentID   string             `json:"agent_id"`
	Buckets   map[string]int64   `json:"buckets"`
	ToolCalls map[string]int64   `json:"tool_calls"`
	LastSeen  time.Time          `json:"last_seen"`
}

// HeatmapResponse is returned by the heatmap endpoint.
type HeatmapResponse struct {
	Agents    []AgentHeatmapEntry `json:"agents"`
	TimeRange string              `json:"time_range"`
	BucketMin int                 `json:"bucket_size_minutes"`
}

// AgentHeatmapEntry is a single agent's heatmap data.
type AgentHeatmapEntry struct {
	AgentID    string           `json:"agent_id"`
	TotalCalls int64            `json:"total_calls"`
	ErrorRate  float64          `json:"error_rate"`
	Buckets    []HeatmapBucket  `json:"buckets"`
	TopTools   []ToolHeatEntry  `json:"top_tools"`
}

// HeatmapBucket represents activity in a time window.
type HeatmapBucket struct {
	Time     string `json:"time"`
	Count    int64  `json:"count"`
	Errors   int64  `json:"errors"`
}

// ToolHeatEntry tracks per-tool usage in the heatmap.
type ToolHeatEntry struct {
	Tool  string `json:"tool"`
	Count int64  `json:"count"`
}

// MessageTailer maintains state for live message tailing.
type MessageTailer struct {
	ID        string    `json:"id"`
	Topic     string    `json:"topic"`
	Cluster   string    `json:"cluster"`
	StartedAt time.Time `json:"started_at"`
	Count     int64     `json:"messages_received"`
	cancel    context.CancelFunc
}

// TopologyNode represents a node in the topology graph.
type TopologyNode struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	Name       string            `json:"name"`
	Properties map[string]any    `json:"properties,omitempty"`
}

// TopologyEdge represents a connection in the topology graph.
type TopologyEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"`
}

// TopologyResponse is the full topology visualization payload.
type TopologyResponse struct {
	Nodes      []TopologyNode `json:"nodes"`
	Edges      []TopologyEdge `json:"edges"`
	Clusters   []ClusterInfo  `json:"clusters"`
	UpdatedAt  string         `json:"updated_at"`
}

// ClusterInfo holds cluster summary data for the dashboard.
type ClusterInfo struct {
	Name       string `json:"name"`
	Brokers    int    `json:"brokers"`
	Topics     int    `json:"topics"`
	Groups     int    `json:"consumer_groups"`
	Status     string `json:"status"`
}

// NewDashboardV2 creates an enhanced dashboard.
func NewDashboardV2(km kafka.KafkaManager, al *audit.Logger, logger interface{ Error(string, ...any) }) *DashboardV2 {
	return &DashboardV2{
		kafkaManager: km,
		auditLogger:  al,
		logger:       logger,
		heatmap:      make(map[string]*AgentHeatmapData),
		tailers:      make(map[string]*MessageTailer),
	}
}

// RegisterRoutes adds v2 dashboard API routes to the given mux.
func (d *DashboardV2) RegisterRoutes(mux *http.ServeMux, authMiddleware func(http.HandlerFunc) http.HandlerFunc) {
	wrap := authMiddleware
	if wrap == nil {
		wrap = func(h http.HandlerFunc) http.HandlerFunc { return h }
	}

	mux.HandleFunc("/api/v2/topology", wrap(d.handleTopologyV2))
	mux.HandleFunc("/api/v2/heatmap", wrap(d.handleHeatmap))
	mux.HandleFunc("/api/v2/messages/tail", wrap(d.handleMessageTail))
	mux.HandleFunc("/api/v2/messages/tailers", wrap(d.handleListTailers))
	mux.HandleFunc("/api/v2/cluster/overview", wrap(d.handleClusterOverview))
	mux.HandleFunc("/api/v2/consumer-groups/detail", wrap(d.handleConsumerGroupsDetail))
	mux.HandleFunc("/api/v2/partitions", wrap(d.handlePartitionView))
}

// marshalJSON is a helper to serialize to JSON with indentation.
func marshalJSON(v any) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

const maxQueryParamLength = 1024

// validateQueryParam checks that a query parameter doesn't exceed the maximum length.
// Returns true if the value is too long (and writes an error response).
func validateQueryParam(w http.ResponseWriter, name, value string) bool {
	if len(value) > maxQueryParamLength {
		http.Error(w, fmt.Sprintf("%s exceeds maximum length", name), http.StatusBadRequest)
		return true
	}
	return false
}
