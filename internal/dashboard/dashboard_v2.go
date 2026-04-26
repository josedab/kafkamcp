package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
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

func (d *DashboardV2) handleTopologyV2(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	cluster := r.URL.Query().Get("cluster")
	if validateQueryParam(w, "cluster", cluster) {
		return
	}

	resp := TopologyResponse{
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	topics, err := d.kafkaManager.ListTopics(ctx, cluster, false)
	if err != nil {
		d.logger.Error("failed to list topics for topology", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	for _, t := range topics {
		resp.Nodes = append(resp.Nodes, TopologyNode{
			ID:   "topic:" + t.Name,
			Type: "topic",
			Name: t.Name,
			Properties: map[string]any{
				"partitions":        t.Partitions,
				"replication_factor": t.ReplicationFactor,
				"message_count":     t.MessageCount,
			},
		})
	}

	groups, err := d.kafkaManager.ListConsumerGroups(ctx, cluster)
	if err == nil {
		for _, g := range groups {
			nodeID := "group:" + g.GroupID
			resp.Nodes = append(resp.Nodes, TopologyNode{
				ID:   nodeID,
				Type: "consumer_group",
				Name: g.GroupID,
				Properties: map[string]any{
					"state":    g.State,
					"members":  g.MemberCount,
					"total_lag": g.TotalLag,
				},
			})

			detail, err := d.kafkaManager.DescribeConsumerGroup(ctx, g.GroupID, cluster)
			if err == nil {
				for _, offset := range detail.Offsets {
					resp.Edges = append(resp.Edges, TopologyEdge{
						Source: "topic:" + offset.Topic,
						Target: nodeID,
						Type:   "consumes",
					})
				}
			}
		}
	}

	// Cluster info
	meta, err := d.kafkaManager.GetClusterMetadata(ctx, cluster)
	if err == nil {
		resp.Clusters = append(resp.Clusters, ClusterInfo{
			Name:   meta.ClusterID,
			Brokers: len(meta.Brokers),
			Topics: len(topics),
			Groups: len(groups),
			Status: "healthy",
		})
	}

	writeJSON(w, resp)
}

func (d *DashboardV2) handleHeatmap(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hoursStr := r.URL.Query().Get("hours")
	hours := 24
	if hoursStr != "" {
		if h, err := strconv.Atoi(hoursStr); err == nil && h > 0 && h <= 168 {
			hours = h
		}
	}

	bucketMin := 60
	if hours <= 6 {
		bucketMin = 15
	} else if hours <= 12 {
		bucketMin = 30
	}

	entries := d.auditLogger.Recent(10000)

	cutoff := time.Now().Add(-time.Duration(hours) * time.Hour)

	type agentData struct {
		buckets map[string]int64
		errors  map[string]int64
		tools   map[string]int64
		total   int64
		errTot  int64
	}

	agents := make(map[string]*agentData)

	for _, e := range entries {
		ts, err := time.Parse(time.RFC3339, e.Timestamp)
		if err != nil || ts.Before(cutoff) {
			continue
		}

		ad, ok := agents[e.AgentID]
		if !ok {
			ad = &agentData{
				buckets: make(map[string]int64),
				errors:  make(map[string]int64),
				tools:   make(map[string]int64),
			}
			agents[e.AgentID] = ad
		}

		bucketTime := ts.Truncate(time.Duration(bucketMin) * time.Minute)
		bucketKey := bucketTime.UTC().Format(time.RFC3339)
		ad.buckets[bucketKey]++
		ad.tools[e.Tool]++
		ad.total++
		if e.Result.Status == "error" {
			ad.errors[bucketKey]++
			ad.errTot++
		}
	}

	resp := HeatmapResponse{
		TimeRange: fmt.Sprintf("%dh", hours),
		BucketMin: bucketMin,
	}

	for agentID, ad := range agents {
		entry := AgentHeatmapEntry{
			AgentID:    agentID,
			TotalCalls: ad.total,
		}
		if ad.total > 0 {
			entry.ErrorRate = float64(ad.errTot) / float64(ad.total)
		}

		// Sorted buckets
		var bucketKeys []string
		for k := range ad.buckets {
			bucketKeys = append(bucketKeys, k)
		}
		sort.Strings(bucketKeys)
		for _, k := range bucketKeys {
			entry.Buckets = append(entry.Buckets, HeatmapBucket{
				Time:   k,
				Count:  ad.buckets[k],
				Errors: ad.errors[k],
			})
		}

		// Top tools
		type kv struct {
			k string
			v int64
		}
		var toolList []kv
		for t, c := range ad.tools {
			toolList = append(toolList, kv{t, c})
		}
		sort.Slice(toolList, func(i, j int) bool { return toolList[i].v > toolList[j].v })
		if len(toolList) > 10 {
			toolList = toolList[:10]
		}
		for _, t := range toolList {
			entry.TopTools = append(entry.TopTools, ToolHeatEntry{Tool: t.k, Count: t.v})
		}

		resp.Agents = append(resp.Agents, entry)
	}

	sort.Slice(resp.Agents, func(i, j int) bool { return resp.Agents[i].TotalCalls > resp.Agents[j].TotalCalls })

	writeJSON(w, resp)
}

func (d *DashboardV2) handleMessageTail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic parameter required", http.StatusBadRequest)
		return
	}
	if validateQueryParam(w, "topic", topic) {
		return
	}

	cluster := r.URL.Query().Get("cluster")
	if validateQueryParam(w, "cluster", cluster) {
		return
	}
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 200 {
			limit = l
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	messages, err := d.kafkaManager.Consume(ctx, cluster, topic, -1, "latest", limit, 5000, "")
	if err != nil {
		d.logger.Error("failed to tail messages", "error", err)
		http.Error(w, "failed to tail messages", http.StatusInternalServerError)
		return
	}

	type tailMessage struct {
		Partition int32             `json:"partition"`
		Offset    int64             `json:"offset"`
		Key       string            `json:"key"`
		Value     string            `json:"value"`
		Headers   map[string]string `json:"headers,omitempty"`
		Timestamp string            `json:"timestamp"`
	}

	var result []tailMessage
	for _, m := range messages {
		tm := tailMessage{
			Partition: m.Partition,
			Offset:    m.Offset,
			Key:       fmt.Sprintf("%v", m.Key),
			Timestamp: m.Timestamp,
		}
		valStr := fmt.Sprintf("%v", m.Value)
		if len(valStr) > 2048 {
			tm.Value = valStr[:2048] + "...(truncated)"
		} else {
			tm.Value = valStr
		}
		if len(m.Headers) > 0 {
			tm.Headers = m.Headers
		}
		result = append(result, tm)
	}

	writeJSON(w, map[string]any{
		"topic":    topic,
		"messages": result,
		"count":    len(result),
	})
}

func (d *DashboardV2) handleListTailers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	d.mu.RLock()
	defer d.mu.RUnlock()

	var tailers []MessageTailer
	for _, t := range d.tailers {
		tailers = append(tailers, *t)
	}

	writeJSON(w, map[string]any{"tailers": tailers, "count": len(tailers)})
}

func (d *DashboardV2) handleClusterOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	cluster := r.URL.Query().Get("cluster")
	if validateQueryParam(w, "cluster", cluster) {
		return
	}

	overview := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	if meta, err := d.kafkaManager.GetClusterMetadata(ctx, cluster); err == nil {
		overview["broker_count"] = len(meta.Brokers)
	}

	if topics, err := d.kafkaManager.ListTopics(ctx, cluster, true); err == nil {
		var totalPartitions int
		var totalMessages int64
		var internalCount int
		for _, t := range topics {
			totalPartitions += t.Partitions
			totalMessages += t.MessageCount
			if t.IsInternal {
				internalCount++
			}
		}
		overview["topic_count"] = len(topics)
		overview["internal_topics"] = internalCount
		overview["total_partitions"] = totalPartitions
		overview["total_messages_estimate"] = totalMessages
	}

	if groups, err := d.kafkaManager.ListConsumerGroups(ctx, cluster); err == nil {
		var totalLag int64
		stateCount := make(map[string]int)
		for _, g := range groups {
			totalLag += g.TotalLag
			stateCount[g.State]++
		}
		overview["consumer_group_count"] = len(groups)
		overview["total_consumer_lag"] = totalLag
		overview["group_states"] = stateCount
	}

	if err := d.kafkaManager.Ping(ctx); err != nil {
		overview["status"] = "unhealthy"
	} else {
		overview["status"] = "healthy"
	}

	writeJSON(w, overview)
}

func (d *DashboardV2) handleConsumerGroupsDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	cluster := r.URL.Query().Get("cluster")
	groupID := r.URL.Query().Get("group_id")
	if validateQueryParam(w, "cluster", cluster) {
		return
	}
	if validateQueryParam(w, "group_id", groupID) {
		return
	}

	if groupID != "" {
		detail, err := d.kafkaManager.DescribeConsumerGroup(ctx, groupID, cluster)
		if err != nil {
			d.logger.Error("failed to describe consumer group", "error", err)
			http.Error(w, "consumer group not found", http.StatusNotFound)
			return
		}
		writeJSON(w, detail)
		return
	}

	groups, err := d.kafkaManager.ListConsumerGroups(ctx, cluster)
	if err != nil {
		d.logger.Error("failed to list consumer groups", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{
		"consumer_groups": groups,
		"total":           len(groups),
	})
}

func (d *DashboardV2) handlePartitionView(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic parameter required", http.StatusBadRequest)
		return
	}
	if validateQueryParam(w, "topic", topic) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	cluster := r.URL.Query().Get("cluster")
	if validateQueryParam(w, "cluster", cluster) {
		return
	}
	detail, err := d.kafkaManager.DescribeTopic(ctx, topic, cluster)
	if err != nil {
		d.logger.Error("failed to describe topic", "error", err)
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}

	type partitionView struct {
		ID       int32   `json:"id"`
		Leader   int32   `json:"leader"`
		Replicas []int32 `json:"replicas"`
		ISR      []int32 `json:"isr"`
		Messages int64   `json:"messages"`
		InSync   bool    `json:"in_sync"`
	}

	var partitions []partitionView
	for _, p := range detail.Partitions {
		pv := partitionView{
			ID:       p.ID,
			Leader:   p.Leader,
			Replicas: p.Replicas,
			ISR:      p.ISR,
			Messages: p.LatestOffset - p.EarliestOffset,
			InSync:   len(p.ISR) == len(p.Replicas),
		}
		partitions = append(partitions, pv)
	}

	writeJSON(w, map[string]any{
		"topic":      topic,
		"partitions": partitions,
		"configs":    detail.Configs,
		"total":      len(partitions),
	})
}

// RecordAgentCall updates the heatmap with agent activity data.
func (d *DashboardV2) RecordAgentCall(agentID, tool string, isError bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	hd, ok := d.heatmap[agentID]
	if !ok {
		hd = &AgentHeatmapData{
			AgentID:   agentID,
			Buckets:   make(map[string]int64),
			ToolCalls: make(map[string]int64),
		}
		d.heatmap[agentID] = hd
	}

	bucket := time.Now().Truncate(15 * time.Minute).UTC().Format(time.RFC3339)
	hd.Buckets[bucket]++
	hd.ToolCalls[tool]++
	hd.LastSeen = time.Now()
}

// marshalJSON is a helper to serialize to JSON with indentation.
func marshalJSON(v any) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

// handleSSETail provides a Server-Sent Events stream for live message tailing.
func (d *DashboardV2) HandleSSETail(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic parameter required", http.StatusBadRequest)
		return
	}
	cluster := r.URL.Query().Get("cluster")

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ctx := r.Context()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var lastOffset int64 = -1

	fmt.Fprintf(w, "event: connected\ndata: {\"topic\":%q}\n\n", topic)
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tailCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			messages, err := d.kafkaManager.Consume(tailCtx, cluster, topic, -1, "latest", 10, 3000, "")
			cancel()
			if err != nil {
				continue
			}
			for _, m := range messages {
				if m.Offset <= lastOffset {
					continue
				}
				lastOffset = m.Offset

				valStr := fmt.Sprintf("%v", m.Value)
				payload := map[string]any{
					"partition": m.Partition,
					"offset":    m.Offset,
					"key":       fmt.Sprintf("%v", m.Key),
					"timestamp": m.Timestamp,
				}
				if len(valStr) > 1024 {
					payload["value"] = valStr[:1024] + "..."
				} else {
					payload["value"] = valStr
				}

				data, err := json.Marshal(payload)
				if err != nil {
					d.logger.Error("marshaling SSE payload", "error", err)
					continue
				}
				fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
				flusher.Flush()
			}
		}
	}
}

// UpdateIndexHTML returns the enhanced dashboard HTML with v2 features.
// This replaces the contents that would go in static/index.html
// The actual SPA is embedded via //go:embed and served by the base dashboard.
// V2 endpoints are registered separately on the same mux.

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
