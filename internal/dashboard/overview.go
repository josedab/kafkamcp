package dashboard

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

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
				"partitions":         t.Partitions,
				"replication_factor": t.ReplicationFactor,
				"message_count":      t.MessageCount,
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
					"state":     g.State,
					"members":   g.MemberCount,
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
			Name:    meta.ClusterID,
			Brokers: len(meta.Brokers),
			Topics:  len(topics),
			Groups:  len(groups),
			Status:  "healthy",
		})
	}

	writeJSON(w, resp)
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
