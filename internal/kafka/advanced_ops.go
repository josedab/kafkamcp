package kafka

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// TimeTravelSnapshot represents a point-in-time state of a compacted topic.
type TimeTravelSnapshot struct {
	Topic      string            `json:"topic"`
	Timestamp  string            `json:"timestamp"`
	KeyCount   int               `json:"key_count"`
	State      map[string]any    `json:"state"` // key -> last value before timestamp
	ScanInfo   TimeTravelScanInfo `json:"scan_info"`
}

// TimeTravelScanInfo provides metadata about the scan operation.
type TimeTravelScanInfo struct {
	MessagesScanned int    `json:"messages_scanned"`
	Duration        string `json:"duration_ms"`
}

// TimeTravelDiffResult represents changes between two timestamps.
type TimeTravelDiffResult struct {
	Topic        string              `json:"topic"`
	From         string              `json:"timestamp_from"`
	To           string              `json:"timestamp_to"`
	TotalChanges int                 `json:"total_changes"`
	Added        map[string]any      `json:"added"`   // keys that appeared
	Modified     []TimeTravelModified `json:"modified"` // keys that changed value
	Deleted      []string            `json:"deleted"`  // keys that disappeared
}

// TimeTravelModified represents a modified key between two snapshots.
type TimeTravelModified struct {
	Key      string `json:"key"`
	OldValue any    `json:"old_value"`
	NewValue any    `json:"new_value"`
}

// TimeTravelSnapshot reconstructs topic state at a point in time.
func (m *Manager) TimeTravelSnapshot(ctx context.Context, cluster, topic, timestamp string, maxKeys int) (*TimeTravelSnapshot, error) {
	ts, err := parseTimestamp(timestamp)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}

	startTime := time.Now()

	// Consume from earliest up to the given timestamp
	msgs, err := m.Consume(ctx, cluster, topic, -1, "earliest", maxKeys*2, 30000, "auto")
	if err != nil {
		return nil, fmt.Errorf("consuming for snapshot: %w", err)
	}

	// Build state: last value per key before timestamp
	state := make(map[string]any)
	scanned := 0
	for _, msg := range msgs {
		msgTime, parseErr := time.Parse("2006-01-02T15:04:05.000Z", msg.Timestamp)
		if parseErr != nil {
			continue
		}
		if msgTime.After(ts) {
			break
		}
		scanned++
		key := fmt.Sprintf("%v", msg.Key)
		if key == "" || key == "<nil>" {
			continue
		}
		if msg.Value == nil {
			delete(state, key) // tombstone
		} else {
			state[key] = msg.Value
		}
		if len(state) >= maxKeys {
			break
		}
	}

	return &TimeTravelSnapshot{
		Topic:     topic,
		Timestamp: timestamp,
		KeyCount:  len(state),
		State:     state,
		ScanInfo: TimeTravelScanInfo{
			MessagesScanned: scanned,
			Duration:        fmt.Sprintf("%d", time.Since(startTime).Milliseconds()),
		},
	}, nil
}

// TimeTravelDiff computes the difference between topic state at two timestamps.
func (m *Manager) TimeTravelDiff(ctx context.Context, cluster, topic, tsFrom, tsTo string, maxKeys int) (*TimeTravelDiffResult, error) {
	fromSnapshot, err := m.TimeTravelSnapshot(ctx, cluster, topic, tsFrom, maxKeys)
	if err != nil {
		return nil, fmt.Errorf("building from-snapshot: %w", err)
	}

	toSnapshot, err := m.TimeTravelSnapshot(ctx, cluster, topic, tsTo, maxKeys)
	if err != nil {
		return nil, fmt.Errorf("building to-snapshot: %w", err)
	}

	result := &TimeTravelDiffResult{
		Topic: topic,
		From:  tsFrom,
		To:    tsTo,
		Added: make(map[string]any),
	}

	// Find added and modified
	for key, newVal := range toSnapshot.State {
		if oldVal, exists := fromSnapshot.State[key]; exists {
			if fmt.Sprintf("%v", oldVal) != fmt.Sprintf("%v", newVal) {
				result.Modified = append(result.Modified, TimeTravelModified{
					Key: key, OldValue: oldVal, NewValue: newVal,
				})
			}
		} else {
			result.Added[key] = newVal
		}
	}

	// Find deleted
	for key := range fromSnapshot.State {
		if _, exists := toSnapshot.State[key]; !exists {
			result.Deleted = append(result.Deleted, key)
		}
	}

	result.TotalChanges = len(result.Added) + len(result.Modified) + len(result.Deleted)
	return result, nil
}

// Recommendation represents an actionable insight about a topic or cluster.
type Recommendation struct {
	Type       string  `json:"type"`       // partition_hotspot, idle_topic, under_replicated, lag_trend, retention, compression
	Severity   string  `json:"severity"`   // critical, warning, info
	Topic      string  `json:"topic,omitempty"`
	Message    string  `json:"message"`
	Suggestion string  `json:"suggestion"`
	Confidence float64 `json:"confidence"` // 0.0 - 1.0
}

// RecommendationResult contains all recommendations.
type RecommendationResult struct {
	Recommendations []Recommendation `json:"recommendations"`
	TopicsAnalyzed  int              `json:"topics_analyzed"`
	Cluster         string           `json:"cluster"`
}

// GetRecommendations analyzes topics and provides optimization suggestions.
func (m *Manager) GetRecommendations(ctx context.Context, cluster, specificTopic string) (*RecommendationResult, error) {
	topics, err := m.ListTopics(ctx, cluster, false)
	if err != nil {
		return nil, err
	}

	if specificTopic != "" {
		var filtered []TopicSummary
		for _, t := range topics {
			if t.Name == specificTopic {
				filtered = append(filtered, t)
			}
		}
		topics = filtered
	}

	var recommendations []Recommendation

	for _, topic := range topics {
		detail, err := m.DescribeTopic(ctx, cluster, topic.Name)
		if err != nil {
			continue
		}

		// Check under-replicated partitions
		for _, p := range detail.Partitions {
			if len(p.ISR) < len(p.Replicas) {
				recommendations = append(recommendations, Recommendation{
					Type:       "under_replicated",
					Severity:   "critical",
					Topic:      topic.Name,
					Message:    fmt.Sprintf("Partition %d has %d ISR out of %d replicas", p.ID, len(p.ISR), len(p.Replicas)),
					Suggestion: "Check broker health. Under-replicated partitions risk data loss.",
					Confidence: 0.95,
				})
			}
		}

		// Check idle topics (zero messages)
		if topic.MessageCount == 0 {
			recommendations = append(recommendations, Recommendation{
				Type:       "idle_topic",
				Severity:   "info",
				Topic:      topic.Name,
				Message:    "Topic has zero messages",
				Suggestion: "Consider removing if unused, or verify producers are functioning.",
				Confidence: 0.8,
			})
		}

		// Partition hotspot detection
		if len(detail.Partitions) > 1 {
			var offsets []int64
			for _, p := range detail.Partitions {
				offsets = append(offsets, p.LatestOffset-p.EarliestOffset)
			}
			sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
			if len(offsets) > 0 && offsets[len(offsets)-1] > 0 {
				ratio := float64(offsets[0]) / float64(offsets[len(offsets)-1])
				if ratio < 0.1 && offsets[len(offsets)-1] > 1000 {
					recommendations = append(recommendations, Recommendation{
						Type:       "partition_hotspot",
						Severity:   "warning",
						Topic:      topic.Name,
						Message:    fmt.Sprintf("Partition skew detected: min/max ratio %.2f", ratio),
						Suggestion: "Consider using a better partitioning key or increasing partition count.",
						Confidence: 0.85,
					})
				}
			}
		}

		// Retention tuning suggestion
		if retMs, ok := detail.Configs["retention.ms"]; ok {
			if retMs == "-1" {
				recommendations = append(recommendations, Recommendation{
					Type:       "retention",
					Severity:   "info",
					Topic:      topic.Name,
					Message:    "Topic has infinite retention",
					Suggestion: "Set a finite retention period to manage disk usage.",
					Confidence: 0.7,
				})
			}
		}

		// Compression suggestion
		if compression, ok := detail.Configs["compression.type"]; ok {
			if compression == "none" || compression == "producer" {
				recommendations = append(recommendations, Recommendation{
					Type:       "compression",
					Severity:   "info",
					Topic:      topic.Name,
					Message:    fmt.Sprintf("Topic compression is '%s'", compression),
					Suggestion: "Consider enabling lz4 or zstd compression to reduce storage and network usage.",
					Confidence: 0.75,
				})
			}
		}

		// Partition count suggestion
		if topic.MessageCount > 100000 && topic.Partitions == 1 {
			recommendations = append(recommendations, Recommendation{
				Type:       "partition_count",
				Severity:   "warning",
				Topic:      topic.Name,
				Message:    fmt.Sprintf("High-volume topic (%d msgs) with only 1 partition", topic.MessageCount),
				Suggestion: "Increase partition count to improve parallelism and throughput.",
				Confidence: 0.85,
			})
		}
	}

	// Consumer lag trend detection
	groups, err := m.ListConsumerGroups(ctx, cluster)
	if err == nil {
		for _, g := range groups {
			if g.TotalLag > 10000 {
				recommendations = append(recommendations, Recommendation{
					Type:       "lag_trend",
					Severity:   "warning",
					Topic:      "",
					Message:    fmt.Sprintf("Consumer group '%s' has high lag: %d", g.GroupID, g.TotalLag),
					Suggestion: "Scale consumers or investigate processing bottlenecks.",
					Confidence: 0.9,
				})
			}
		}
	}

	return &RecommendationResult{
		Recommendations: recommendations,
		TopicsAnalyzed:  len(topics),
		Cluster:         m.resolveCluster(cluster),
	}, nil
}

func (m *Manager) resolveCluster(cluster string) string {
	if cluster == "" {
		return m.defClust
	}
	return cluster
}
