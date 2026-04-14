package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// CreateTopicRequest defines parameters for creating a topic.
type CreateTopicRequest struct {
	Name              string            `json:"name"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replication_factor"`
	Configs           map[string]string `json:"configs,omitempty"`
}

// CreateTopic creates a new Kafka topic.
func (m *Manager) CreateTopic(ctx context.Context, cluster string, req CreateTopicRequest) error {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return err
	}

	partitions := req.Partitions
	if partitions <= 0 {
		partitions = 1
	}
	replFactor := req.ReplicationFactor
	if replFactor <= 0 {
		replFactor = 1
	}

	var configs []kadm.AlterConfig
	for k, v := range req.Configs {
		configs = append(configs, kadm.AlterConfig{Name: k, Value: &v, Op: kadm.SetConfig})
	}

	resp, err := admin.CreateTopics(ctx, partitions, replFactor, nil, req.Name)
	if err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}
	for _, t := range resp.Sorted() {
		if t.Err != nil {
			return fmt.Errorf("creating topic %q: %w", t.Topic, t.Err)
		}
	}

	// Apply additional configs if any
	if len(configs) > 0 {
		resps, err := admin.AlterTopicConfigs(ctx, configs, req.Name)
		if err != nil {
			m.logger.Warn("failed to set topic configs", "topic", req.Name, "error", err)
		} else {
			for _, r := range resps {
				if r.Err != nil {
					m.logger.Warn("failed to set topic config", "topic", req.Name, "error", r.Err)
				}
			}
		}
	}

	return nil
}

// AlterTopicRequest defines parameters for altering a topic.
type AlterTopicRequest struct {
	Name    string            `json:"name"`
	Configs map[string]string `json:"configs"`
}

// AlterTopic modifies topic configurations.
func (m *Manager) AlterTopic(ctx context.Context, cluster string, req AlterTopicRequest) error {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return err
	}

	var configs []kadm.AlterConfig
	for k, v := range req.Configs {
		val := v
		configs = append(configs, kadm.AlterConfig{Name: k, Value: &val, Op: kadm.SetConfig})
	}

	resps, err := admin.AlterTopicConfigs(ctx, configs, req.Name)
	if err != nil {
		return fmt.Errorf("altering topic configs: %w", err)
	}
	for _, r := range resps {
		if r.Err != nil {
			return fmt.Errorf("altering config for %q: %w", r.Name, r.Err)
		}
	}

	return nil
}

// DeleteTopic deletes a Kafka topic.
func (m *Manager) DeleteTopic(ctx context.Context, cluster, topicName string) error {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return err
	}

	resp, err := admin.DeleteTopics(ctx, topicName)
	if err != nil {
		return fmt.Errorf("deleting topic: %w", err)
	}
	for _, t := range resp.Sorted() {
		if t.Err != nil {
			return fmt.Errorf("deleting topic %q: %w", t.Topic, t.Err)
		}
	}
	return nil
}

// ResetOffsetsRequest defines parameters for resetting consumer group offsets.
type ResetOffsetsRequest struct {
	GroupID   string `json:"group_id"`
	Topic     string `json:"topic"`
	Strategy  string `json:"strategy"` // earliest, latest, timestamp, offset
	Value     string `json:"value,omitempty"`
	DryRun    bool   `json:"dry_run"`
}

// ResetOffsetsResult is the result of an offset reset.
type ResetOffsetsResult struct {
	Offsets []GroupOffsetDetail `json:"offsets"`
	DryRun  bool               `json:"dry_run"`
}

// ResetOffsets resets consumer group offsets.
func (m *Manager) ResetOffsets(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	// Build target offsets based on strategy
	var targetOffsets kadm.Offsets
	switch req.Strategy {
	case "earliest":
		listed, err := admin.ListStartOffsets(ctx, req.Topic)
		if err != nil {
			return nil, fmt.Errorf("listing start offsets: %w", err)
		}
		targetOffsets = listed.Offsets()
	case "latest":
		listed, err := admin.ListEndOffsets(ctx, req.Topic)
		if err != nil {
			return nil, fmt.Errorf("listing end offsets: %w", err)
		}
		targetOffsets = listed.Offsets()
	case "timestamp":
		ts, err := parseTimestamp(req.Value)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp: %w", err)
		}
		listed, err := admin.ListOffsetsAfterMilli(ctx, ts.UnixMilli(), req.Topic)
		if err != nil {
			return nil, fmt.Errorf("listing offsets for timestamp: %w", err)
		}
		targetOffsets = listed.Offsets()
	default:
		return nil, fmt.Errorf("invalid strategy %q; must be: earliest, latest, timestamp", req.Strategy)
	}

	result := &ResetOffsetsResult{DryRun: req.DryRun}
	targetOffsets.Each(func(o kadm.Offset) {
		result.Offsets = append(result.Offsets, GroupOffsetDetail{
			Topic:         o.Topic,
			Partition:     o.Partition,
			CurrentOffset: o.At,
		})
	})

	if req.DryRun {
		return result, nil
	}

	// Commit the offsets
	if err := admin.CommitAllOffsets(ctx, req.GroupID, targetOffsets); err != nil {
		return nil, fmt.Errorf("committing offsets: %w", err)
	}

	return result, nil
}

// SearchMessage represents a message matching search criteria.
type SearchMessage = Message

// SearchRequest defines parameters for searching messages.
type SearchRequest struct {
	Topic          string `json:"topic"`
	KeyFilter      string `json:"key_filter,omitempty"`
	HeaderKey      string `json:"header_key,omitempty"`
	HeaderValue    string `json:"header_value,omitempty"`
	ValueContains  string `json:"value_contains,omitempty"`
	StartTime      string `json:"start_time,omitempty"`
	EndTime        string `json:"end_time,omitempty"`
	MaxScan        int    `json:"max_scan"`
	Limit          int    `json:"limit"`
}

// SearchMessages searches for messages matching the given criteria.
func (m *Manager) SearchMessages(ctx context.Context, cluster string, req SearchRequest) ([]SearchMessage, int, error) {
	if req.MaxScan <= 0 {
		req.MaxScan = 10000
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}

	offset := "earliest"
	if req.StartTime != "" {
		offset = req.StartTime
	}

	var endTime time.Time
	if req.EndTime != "" {
		if t, err := parseTimestamp(req.EndTime); err == nil {
			endTime = t
		}
	}

	// Scale timeout with max_scan: ~100 msgs/sec baseline, min 5s, max 60s
	timeoutMs := req.MaxScan * 10
	if timeoutMs < 5000 {
		timeoutMs = 5000
	}
	if timeoutMs > 60000 {
		timeoutMs = 60000
	}

	msgs, err := m.Consume(ctx, cluster, req.Topic, -1, offset, req.MaxScan, timeoutMs, "auto")
	if err != nil {
		return nil, 0, err
	}

	var matched []SearchMessage
	scanned := 0
	for _, msg := range msgs {
		scanned++
		if len(matched) >= req.Limit {
			break
		}

		// Time range filter
		if !endTime.IsZero() {
			if t, err := time.Parse("2006-01-02T15:04:05.000Z", msg.Timestamp); err == nil {
				if t.After(endTime) {
					continue
				}
			}
		}

		// Key filter
		if req.KeyFilter != "" {
			keyStr := fmt.Sprintf("%v", msg.Key)
			if !strings.Contains(keyStr, req.KeyFilter) {
				continue
			}
		}

		// Header filter
		if req.HeaderKey != "" {
			if msg.Headers == nil {
				continue
			}
			hv, ok := msg.Headers[req.HeaderKey]
			if !ok {
				continue
			}
			if req.HeaderValue != "" && hv != req.HeaderValue {
				continue
			}
		}

		// Value content filter
		if req.ValueContains != "" {
			valStr := fmt.Sprintf("%v", msg.Value)
			if !strings.Contains(valStr, req.ValueContains) {
				continue
			}
		}

		matched = append(matched, msg)
	}

	return matched, scanned, nil
}

// AnalyzeDLQResult is the result of DLQ analysis.
type AnalyzeDLQResult struct {
	Topic         string              `json:"topic"`
	TotalMessages int                 `json:"total_messages"`
	ErrorTypes    map[string]int      `json:"error_types"`
	Samples       []Message           `json:"samples"`
	TimeRange     DLQTimeRange        `json:"time_range"`
}

// DLQTimeRange describes the time range of DLQ messages.
type DLQTimeRange struct {
	Earliest string `json:"earliest"`
	Latest   string `json:"latest"`
}

// AnalyzeDLQ consumes from a DLQ topic, parses error headers, and categorizes failures.
func (m *Manager) AnalyzeDLQ(ctx context.Context, cluster, topic string, limit int) (*AnalyzeDLQResult, error) {
	if limit <= 0 {
		limit = 100
	}

	msgs, err := m.Consume(ctx, cluster, topic, -1, "latest", limit, 10000, "auto")
	if err != nil {
		return nil, err
	}

	result := &AnalyzeDLQResult{
		Topic:         topic,
		TotalMessages: len(msgs),
		ErrorTypes:    make(map[string]int),
	}

	for _, msg := range msgs {
		// Categorize by error headers
		errorType := "unknown"
		if msg.Headers != nil {
			if et, ok := msg.Headers["error.type"]; ok {
				errorType = et
			} else if et, ok := msg.Headers["x-error-type"]; ok {
				errorType = et
			} else if et, ok := msg.Headers["error_type"]; ok {
				errorType = et
			} else if _, ok := msg.Headers["error.message"]; ok {
				errorType = "processing_error"
			} else if _, ok := msg.Headers["x-error-message"]; ok {
				errorType = "processing_error"
			}
		}
		result.ErrorTypes[errorType]++

		if result.TimeRange.Earliest == "" || msg.Timestamp < result.TimeRange.Earliest {
			result.TimeRange.Earliest = msg.Timestamp
		}
		if result.TimeRange.Latest == "" || msg.Timestamp > result.TimeRange.Latest {
			result.TimeRange.Latest = msg.Timestamp
		}
	}

	// Include a few sample messages (max 5)
	sampleCount := 5
	if len(msgs) < sampleCount {
		sampleCount = len(msgs)
	}
	result.Samples = msgs[:sampleCount]

	return result, nil
}
