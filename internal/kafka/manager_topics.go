package kafka

import (
	"context"
	"fmt"
)

// ListTopics lists topics in the cluster, optionally filtering by pattern.
func (m *Manager) ListTopics(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	topics, err := admin.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}

	startOffsets, err := admin.ListStartOffsets(ctx)
	if err != nil {
		m.logger.Warn("failed to list start offsets", "error", err)
	}
	endOffsets, err := admin.ListEndOffsets(ctx)
	if err != nil {
		m.logger.Warn("failed to list end offsets", "error", err)
	}

	var result []TopicSummary
	for _, t := range topics.Sorted() {
		if !includeInternal && t.IsInternal {
			continue
		}

		var msgCount int64
		if startOffsets != nil && endOffsets != nil {
			for _, p := range t.Partitions.Sorted() {
				so, ok1 := startOffsets.Lookup(t.Topic, p.Partition)
				eo, ok2 := endOffsets.Lookup(t.Topic, p.Partition)
				if ok1 && ok2 {
					msgCount += eo.Offset - so.Offset
				}
			}
		}

		replFactor := 0
		if len(t.Partitions) > 0 {
			replFactor = len(t.Partitions.Sorted()[0].Replicas)
		}

		result = append(result, TopicSummary{
			Name:              t.Topic,
			Partitions:        len(t.Partitions),
			ReplicationFactor: replFactor,
			IsInternal:        t.IsInternal,
			MessageCount:      msgCount,
		})
	}

	return result, nil
}

// DescribeTopic returns detailed topic metadata.
func (m *Manager) DescribeTopic(ctx context.Context, cluster, topicName string) (*TopicDetail, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	topics, err := admin.ListTopics(ctx, topicName)
	if err != nil {
		return nil, fmt.Errorf("describing topic: %w", err)
	}

	t, ok := topics[topicName]
	if !ok {
		return nil, fmt.Errorf("topic %q not found", topicName)
	}

	startOffsets, err := admin.ListStartOffsets(ctx, topicName)
	if err != nil {
		m.logger.Warn("failed to list start offsets", "topic", topicName, "error", err)
	}
	endOffsets, err := admin.ListEndOffsets(ctx, topicName)
	if err != nil {
		m.logger.Warn("failed to list end offsets", "topic", topicName, "error", err)
	}

	// Fetch topic configs via admin API
	configs := make(map[string]string)
	topicConfigs, err := admin.DescribeTopicConfigs(ctx, topicName)
	if err == nil {
		for _, rc := range topicConfigs {
			if rc.Err == nil {
				for _, entry := range rc.Configs {
					if entry.Value != nil {
						configs[entry.Key] = *entry.Value
					}
				}
			}
		}
	}

	replFactor := 0
	var partitions []PartitionDetail
	var totalMessages int64

	for _, p := range t.Partitions.Sorted() {
		if replFactor == 0 {
			replFactor = len(p.Replicas)
		}
		earliest := int64(0)
		latest := int64(0)
		if startOffsets != nil {
			if so, ok := startOffsets.Lookup(topicName, p.Partition); ok {
				earliest = so.Offset
			}
		}
		if endOffsets != nil {
			if eo, ok := endOffsets.Lookup(topicName, p.Partition); ok {
				latest = eo.Offset
			}
		}
		totalMessages += latest - earliest

		partitions = append(partitions, PartitionDetail{
			ID:             p.Partition,
			Leader:         p.Leader,
			Replicas:       p.Replicas,
			ISR:            p.ISR,
			EarliestOffset: earliest,
			LatestOffset:   latest,
		})
	}

	return &TopicDetail{
		Name:              topicName,
		Partitions:        partitions,
		ReplicationFactor: replFactor,
		Configs:           configs,
		IsInternal:        t.IsInternal,
		MessageCount:      totalMessages,
	}, nil
}
