package kafka

import (
	"context"
	"fmt"
)

// GetClusterMetadata returns cluster-level metadata.
func (m *Manager) GetClusterMetadata(ctx context.Context, cluster string) (*ClusterMetadata, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	meta, err := admin.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting metadata: %w", err)
	}

	var brokers []BrokerInfo
	for _, b := range meta.Brokers {
		bi := BrokerInfo{
			ID:   b.NodeID,
			Host: b.Host,
			Port: b.Port,
		}
		if b.Rack != nil {
			bi.Rack = *b.Rack
		}
		brokers = append(brokers, bi)
	}

	var controller BrokerInfo
	for _, b := range meta.Brokers {
		if b.NodeID == meta.Controller {
			controller = BrokerInfo{
				ID:   b.NodeID,
				Host: b.Host,
				Port: b.Port,
			}
			if b.Rack != nil {
				controller.Rack = *b.Rack
			}
			break
		}
	}

	totalPartitions := 0
	for _, t := range meta.Topics {
		totalPartitions += len(t.Partitions)
	}

	// KRaft mode: cluster ID is non-empty in KRaft clusters
	kraftMode := meta.Cluster != ""

	return &ClusterMetadata{
		ClusterID:      meta.Cluster,
		Controller:     controller,
		Brokers:        brokers,
		BrokerCount:    len(brokers),
		TopicCount:     len(meta.Topics),
		PartitionCount: totalPartitions,
		KraftMode:      kraftMode,
	}, nil
}
