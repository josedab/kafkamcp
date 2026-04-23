package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
)

// ListConsumerGroups lists all consumer groups in the cluster.
func (m *Manager) ListConsumerGroups(ctx context.Context, cluster string) ([]ConsumerGroupSummary, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	groups, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing consumer groups: %w", err)
	}

	var result []ConsumerGroupSummary
	for _, g := range groups.Sorted() {
		result = append(result, ConsumerGroupSummary{
			GroupID:     g.Group,
			State:       g.State,
			Protocol:    g.ProtocolType,
		})
	}

	// Try to enrich with lag info and member count
	if len(result) > 0 {
		groupIDs := make([]string, len(result))
		for i, g := range result {
			groupIDs[i] = g.GroupID
		}
		described, err := admin.DescribeGroups(ctx, groupIDs...)
		if err == nil {
			for i := range result {
				if dg, ok := described[result[i].GroupID]; ok {
					result[i].State = dg.State
					result[i].MemberCount = len(dg.Members)
				}
			}
		} else {
			m.logger.Warn("failed to describe consumer groups for enrichment", "error", err)
		}
		lags, err := admin.Lag(ctx, groupIDs...)
		if err == nil {
			lags.Each(func(l kadm.DescribedGroupLag) {
				for i := range result {
					if result[i].GroupID == l.Group {
						result[i].TotalLag = l.Lag.Total()
						break
					}
				}
			})
		} else {
			m.logger.Warn("failed to fetch consumer group lag for enrichment", "error", err)
		}
	}

	return result, nil
}

// DescribeConsumerGroup returns detailed consumer group info.
func (m *Manager) DescribeConsumerGroup(ctx context.Context, cluster, groupID string) (*ConsumerGroupDetail, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	described, err := admin.DescribeGroups(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("describing consumer group: %w", err)
	}

	dg, ok := described[groupID]
	if !ok {
		return nil, fmt.Errorf("consumer group %q not found", groupID)
	}
	if dg.Err != nil {
		return nil, fmt.Errorf("describing consumer group %q: %w", groupID, dg.Err)
	}

	detail := &ConsumerGroupDetail{
		GroupID:      groupID,
		State:        dg.State,
		ProtocolType: dg.ProtocolType,
		Protocol:     dg.Protocol,
		Coordinator: BrokerInfo{
			ID:   int32(dg.Coordinator.NodeID),
			Host: dg.Coordinator.Host,
			Port: dg.Coordinator.Port,
		},
	}

	// Members
	for _, member := range dg.Members {
		md := GroupMemberDetail{
			MemberID: member.MemberID,
			ClientID: member.ClientID,
			Host:     member.ClientHost,
		}
		if consumerAssign, ok := member.Assigned.AsConsumer(); ok {
			for _, ta := range consumerAssign.Topics {
				md.Assignment = append(md.Assignment, TopicPartitions{
					Topic:      ta.Topic,
					Partitions: ta.Partitions,
				})
			}
		}
		detail.Members = append(detail.Members, md)
	}

	// Offsets and lag
	lags, err := admin.Lag(ctx, groupID)
	if err == nil {
		if gl, ok := lags[groupID]; ok {
			for _, memberLag := range gl.Lag.Sorted() {
				detail.Offsets = append(detail.Offsets, GroupOffsetDetail{
					Topic:         memberLag.Topic,
					Partition:     memberLag.Partition,
					CurrentOffset: memberLag.Commit.At,
					LogEndOffset:  memberLag.End.Offset,
					Lag:           memberLag.Lag,
				})
			}
			detail.TotalLag = gl.Lag.Total()
		}
	}

	return detail, nil
}
