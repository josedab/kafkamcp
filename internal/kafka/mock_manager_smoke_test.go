package kafka

import (
	"context"
	"testing"
)

// TestMockKafkaManager_AllStubs invokes every MockKafkaManager method with zero
// values to exercise its default fall-through branches. The mock is widely
// reused across server tests; this guarantees the no-Fn paths stay live.
func TestMockKafkaManager_AllStubs(t *testing.T) {
	t.Parallel()
	m := &MockKafkaManager{}
	ctx := context.Background()

	_ = m.DefaultClusterName()
	_ = m.ClusterNames()
	_, _ = m.GetClusterConfig("c")
	_ = m.Ping(ctx)
	_, _ = m.ListTopics(ctx, "c", false)
	_, _ = m.DescribeTopic(ctx, "c", "t")
	_ = m.CreateTopic(ctx, "c", CreateTopicRequest{})
	_ = m.AlterTopic(ctx, "c", AlterTopicRequest{})
	_ = m.DeleteTopic(ctx, "c", "t")
	_, _ = m.Consume(ctx, "c", "t", 0, "earliest", 1, 100, "")
	_, _ = m.Produce(ctx, "c", "t", nil, ProduceOptions{})
	_, _ = m.Subscribe(ctx, "c", SubscribeRequest{})
	_, _, _ = m.SearchMessages(ctx, "c", SearchRequest{})
	_, _ = m.ListConsumerGroups(ctx, "c")
	_, _ = m.DescribeConsumerGroup(ctx, "c", "g")
	_, _ = m.ResetOffsets(ctx, "c", ResetOffsetsRequest{})
	_, _ = m.GetClusterMetadata(ctx, "c")
	_, _ = m.Aggregate(ctx, "c", AggregateRequest{})
	_, _ = m.GetTopicGraph(ctx, "c")
	_, _ = m.JoinTopics(ctx, "c", JoinRequest{})
	_, _ = m.NaturalLanguageQuery(ctx, "c", "q", nil)
	_, _ = m.AnalyzeDLQ(ctx, "c", "t", 0)
	_, _ = m.StartStream(ctx, "c", StreamConfig{})
	_, _ = m.PollStream("s", 100)
	_, _ = m.ControlStream("s", "pause")
	_ = m.ListStreams()
	_, _ = m.GetStreamSession("s")
	_, _ = m.TimeTravelSnapshot(ctx, "c", "t", "0", 0)
	_, _ = m.TimeTravelDiff(ctx, "c", "t", "0", "1", 0)
	_, _ = m.GetRecommendations(ctx, "c", "t")
	_, _ = m.TraceMessage(ctx, "c", "id", "h", nil, 0)
	_, _ = m.ListACLs(ctx, "c", "", "", false)
	_, _ = m.CompareConfigs(ctx, "a", "b", "t")
	_, _ = m.AuditConfigs(ctx, "c", "topic", map[string]string{})
	m.Close()

	if len(m.Calls) == 0 {
		t.Error("expected mock to record calls")
	}
}
