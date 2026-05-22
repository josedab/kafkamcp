package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/config"
)

// TestMockKafkaManager_AllFnHooks exercises every configurable Fn hook
// so the `if Fn != nil { return Fn(...) }` branches are covered.
func TestMockKafkaManager_AllFnHooks(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("sentinel")

	m := &MockKafkaManager{
		DefaultClusterNameFn: func() string { return "x" },
		ClusterNamesFn:       func() []string { return []string{"x"} },
		GetClusterConfigFn:   func(string) (*config.ClusterConfig, error) { return &config.ClusterConfig{}, nil },
		PingFn:               func(context.Context) error { return sentinel },
		CloseFn:              func() {},
		ListTopicsFn:         func(context.Context, string, bool) ([]TopicSummary, error) { return nil, sentinel },
		DescribeTopicFn:      func(context.Context, string, string) (*TopicDetail, error) { return nil, sentinel },
		CreateTopicFn:        func(context.Context, string, CreateTopicRequest) error { return sentinel },
		AlterTopicFn:         func(context.Context, string, AlterTopicRequest) error { return sentinel },
		DeleteTopicFn:        func(context.Context, string, string) error { return sentinel },
		ConsumeFn: func(context.Context, string, string, int32, string, int, int, string) ([]Message, error) {
			return nil, sentinel
		},
		ProduceFn: func(context.Context, string, string, []ProduceMessage, ProduceOptions) ([]DeliveryResult, error) {
			return nil, sentinel
		},
		SubscribeFn:             func(context.Context, string, SubscribeRequest) (*SubscribeResult, error) { return nil, sentinel },
		SearchMessagesFn:        func(context.Context, string, SearchRequest) ([]SearchMessage, int, error) { return nil, 0, sentinel },
		ListConsumerGroupsFn:    func(context.Context, string) ([]ConsumerGroupSummary, error) { return nil, sentinel },
		DescribeConsumerGroupFn: func(context.Context, string, string) (*ConsumerGroupDetail, error) { return nil, sentinel },
		ResetOffsetsFn:          func(context.Context, string, ResetOffsetsRequest) (*ResetOffsetsResult, error) { return nil, sentinel },
		GetClusterMetadataFn:    func(context.Context, string) (*ClusterMetadata, error) { return nil, sentinel },
		AggregateFn:             func(context.Context, string, AggregateRequest) (*AggregateResult, error) { return nil, sentinel },
		GetTopicGraphFn:         func(context.Context, string) (*TopicGraph, error) { return nil, sentinel },
		JoinTopicsFn:            func(context.Context, string, JoinRequest) (*JoinResult, error) { return nil, sentinel },
		NaturalLanguageQueryFn: func(context.Context, string, string, []TopicSummary) (*NaturalLanguageQueryResult, error) {
			return nil, sentinel
		},
		AnalyzeDLQFn:         func(context.Context, string, string, int) (*AnalyzeDLQResult, error) { return nil, sentinel },
		StartStreamFn:        func(context.Context, string, StreamConfig) (*StreamResult, error) { return nil, sentinel },
		PollStreamFn:         func(string, int) (*StreamResult, error) { return nil, sentinel },
		ControlStreamFn:      func(string, string) (*StreamResult, error) { return nil, sentinel },
		ListStreamsFn:        func() []StreamSessionSnapshot { return []StreamSessionSnapshot{{}} },
		GetStreamSessionFn:   func(string) (StreamSessionSnapshot, bool) { return StreamSessionSnapshot{}, false },
		TimeTravelSnapshotFn: func(context.Context, string, string, string, int) (*TimeTravelSnapshot, error) { return nil, sentinel },
		TimeTravelDiffFn: func(context.Context, string, string, string, string, int) (*TimeTravelDiffResult, error) {
			return nil, sentinel
		},
		GetRecommendationsFn: func(context.Context, string, string) (*RecommendationResult, error) { return nil, sentinel },
		TraceMessageFn: func(context.Context, string, string, string, []string, int) (*TraceResult, error) {
			return nil, sentinel
		},
		ListACLsFn:       func(context.Context, string, string, string, bool) (*ACLResult, error) { return nil, sentinel },
		CompareConfigsFn: func(context.Context, string, string, string) (*ConfigCompareResult, error) { return nil, sentinel },
		AuditConfigsFn: func(context.Context, string, string, map[string]string) (*ConfigAuditResult, error) {
			return nil, sentinel
		},
		PlaygroundCreateFn: func(context.Context, string, string, string, int32) (*PlaygroundTopicResult, error) {
			return nil, sentinel
		},
		PlaygroundProduceSampleFn: func(context.Context, string, string, string, string, int) (map[string]any, error) {
			return nil, sentinel
		},
		PlaygroundListFn:    func(context.Context, string, string) ([]TopicSummary, error) { return nil, sentinel },
		PlaygroundCleanupFn: func(context.Context, string, string) (int, error) { return 0, sentinel },
	}
	ctx := context.Background()

	_ = m.DefaultClusterName()
	_ = m.ClusterNames()
	_, _ = m.GetClusterConfig("x")
	if err := m.Ping(ctx); !errors.Is(err, sentinel) {
		t.Errorf("Ping returned %v, want sentinel", err)
	}
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
	_, _ = m.PlaygroundCreate(ctx, "c", "a", "t", 1)
	_, _ = m.PlaygroundProduceSample(ctx, "c", "a", "t", "p", 1)
	_, _ = m.PlaygroundList(ctx, "c", "a")
	_, _ = m.PlaygroundCleanup(ctx, "c", "a")
	m.Close()

	if got := m.CallCount("Ping"); got != 1 {
		t.Errorf("expected Ping CallCount=1, got %d", got)
	}
	if m.LastCall("Ping") == nil {
		t.Error("expected LastCall(Ping) to be non-nil")
	}
	if m.LastCall("NonExistent") != nil {
		t.Error("expected LastCall(NonExistent) to be nil")
	}
}

func TestMockManager_VerifyCallTracking(t *testing.T) {
	t.Parallel()
	m := &MockKafkaManager{}
	_ = m.Ping(context.Background())
	_ = m.Ping(context.Background())
	if got := m.CallCount("Ping"); got != 2 {
		t.Errorf("expected CallCount=2, got %d", got)
	}
	if m.LastCall("Ping") == nil {
		t.Error("expected non-nil LastCall")
	}
	if m.LastCall("Missing") != nil {
		t.Error("expected nil LastCall for unknown method")
	}
}
