package server

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/josedab/kafkamcp/internal/kafka"
)

func TestAggregateMetricFamilyDropsLabels(t *testing.T) {
	name := "calls"
	metricType := dto.MetricType_COUNTER
	one := 1.0
	two := 2.0
	family := &dto.MetricFamily{
		Name: &name,
		Type: &metricType,
		Metric: []*dto.Metric{
			{Counter: &dto.Counter{Value: &one}},
			{Counter: &dto.Counter{Value: &two}},
		},
	}
	got := aggregateMetricFamily(family)
	if got["samples"] != float64(3) || got["series"] != 2 {
		t.Fatalf("unexpected aggregate: %#v", got)
	}
}

func TestHandleCoreHealthUsesLiveMetadata(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		GetClusterMetadataFn: func(context.Context, string) (*kafka.ClusterMetadata, error) {
			return &kafka.ClusterMetadata{ClusterID: "cluster-1", Controller: kafka.BrokerInfo{ID: 1}, Brokers: []kafka.BrokerInfo{{ID: 1}}}, nil
		},
		ListTopicsFn: func(context.Context, string, bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{{Name: "orders"}}, nil
		},
		ListConsumerGroupsFn: func(context.Context, string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{{GroupID: "orders-consumer"}}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleCoreHealth(context.Background(), makeToolRequest(map[string]any{"cluster": "default"}), "agent", map[string]any{"cluster": "default"}, time.Now())
	if err != nil || result == nil || result.IsError {
		t.Fatalf("health failed: result=%+v err=%v", result, err)
	}
}
