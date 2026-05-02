package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/audit"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

func testV2Logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

func newTestDashboardV2(km kafka.KafkaManager) *DashboardV2 {
	al, _ := audit.NewLogger(context.Background(), true, 1000, "", testV2Logger())
	return NewDashboardV2(km, al, testV2Logger())
}

func TestNewDashboardV2(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(nil)
	if d == nil {
		t.Fatal("expected non-nil dashboard v2")
	}
}

func TestHandleTopologyV2(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 2, MessageCount: 100},
			}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{
				{GroupID: "order-processor", State: "Stable", TotalLag: 10},
			}, nil
		},
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return &kafka.ConsumerGroupDetail{
				GroupID: groupID,
				Offsets: []kafka.GroupOffsetDetail{
					{Topic: "orders", Partition: 0},
				},
			}, nil
		},
		GetClusterMetadataFn: func(ctx context.Context, cluster string) (*kafka.ClusterMetadata, error) {
			return &kafka.ClusterMetadata{
				ClusterID: "test-cluster",
				Brokers:   []kafka.BrokerInfo{{ID: 1}},
			}, nil
		},
	}

	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/topology", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp TopologyResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Nodes) < 2 {
		t.Errorf("expected at least 2 nodes (topic + group), got %d", len(resp.Nodes))
	}
	if len(resp.Edges) < 1 {
		t.Errorf("expected at least 1 edge, got %d", len(resp.Edges))
	}
}

func TestHandleTopologyV2MethodNotAllowed(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/topology", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleTopologyV2ListTopicsError(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, errors.New("connection refused")
		},
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/topology", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandleHeatmap(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/heatmap?hours=6", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp HeatmapResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.TimeRange != "6h" {
		t.Errorf("expected 6h time range, got %s", resp.TimeRange)
	}
	if resp.BucketMin != 15 {
		t.Errorf("expected 15min buckets for 6h, got %d", resp.BucketMin)
	}
}

func TestHandleHeatmapDefaultHours(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/heatmap", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var resp HeatmapResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.TimeRange != "24h" {
		t.Errorf("expected 24h default time range, got %s", resp.TimeRange)
	}
	if resp.BucketMin != 60 {
		t.Errorf("expected 60min buckets for 24h, got %d", resp.BucketMin)
	}
}

func TestHandleHeatmap12Hours(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/heatmap?hours=12", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var resp HeatmapResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.BucketMin != 30 {
		t.Errorf("expected 30min buckets for 12h, got %d", resp.BucketMin)
	}
}

func TestHandleHeatmapMethodNotAllowed(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/heatmap", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleMessageTail(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 1, Key: "k1", Value: "small value", Timestamp: "2025-01-01T00:00:00Z"},
			}, nil
		},
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/messages/tail?topic=orders&limit=10", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["topic"] != "orders" {
		t.Errorf("expected topic orders, got %v", resp["topic"])
	}
}

func TestHandleMessageTailTruncation(t *testing.T) {
	t.Parallel()
	largeValue := make([]byte, 3000)
	for i := range largeValue {
		largeValue[i] = 'x'
	}
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{
				{Partition: 0, Offset: 1, Key: "k1", Value: string(largeValue), Timestamp: "2025-01-01T00:00:00Z"},
			}, nil
		},
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/messages/tail?topic=orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleMessageTailNoTopic(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/messages/tail", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandleMessageTailError(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return nil, errors.New("kafka error")
		},
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/messages/tail?topic=orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandleMessageTailMethodNotAllowed(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/messages/tail?topic=orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleListTailers(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/messages/tailers", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["count"].(float64) != 0 {
		t.Errorf("expected 0 tailers, got %v", resp["count"])
	}
}

func TestHandleListTailersMethodNotAllowed(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/messages/tailers", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleClusterOverview(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterMetadataFn: func(ctx context.Context, cluster string) (*kafka.ClusterMetadata, error) {
			return &kafka.ClusterMetadata{
				ClusterID: "test-cluster",
				Brokers:   []kafka.BrokerInfo{{ID: 1}, {ID: 2}},
			}, nil
		},
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, MessageCount: 100},
				{Name: "__offsets", Partitions: 50, IsInternal: true},
			}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{
				{GroupID: "g1", State: "Stable", TotalLag: 100},
			}, nil
		},
		PingFn: func(ctx context.Context) error { return nil },
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/cluster/overview", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["status"] != "healthy" {
		t.Errorf("expected healthy, got %v", resp["status"])
	}
	if resp["broker_count"] == nil {
		t.Error("expected broker_count in response")
	}
}

func TestHandleClusterOverviewUnhealthy(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterMetadataFn: func(ctx context.Context, cluster string) (*kafka.ClusterMetadata, error) {
			return &kafka.ClusterMetadata{ClusterID: "c1"}, nil
		},
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, nil
		},
		PingFn: func(ctx context.Context) error { return errors.New("unreachable") },
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/cluster/overview", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["status"] != "unhealthy" {
		t.Errorf("expected unhealthy, got %v", resp["status"])
	}
}

func TestHandleClusterOverviewMethodNotAllowed(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/cluster/overview", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandleConsumerGroupsDetailSpecific(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return &kafka.ConsumerGroupDetail{
				GroupID:  groupID,
				State:    "Stable",
				TotalLag: 50,
			}, nil
		},
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/consumer-groups/detail?group_id=my-group", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleConsumerGroupsDetailNotFound(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return nil, errors.New("not found")
		},
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/consumer-groups/detail?group_id=missing", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHandleConsumerGroupsDetailListAll(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{
				{GroupID: "g1"},
				{GroupID: "g2"},
			}, nil
		},
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/consumer-groups/detail", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["total"].(float64) != 2 {
		t.Errorf("expected 2 groups, got %v", resp["total"])
	}
}

func TestHandleConsumerGroupsDetailListError(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, errors.New("error")
		},
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/consumer-groups/detail", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandleConsumerGroupsDetailMethodNotAllowed(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/consumer-groups/detail", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestHandlePartitionView(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{
				Name: "orders",
				Partitions: []kafka.PartitionDetail{
					{ID: 0, Leader: 1, Replicas: []int32{1, 2}, ISR: []int32{1, 2}, EarliestOffset: 0, LatestOffset: 100},
					{ID: 1, Leader: 2, Replicas: []int32{2, 3}, ISR: []int32{2}, EarliestOffset: 0, LatestOffset: 200},
				},
				Configs: map[string]string{"retention.ms": "86400000"},
			}, nil
		},
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/partitions?topic=orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["total"].(float64) != 2 {
		t.Errorf("expected 2 partitions, got %v", resp["total"])
	}
}

func TestHandlePartitionViewNoTopic(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/partitions", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestHandlePartitionViewNotFound(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return nil, errors.New("not found")
		},
	}
	d := newTestDashboardV2(mock)
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/partitions?topic=missing", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHandlePartitionViewMethodNotAllowed(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v2/partitions?topic=orders", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}
}

func TestRecordAgentCall(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	d.RecordAgentCall("agent-1", "list_topics", false)
	d.RecordAgentCall("agent-1", "consume", true)
	d.RecordAgentCall("agent-2", "produce", false)

	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.heatmap) != 2 {
		t.Errorf("expected 2 agents in heatmap, got %d", len(d.heatmap))
	}

	a1 := d.heatmap["agent-1"]
	if a1.ToolCalls["list_topics"] != 1 {
		t.Errorf("expected 1 list_topics call, got %d", a1.ToolCalls["list_topics"])
	}
	if a1.ToolCalls["consume"] != 1 {
		t.Errorf("expected 1 consume call, got %d", a1.ToolCalls["consume"])
	}
}

func TestMarshalJSON(t *testing.T) {
	t.Parallel()
	data, err := marshalJSON(map[string]string{"key": "value"})
	if err != nil {
		t.Fatalf("marshalJSON failed: %v", err)
	}
	var result map[string]string
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("expected value, got %s", result["key"])
	}
}

func TestHandleSSETailNoFlusher(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})

	// httptest.ResponseRecorder implements http.Flusher, so use a custom writer that doesn't
	req := httptest.NewRequest(http.MethodGet, "/api/v2/messages/tail-stream?topic=orders", nil)
	w := &nonFlushWriter{httptest.NewRecorder()}
	d.HandleSSETail(w, req)

	if w.rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for non-flusher, got %d", w.rec.Code)
	}
}

func TestHandleSSETailNoTopic(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})

	req := httptest.NewRequest(http.MethodGet, "/api/v2/messages/tail-stream", nil)
	w := httptest.NewRecorder()
	d.HandleSSETail(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for missing topic, got %d", w.Code)
	}
}

func TestRegisterRoutesWithNilAuth(t *testing.T) {
	t.Parallel()
	d := newTestDashboardV2(&kafka.MockKafkaManager{})
	mux := http.NewServeMux()
	d.RegisterRoutes(mux, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v2/messages/tailers", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with nil auth, got %d", w.Code)
	}
}

// nonFlushWriter wraps httptest.ResponseRecorder without implementing http.Flusher
type nonFlushWriter struct {
	rec *httptest.ResponseRecorder
}

func (w *nonFlushWriter) Header() http.Header         { return w.rec.Header() }
func (w *nonFlushWriter) Write(b []byte) (int, error)  { return w.rec.Write(b) }
func (w *nonFlushWriter) WriteHeader(code int)         { w.rec.WriteHeader(code) }
