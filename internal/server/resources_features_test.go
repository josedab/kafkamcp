package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

// newTestServerWithMockForResource creates a Server with a MockKafkaManager for resource handler tests.
func newTestServerWithMockForResource(t *testing.T, mock *kafka.MockKafkaManager) *Server {
	t.Helper()
	s := newTestServer(t)
	s.kafkaManager = mock
	return s
}

// resourceText extracts the JSON text from the first resource content.
func resourceText(t *testing.T, contents []mcp.ResourceContents) string {
	t.Helper()
	if len(contents) == 0 {
		t.Fatal("expected non-empty resource contents")
	}
	tc, ok := contents[0].(mcp.TextResourceContents)
	if !ok {
		t.Fatalf("expected TextResourceContents, got %T", contents[0])
	}
	return tc.Text
}

// parseResourceJSON parses the JSON from resource contents into a map.
func parseResourceJSON(t *testing.T, contents []mcp.ResourceContents) map[string]any {
	t.Helper()
	text := resourceText(t, contents)
	var result map[string]any
	if err := json.Unmarshal([]byte(text), &result); err != nil {
		t.Fatalf("failed to parse resource JSON: %v\ntext: %s", err, text)
	}
	return result
}

// ===================== handleAnomalyResource =====================

func TestHandleAnomalyResource_NilDetector(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMockForResource(t, mock)
	s.anomalyDetector = nil
	ctx := context.Background()

	contents, err := s.handleAnomalyResource(ctx, makeResourceRequest("kafka://anomalies"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}

	parsed := parseResourceJSON(t, contents)

	alerts, ok := parsed["alerts"]
	if !ok {
		t.Error("expected 'alerts' key in response")
	}
	alertSlice, ok := alerts.([]any)
	if !ok {
		t.Fatalf("expected alerts to be a slice, got %T", alerts)
	}
	if len(alertSlice) != 0 {
		t.Errorf("expected empty alerts, got %d", len(alertSlice))
	}

	status, ok := parsed["status"].(map[string]any)
	if !ok {
		t.Fatal("expected 'status' key to be a map")
	}
	if active, _ := status["active"].(bool); active {
		t.Error("expected status.active=false for nil detector")
	}
}

func TestHandleAnomalyResource_WithDetector(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMockForResource(t, mock)
	ctx := context.Background()

	contents, err := s.handleAnomalyResource(ctx, makeResourceRequest("kafka://anomalies"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}

	parsed := parseResourceJSON(t, contents)
	if _, ok := parsed["alerts"]; !ok {
		t.Error("expected 'alerts' key in response")
	}
	if _, ok := parsed["status"]; !ok {
		t.Error("expected 'status' key in response")
	}
}

func TestHandleAnomalyResource_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	ctx := context.Background()

	// Anonymous request with default deny should fail
	_, err := s.handleAnomalyResource(ctx, makeResourceRequest("kafka://anomalies"))
	if err == nil {
		t.Error("expected error for anonymous request with default deny")
	}
}

// ===================== handleWorkflowsResource =====================

func TestHandleWorkflowsResource_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMockForResource(t, mock)
	ctx := context.Background()

	contents, err := s.handleWorkflowsResource(ctx, makeResourceRequest("kafka://workflows"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}

	parsed := parseResourceJSON(t, contents)
	if _, ok := parsed["templates"]; !ok {
		t.Error("expected 'templates' key in response")
	}
	if _, ok := parsed["categories"]; !ok {
		t.Error("expected 'categories' key in response")
	}
	if _, ok := parsed["total_count"]; !ok {
		t.Error("expected 'total_count' key in response")
	}
}

func TestHandleWorkflowsResource_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	ctx := context.Background()

	// Anonymous request with default deny should fail at validateResourceAgentID
	_, err := s.handleWorkflowsResource(ctx, makeResourceRequest("kafka://workflows"))
	if err == nil {
		t.Error("expected error for anonymous request with default deny")
	}
}

// ===================== handleKnowledgeGraphResource =====================

func TestHandleKnowledgeGraphResource_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3},
			}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, nil
		},
	}
	s := newTestServerWithMockForResource(t, mock)
	ctx := context.Background()

	contents, err := s.handleKnowledgeGraphResource(ctx, makeResourceRequest("kafka://knowledge_graph"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}

	parsed := parseResourceJSON(t, contents)
	if _, ok := parsed["nodes"]; !ok {
		t.Error("expected 'nodes' key in response")
	}
	if _, ok := parsed["edges"]; !ok {
		t.Error("expected 'edges' key in response")
	}
	if _, ok := parsed["stats"]; !ok {
		t.Error("expected 'stats' key in response")
	}
}

func TestHandleKnowledgeGraphResource_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	ctx := context.Background()

	_, err := s.handleKnowledgeGraphResource(ctx, makeResourceRequest("kafka://knowledge_graph"))
	if err == nil {
		t.Error("expected error for anonymous request with default deny")
	}
}

// ===================== handleFlowGraphResource =====================

func TestHandleFlowGraphResource_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, MessageCount: 1000},
				{Name: "events", Partitions: 6, MessageCount: 5000},
			}, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{
				{GroupID: "order-processor", State: "Stable", TotalLag: 10, MemberCount: 2},
			}, nil
		},
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return &kafka.ConsumerGroupDetail{
				GroupID: "order-processor",
				State:   "Stable",
				Offsets: []kafka.GroupOffsetDetail{
					{Topic: "orders", Partition: 0, CurrentOffset: 990, LogEndOffset: 1000, Lag: 10},
				},
			}, nil
		},
	}
	s := newTestServerWithMockForResource(t, mock)
	ctx := context.Background()

	contents, err := s.handleFlowGraphResource(ctx, makeResourceRequest("kafka://flow_graph"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}

	parsed := parseResourceJSON(t, contents)

	// Verify nodes
	nodes, ok := parsed["nodes"].([]any)
	if !ok {
		t.Fatal("expected 'nodes' to be a slice")
	}
	// 2 topics + 1 consumer group = 3 nodes
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}

	// Verify edges
	edges, ok := parsed["edges"].([]any)
	if !ok {
		t.Fatal("expected 'edges' to be a slice")
	}
	if len(edges) != 1 {
		t.Errorf("expected 1 edge (order-processor -> orders), got %d", len(edges))
	}

	// Verify edge structure
	if len(edges) > 0 {
		edge, ok := edges[0].(map[string]any)
		if !ok {
			t.Fatal("expected edge to be a map")
		}
		if edge["source"] != "orders" {
			t.Errorf("expected edge source 'orders', got %v", edge["source"])
		}
		if edge["target"] != "group:order-processor" {
			t.Errorf("expected edge target 'group:order-processor', got %v", edge["target"])
		}
		if edge["type"] != "consumes" {
			t.Errorf("expected edge type 'consumes', got %v", edge["type"])
		}
	}

	// Verify counts
	if parsed["total_nodes"] != float64(3) {
		t.Errorf("expected total_nodes=3, got %v", parsed["total_nodes"])
	}
	if parsed["total_edges"] != float64(1) {
		t.Errorf("expected total_edges=1, got %v", parsed["total_edges"])
	}

	// Verify mock calls
	if mock.CallCount("ListTopics") != 1 {
		t.Error("expected ListTopics to be called once")
	}
	if mock.CallCount("ListConsumerGroups") != 1 {
		t.Error("expected ListConsumerGroups to be called once")
	}
	if mock.CallCount("DescribeConsumerGroup") != 1 {
		t.Error("expected DescribeConsumerGroup to be called once")
	}
}

func TestHandleFlowGraphResource_EmptyCluster(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, nil
		},
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, nil
		},
	}
	s := newTestServerWithMockForResource(t, mock)
	ctx := context.Background()

	contents, err := s.handleFlowGraphResource(ctx, makeResourceRequest("kafka://flow_graph"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseResourceJSON(t, contents)
	if parsed["total_nodes"] != float64(0) {
		t.Errorf("expected total_nodes=0, got %v", parsed["total_nodes"])
	}
	if parsed["total_edges"] != float64(0) {
		t.Errorf("expected total_edges=0, got %v", parsed["total_edges"])
	}
}

func TestHandleFlowGraphResource_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	ctx := context.Background()

	_, err := s.handleFlowGraphResource(ctx, makeResourceRequest("kafka://flow_graph"))
	if err == nil {
		t.Error("expected error for anonymous request with default deny")
	}
}

// ===================== handlePlatformResource =====================

func TestHandlePlatformResource_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ClusterNamesFn: func() []string {
			return []string{"prod", "staging"}
		},
	}
	s := newTestServerWithMockForResource(t, mock)
	ctx := context.Background()

	contents, err := s.handlePlatformResource(ctx, makeResourceRequest("kafka://platforms"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) != 1 {
		t.Fatalf("expected 1 content, got %d", len(contents))
	}

	parsed := parseResourceJSON(t, contents)
	if _, ok := parsed["platforms"]; !ok {
		t.Error("expected 'platforms' key in response")
	}
	if _, ok := parsed["cluster_backends"]; !ok {
		t.Error("expected 'cluster_backends' key in response")
	}
	if _, ok := parsed["supported_types"]; !ok {
		t.Error("expected 'supported_types' key in response")
	}

	// Verify cluster backends
	backends, ok := parsed["cluster_backends"].([]any)
	if !ok {
		t.Fatal("expected cluster_backends to be a slice")
	}
	if len(backends) != 2 {
		t.Errorf("expected 2 cluster backends, got %d", len(backends))
	}

	// Verify mock was called
	if mock.CallCount("ClusterNames") != 1 {
		t.Error("expected ClusterNames to be called once")
	}
}

func TestHandlePlatformResource_SingleCluster(t *testing.T) {
	mock := &kafka.MockKafkaManager{} // defaults to returning ["default"]
	s := newTestServerWithMockForResource(t, mock)
	ctx := context.Background()

	contents, err := s.handlePlatformResource(ctx, makeResourceRequest("kafka://platforms"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	parsed := parseResourceJSON(t, contents)
	backends, ok := parsed["cluster_backends"].([]any)
	if !ok {
		t.Fatal("expected cluster_backends to be a slice")
	}
	if len(backends) != 1 {
		t.Errorf("expected 1 cluster backend, got %d", len(backends))
	}
	backend, ok := backends[0].(map[string]any)
	if !ok {
		t.Fatal("expected backend to be a map")
	}
	if backend["cluster"] != "default" {
		t.Errorf("expected cluster name 'default', got %v", backend["cluster"])
	}
	if backend["type"] != "kafka" {
		t.Errorf("expected type 'kafka', got %v", backend["type"])
	}
}

func TestHandlePlatformResource_AuthDenied(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock
	ctx := context.Background()

	_, err := s.handlePlatformResource(ctx, makeResourceRequest("kafka://platforms"))
	if err == nil {
		t.Error("expected error for anonymous request with default deny")
	}
}
