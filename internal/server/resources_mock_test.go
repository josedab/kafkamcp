package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/kafka"

	"github.com/mark3labs/mcp-go/mcp"
)

// resourceText extracts the text payload from the first resource content entry.
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

// ===================== handleClusterMetadata =====================

func TestHandleClusterMetadata_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterMetadataFn: func(ctx context.Context, cluster string) (*kafka.ClusterMetadata, error) {
			return &kafka.ClusterMetadata{
				ClusterID:      "test-cluster-id",
				BrokerCount:    3,
				TopicCount:     10,
				PartitionCount: 30,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	contents, err := s.handleClusterMetadata(ctx, makeResourceRequest("kafka://cluster/metadata"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) == 0 {
		t.Fatal("expected non-empty contents")
	}
	if mock.CallCount("GetClusterMetadata") != 1 {
		t.Error("expected GetClusterMetadata to be called once")
	}
}

func TestHandleClusterMetadata_KafkaManagerError(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		GetClusterMetadataFn: func(ctx context.Context, cluster string) (*kafka.ClusterMetadata, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	s := newTestServerWithMock(t, mock)
	ctx := context.Background()

	_, err := s.handleClusterMetadata(ctx, makeResourceRequest("kafka://cluster/metadata"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("expected 'connection refused' in error, got: %v", err)
	}
}

func TestHandleClusterMetadata_AuthDenied(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock

	_, err := s.handleClusterMetadata(context.Background(), makeResourceRequest("kafka://cluster/metadata"))
	if err == nil {
		t.Fatal("expected error for auth denied")
	}
	if !strings.Contains(err.Error(), "unauthorized") && !strings.Contains(err.Error(), "authentication required") {
		t.Errorf("expected auth error, got: %v", err)
	}
	if mock.CallCount("GetClusterMetadata") != 0 {
		t.Error("GetClusterMetadata should not be called when auth denied")
	}
}

// ===================== handleClusterList =====================

func TestHandleClusterList_MultiCluster(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, func(cfg *config.Config) {
		cfg.Clusters = []config.ClusterConfig{
			{Name: "prod", Default: true, SchemaRegistry: config.SchemaRegistryConfig{URL: "http://sr:8081"}},
			{Name: "staging", Default: false},
			{Name: "dev", Default: false, SchemaRegistry: config.SchemaRegistryConfig{URL: "http://sr-dev:8081"}},
		}
	})
	s.kafkaManager = mock
	ctx := context.Background()

	contents, err := s.handleClusterList(ctx, makeResourceRequest("kafka://clusters"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(contents) == 0 {
		t.Fatal("expected non-empty contents")
	}

	text := resourceText(t, contents)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
	count := parsed["total_count"].(float64)
	if int(count) != 3 {
		t.Errorf("expected 3 clusters, got %v", count)
	}
}

func TestHandleClusterList_EmptyConfig(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, func(cfg *config.Config) {
		cfg.Clusters = nil
	})
	s.kafkaManager = mock

	contents, err := s.handleClusterList(context.Background(), makeResourceRequest("kafka://clusters"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := resourceText(t, contents)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
	count := parsed["total_count"].(float64)
	if int(count) != 0 {
		t.Errorf("expected 0 clusters, got %v", count)
	}
}

// ===================== handleTopicCatalog =====================

func TestHandleTopicCatalog_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, MessageCount: 1000},
				{Name: "events", Partitions: 6, MessageCount: 5000},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	contents, err := s.handleTopicCatalog(context.Background(), makeResourceRequest("kafka://topics"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := resourceText(t, contents)
	if !strings.Contains(text, "orders") || !strings.Contains(text, "events") {
		t.Errorf("expected topics in result, got: %s", text)
	}
}

func TestHandleTopicCatalog_WithPolicyFiltering(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3},
				{Name: "secret-data", Partitions: 1},
			}, nil
		},
	}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock

	// Without a properly authorized agent, this should fail at auth
	_, err := s.handleTopicCatalog(context.Background(), makeResourceRequest("kafka://topics"))
	if err == nil {
		t.Fatal("expected auth error with default deny and no agent")
	}
}

func TestHandleTopicCatalog_KafkaManagerError(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, fmt.Errorf("broker unavailable")
		},
	}
	s := newTestServerWithMock(t, mock)

	_, err := s.handleTopicCatalog(context.Background(), makeResourceRequest("kafka://topics"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "broker unavailable") {
		t.Errorf("expected broker error, got: %v", err)
	}
}

// ===================== handleSchemaCatalog =====================

func TestHandleSchemaCatalog_NoSchemaRegistry(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)
	s.kafkaManager = &kafka.MockKafkaManager{}

	contents, err := s.handleSchemaCatalog(context.Background(), makeResourceRequest("kafka://schemas"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := resourceText(t, contents)
	if !strings.Contains(text, "not configured") {
		t.Errorf("expected 'not configured' message, got: %s", text)
	}
}

// ===================== handleGroupCatalog =====================

func TestHandleGroupCatalog_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return []kafka.ConsumerGroupSummary{
				{GroupID: "order-processor", State: "Stable", MemberCount: 3, TotalLag: 0},
				{GroupID: "analytics", State: "Empty", MemberCount: 0, TotalLag: 500},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	contents, err := s.handleGroupCatalog(context.Background(), makeResourceRequest("kafka://groups"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := resourceText(t, contents)
	if !strings.Contains(text, "order-processor") {
		t.Error("expected consumer group in result")
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
	count := parsed["total_count"].(float64)
	if int(count) != 2 {
		t.Errorf("expected 2 groups, got %v", count)
	}
}

func TestHandleGroupCatalog_EmptyGroups(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	contents, err := s.handleGroupCatalog(context.Background(), makeResourceRequest("kafka://groups"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := resourceText(t, contents)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(text), &parsed); err != nil {
		t.Fatalf("expected valid JSON: %v", err)
	}
}

func TestHandleGroupCatalog_Error(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		ListConsumerGroupsFn: func(ctx context.Context, cluster string) ([]kafka.ConsumerGroupSummary, error) {
			return nil, fmt.Errorf("connection timeout")
		},
	}
	s := newTestServerWithMock(t, mock)

	_, err := s.handleGroupCatalog(context.Background(), makeResourceRequest("kafka://groups"))
	if err == nil {
		t.Fatal("expected error")
	}
}

// ===================== handleTopicGraphResource =====================

// ===================== handleSchemaResource =====================

func TestHandleSchemaResource_NoRegistry(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t)
	s.kafkaManager = mock

	_, err := s.handleSchemaResource(context.Background(), makeResourceRequest("kafka://schemas/user-value"))
	if err == nil {
		t.Fatal("expected error when schema registry not configured")
	}
	if !strings.Contains(err.Error(), "not configured") {
		t.Errorf("expected 'not configured', got: %v", err)
	}
}

func TestHandleSchemaResource_InvalidURI(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t)
	s.kafkaManager = mock

	_, err := s.handleSchemaResource(context.Background(), makeResourceRequest("kafka://schemas/"))
	if err == nil {
		t.Fatal("expected error for empty subject")
	}
}

func TestHandleSchemaResource_SubjectTooLong(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t)
	s.kafkaManager = mock
	longSubject := strings.Repeat("s", maxInputLength+1)

	_, err := s.handleSchemaResource(context.Background(), makeResourceRequest("kafka://schemas/"+longSubject))
	if err == nil {
		t.Fatal("expected error for subject too long")
	}
}

// ===================== handleTopicResource with mock =====================

func TestHandleTopicResource_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{
				Name:              "orders",
				ReplicationFactor: 3,
				MessageCount:      42000,
				Configs:           map[string]string{"retention.ms": "604800000"},
				Partitions: []kafka.PartitionDetail{
					{ID: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
				},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	contents, err := s.handleTopicResource(context.Background(), makeResourceRequest("kafka://topics/orders"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := resourceText(t, contents)
	if !strings.Contains(text, "orders") {
		t.Error("expected topic name in result")
	}
}

func TestHandleTopicResource_DescribeError(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return nil, fmt.Errorf("topic %q not found", topicName)
		},
	}
	s := newTestServerWithMock(t, mock)

	_, err := s.handleTopicResource(context.Background(), makeResourceRequest("kafka://topics/nonexistent"))
	if err == nil {
		t.Fatal("expected error for nonexistent topic")
	}
}

// ===================== handleGroupResource with mock =====================

func TestHandleGroupResource_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return &kafka.ConsumerGroupDetail{
				GroupID:  "my-group",
				State:    "Stable",
				TotalLag: 100,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)

	contents, err := s.handleGroupResource(context.Background(), makeResourceRequest("kafka://groups/my-group"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	text := resourceText(t, contents)
	if !strings.Contains(text, "my-group") {
		t.Error("expected group ID in result")
	}
}

func TestHandleGroupResource_DescribeError(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return nil, fmt.Errorf("group not found")
		},
	}
	s := newTestServerWithMock(t, mock)

	_, err := s.handleGroupResource(context.Background(), makeResourceRequest("kafka://groups/nonexistent"))
	if err == nil {
		t.Fatal("expected error")
	}
}
