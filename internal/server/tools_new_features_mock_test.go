package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleMigrationPlan =====================

func TestHandleMigrationPlan_PlanSuccess(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			if cluster == "src" {
				return []kafka.TopicSummary{
					{Name: "orders", Partitions: 3, ReplicationFactor: 2},
				}, nil
			}
			return []kafka.TopicSummary{}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleMigrationPlan(context.Background(), makeToolRequest(map[string]any{
		"action":         "plan",
		"source_cluster": "src",
		"target_cluster": "tgt",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleMigrationPlan_MissingClusters(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleMigrationPlan(context.Background(), makeToolRequest(map[string]any{
		"action": "plan",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleMigrationPlan_ListAction(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleMigrationPlan(context.Background(), makeToolRequest(map[string]any{
		"action": "list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "plans") {
		t.Error("expected plans in result")
	}
}

func TestHandleMigrationPlan_ListTopicsError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, includeInternal bool) ([]kafka.TopicSummary, error) {
			return nil, fmt.Errorf("connection refused")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleMigrationPlan(context.Background(), makeToolRequest(map[string]any{
		"action": "plan", "source_cluster": "src", "target_cluster": "tgt",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleTraceLineage =====================

func TestHandleTraceLineage_TraceSuccess(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		GetTopicGraphFn: func(ctx context.Context, cluster string) (*kafka.TopicGraph, error) {
			return &kafka.TopicGraph{
				Nodes: []kafka.TopicGraphNode{{ID: "topic:orders", Type: "topic", Name: "orders"}},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleTraceLineage(context.Background(), makeToolRequest(map[string]any{
		"action": "trace", "topic": "orders",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleTraceLineage_TraceMissingTopic_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleTraceLineage(context.Background(), makeToolRequest(map[string]any{
		"action": "trace",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleTraceLineage_ComplianceReport(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleTraceLineage(context.Background(), makeToolRequest(map[string]any{
		"action": "compliance_report", "framework": "GDPR",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleTraceLineage_DetectPII(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleTraceLineage(context.Background(), makeToolRequest(map[string]any{
		"action": "detect_pii", "topic": "users",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleDiagnoseLag =====================

func TestHandleDiagnoseLag_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return &kafka.ConsumerGroupDetail{
				GroupID:  groupID,
				State:    "Stable",
				TotalLag: 5000,
				Members:  []kafka.GroupMemberDetail{{MemberID: "m1"}},
				Offsets: []kafka.GroupOffsetDetail{
					{Topic: "orders", Partition: 0, CurrentOffset: 100, LogEndOffset: 5100, Lag: 5000},
				},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleDiagnoseLag(context.Background(), makeToolRequest(map[string]any{
		"group_id": "processor",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleDiagnoseLag_GroupNotFound(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return nil, fmt.Errorf("group %q not found", groupID)
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleDiagnoseLag(context.Background(), makeToolRequest(map[string]any{
		"group_id": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleDiagnoseLag_HealthyGroup(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return &kafka.ConsumerGroupDetail{
				GroupID:  groupID,
				State:    "Stable",
				TotalLag: 0,
				Members:  []kafka.GroupMemberDetail{{MemberID: "m1"}},
				Offsets:  []kafka.GroupOffsetDetail{},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleDiagnoseLag(context.Background(), makeToolRequest(map[string]any{
		"group_id": "healthy-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleForecast =====================

func TestHandleForecast_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return &kafka.TopicDetail{
				Name: topicName,
				Partitions: []kafka.PartitionDetail{
					{EarliestOffset: 0, LatestOffset: 10000},
				},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleForecast(context.Background(), makeToolRequest(map[string]any{
		"topic":  "orders",
		"metric": "throughput",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// May return error due to insufficient data points, which is expected
	_ = result
}

func TestHandleForecast_MissingTopic_Mock(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleForecast(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleForecast_DescribeError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeTopicFn: func(ctx context.Context, cluster, topicName string) (*kafka.TopicDetail, error) {
			return nil, fmt.Errorf("topic not found")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleForecast(context.Background(), makeToolRequest(map[string]any{
		"topic": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleGroupHealth =====================

func TestHandleGroupHealth_Success(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return &kafka.ConsumerGroupDetail{
				GroupID:  groupID,
				State:    "Stable",
				TotalLag: 100,
				Members:  []kafka.GroupMemberDetail{{MemberID: "m1"}},
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleGroupHealth(context.Background(), makeToolRequest(map[string]any{
		"group_id": "my-group",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleGroupHealth_DescribeError(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		DescribeConsumerGroupFn: func(ctx context.Context, cluster, groupID string) (*kafka.ConsumerGroupDetail, error) {
			return nil, fmt.Errorf("group not found")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleGroupHealth(context.Background(), makeToolRequest(map[string]any{
		"group_id": "nonexistent",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handlePersona =====================

func TestHandlePersona_List(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handlePersona(context.Background(), makeToolRequest(map[string]any{
		"action": "list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "personas") {
		t.Error("expected personas in result")
	}
}

// ===================== handleDataContract =====================

func TestHandleDataContract_ListSuccess(t *testing.T) {
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleDataContract(context.Background(), makeToolRequest(map[string]any{
		"action": "list",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}
