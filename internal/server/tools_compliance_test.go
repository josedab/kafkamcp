package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/compliance"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

func newTestServerWithCompliance(t *testing.T, mock *kafka.MockKafkaManager) *Server {
	t.Helper()
	s := newTestServerWithMock(t, mock)
	s.complianceEngine = compliance.NewEngine(s.logger)
	return s
}

// ===================== handleComplianceScan =====================

func TestHandleComplianceScan_WithFramework(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, internal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders", Partitions: 3, ReplicationFactor: 3, RetentionMs: 604800000},
			}, nil
		},
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{}, nil
		},
	}
	s := newTestServerWithCompliance(t, mock)
	result, err := s.handleComplianceScan(context.Background(), makeToolRequest(map[string]any{
		"regulation": "GDPR",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleComplianceScan_EmptyCluster(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, internal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{}, nil
		},
	}
	s := newTestServerWithCompliance(t, mock)
	result, err := s.handleComplianceScan(context.Background(), makeToolRequest(map[string]any{
		"regulation": "SOC2",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleComplianceReport =====================

func TestHandleComplianceReport_MissingRegulation(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, internal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{}, nil
		},
	}
	s := newTestServerWithCompliance(t, mock)
	result, err := s.handleComplianceReport(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "regulation") {
		t.Errorf("expected regulation error, got: %s", text)
	}
}

func TestHandleComplianceReport_ValidRegulation(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, internal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "payments", Partitions: 3},
			}, nil
		},
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{}, nil
		},
	}
	s := newTestServerWithCompliance(t, mock)
	result, err := s.handleComplianceReport(context.Background(), makeToolRequest(map[string]any{
		"regulation": "HIPAA",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	var report map[string]any
	if err := json.Unmarshal([]byte(text), &report); err != nil {
		t.Fatalf("report is not valid JSON: %v", err)
	}

	requiredKeys := []string{"regulation", "recommendations", "generated_at"}
	for _, key := range requiredKeys {
		if report[key] == nil {
			t.Errorf("report missing key: %s", key)
		}
	}
}

// ===================== handleComplianceViolations =====================

func TestHandleComplianceViolations_ReturnsViolations(t *testing.T) {
	s := newTestServerWithCompliance(t, &kafka.MockKafkaManager{})
	result, err := s.handleComplianceViolations(context.Background(), makeToolRequest(map[string]any{
		"regulation": "GDPR",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)

	text := resultText(t, result)
	var data map[string]any
	if err := json.Unmarshal([]byte(text), &data); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if data["violations"] == nil && data["total"] == nil {
		t.Error("missing both violations and total keys")
	}
}

func TestHandleComplianceViolations_NoRegulationFilter(t *testing.T) {
	s := newTestServerWithCompliance(t, &kafka.MockKafkaManager{})
	result, err := s.handleComplianceViolations(context.Background(), makeToolRequest(nil))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== collectComplianceData =====================

func TestCollectComplianceData_SkipsInternalTopics(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, internal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "__consumer_offsets", IsInternal: true},
				{Name: "orders", Partitions: 3},
			}, nil
		},
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{}, nil
		},
	}
	s := newTestServerWithCompliance(t, mock)

	data, err := s.collectComplianceData(context.Background(), "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 1 {
		t.Fatalf("expected 1 topic (internal skipped), got %d", len(data))
	}
	if data[0].Name != "orders" {
		t.Errorf("expected 'orders', got %s", data[0].Name)
	}
}

func TestCollectComplianceData_EmptyCluster(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, internal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{}, nil
		},
	}
	s := newTestServerWithCompliance(t, mock)

	data, err := s.collectComplianceData(context.Background(), "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("expected 0 topics for empty cluster, got %d", len(data))
	}
}

func TestCollectComplianceData_TopicFilter(t *testing.T) {
	mock := &kafka.MockKafkaManager{
		ListTopicsFn: func(ctx context.Context, cluster string, internal bool) ([]kafka.TopicSummary, error) {
			return []kafka.TopicSummary{
				{Name: "orders"},
				{Name: "payments"},
			}, nil
		},
		ConsumeFn: func(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]kafka.Message, error) {
			return []kafka.Message{}, nil
		},
	}
	s := newTestServerWithCompliance(t, mock)

	data, err := s.collectComplianceData(context.Background(), "", "payments")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != 1 || data[0].Name != "payments" {
		t.Errorf("expected only 'payments', got %v", data)
	}
}
