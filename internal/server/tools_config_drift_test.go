package server

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kafkamcp/kafkamcp/internal/kafka"
)

// ===================== handleConfigDrift: compare action =====================

func TestHandleConfigDrift_Compare_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*kafka.ConfigCompareResult, error) {
			return &kafka.ConfigCompareResult{
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
				Diffs: []kafka.ConfigDiff{
					{Topic: "orders", Diffs: []kafka.ConfigValueDiff{
						{Key: "retention.ms", SourceValue: "86400000", TargetValue: "604800000"},
					}},
				},
				MatchCount: 5,
				DiffCount:  1,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":         "compare",
		"source_cluster": "prod",
		"target_cluster": "staging",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "retention.ms") {
		t.Errorf("expected diff in result, got: %s", text)
	}
	if mock.CallCount("CompareConfigs") != 1 {
		t.Error("expected CompareConfigs called once")
	}
}

func TestHandleConfigDrift_Compare_MissingSource(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":         "compare",
		"target_cluster": "staging",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "source_cluster") {
		t.Error("expected error mentioning source_cluster")
	}
}

func TestHandleConfigDrift_Compare_MissingTarget(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":         "compare",
		"source_cluster": "prod",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "target_cluster") {
		t.Error("expected error mentioning target_cluster")
	}
}

func TestHandleConfigDrift_Compare_WithTopicFilter(t *testing.T) {
	t.Parallel()
	var capturedTopic string
	mock := &kafka.MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*kafka.ConfigCompareResult, error) {
			capturedTopic = topic
			return &kafka.ConfigCompareResult{Diffs: nil}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	_, _ = s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":         "compare",
		"source_cluster": "prod",
		"target_cluster": "staging",
		"topic":          "orders",
	}))
	if capturedTopic != "orders" {
		t.Errorf("expected topic filter 'orders', got %q", capturedTopic)
	}
}

func TestHandleConfigDrift_Compare_EmptyDiff(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*kafka.ConfigCompareResult, error) {
			return &kafka.ConfigCompareResult{
				Diffs:      nil,
				MatchCount: 10,
				DiffCount:  0,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":         "compare",
		"source_cluster": "prod",
		"target_cluster": "staging",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleConfigDrift_Compare_Error(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		CompareConfigsFn: func(ctx context.Context, sourceCluster, targetCluster, topic string) (*kafka.ConfigCompareResult, error) {
			return nil, fmt.Errorf("cluster unreachable")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":         "compare",
		"source_cluster": "prod",
		"target_cluster": "staging",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

// ===================== handleConfigDrift: audit action =====================

func TestHandleConfigDrift_Audit_Success(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, topic string, template map[string]string) (*kafka.ConfigAuditResult, error) {
			return &kafka.ConfigAuditResult{
				Cluster: cluster,
				Deviations: []kafka.ConfigDeviation{
					{Topic: "orders", Key: "retention.ms", Expected: "86400000", Actual: "604800000"},
				},
				Compliant:    4,
				NonCompliant: 1,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":   "audit",
		"cluster":  "prod",
		"template": map[string]any{"retention.ms": "86400000"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
	text := resultText(t, result)
	if !strings.Contains(text, "deviations") {
		t.Errorf("expected 'deviations' in result, got: %s", text)
	}
}

func TestHandleConfigDrift_Audit_NilTemplate(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, topic string, template map[string]string) (*kafka.ConfigAuditResult, error) {
			return &kafka.ConfigAuditResult{
				Cluster:   cluster,
				Compliant: 5,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":  "audit",
		"cluster": "prod",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

func TestHandleConfigDrift_Audit_Error(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, topic string, template map[string]string) (*kafka.ConfigAuditResult, error) {
			return nil, fmt.Errorf("access denied")
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":  "audit",
		"cluster": "prod",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}

func TestHandleConfigDrift_Audit_AllCompliant(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{
		AuditConfigsFn: func(ctx context.Context, cluster, topic string, template map[string]string) (*kafka.ConfigAuditResult, error) {
			return &kafka.ConfigAuditResult{
				Cluster:      cluster,
				Deviations:   nil,
				Compliant:    10,
				NonCompliant: 0,
			}, nil
		},
	}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":   "audit",
		"cluster":  "prod",
		"template": map[string]any{"retention.ms": "86400000"},
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertNotError(t, result)
}

// ===================== handleConfigDrift: invalid action =====================

func TestHandleConfigDrift_InvalidAction_Mock(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServerWithMock(t, mock)
	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action": "invalid",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
	if !strings.Contains(resultText(t, result), "unknown action") {
		t.Error("expected 'unknown action' in error")
	}
}

// ===================== handleConfigDrift: auth denied =====================

func TestHandleConfigDrift_AuthDenied_Mock(t *testing.T) {
	t.Parallel()
	mock := &kafka.MockKafkaManager{}
	s := newTestServer(t, withDefaultDeny)
	s.kafkaManager = mock

	result, err := s.handleConfigDrift(context.Background(), makeToolRequest(map[string]any{
		"action":         "compare",
		"source_cluster": "prod",
		"target_cluster": "staging",
	}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertIsError(t, result)
}
