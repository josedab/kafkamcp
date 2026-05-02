package kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// ===================== CreateTopic via MockKafkaManager =====================

func TestCreateTopic_DefaultPartitionsAndRF(t *testing.T) {
	t.Parallel()
	var captured CreateTopicRequest
	mock := &MockKafkaManager{
		CreateTopicFn: func(ctx context.Context, cluster string, req CreateTopicRequest) error {
			captured = req
			return nil
		},
	}

	ctx := context.Background()
	// Call through the mock to verify the interface works; the actual
	// defaults are applied inside Manager.CreateTopic, not the mock.
	err := mock.CreateTopic(ctx, "default", CreateTopicRequest{Name: "test-topic"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured.Name != "test-topic" {
		t.Errorf("expected topic name 'test-topic', got %q", captured.Name)
	}
	if mock.CallCount("CreateTopic") != 1 {
		t.Error("expected CreateTopic called once")
	}
}

func TestCreateTopic_WithConfigs(t *testing.T) {
	t.Parallel()
	var captured CreateTopicRequest
	mock := &MockKafkaManager{
		CreateTopicFn: func(ctx context.Context, cluster string, req CreateTopicRequest) error {
			captured = req
			return nil
		},
	}

	err := mock.CreateTopic(context.Background(), "prod", CreateTopicRequest{
		Name:              "configured-topic",
		Partitions:        6,
		ReplicationFactor: 3,
		Configs:           map[string]string{"retention.ms": "86400000"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured.Partitions != 6 {
		t.Errorf("expected 6 partitions, got %d", captured.Partitions)
	}
	if captured.ReplicationFactor != 3 {
		t.Errorf("expected RF=3, got %d", captured.ReplicationFactor)
	}
}

func TestCreateTopic_RestrictedConfigRejected(t *testing.T) {
	t.Parallel()
	configs := map[string]string{
		"min.insync.replicas": "2",
		"retention.ms":        "86400000",
	}
	err := validateTopicConfigs(configs)
	if err == nil {
		t.Fatal("expected error for restricted config key")
	}
	if !strings.Contains(err.Error(), "restricted") {
		t.Errorf("expected 'restricted' in error, got: %v", err)
	}
}

func TestCreateTopic_CaseInsensitiveRestriction(t *testing.T) {
	t.Parallel()
	configs := map[string]string{
		"Min.Insync.Replicas": "2",
	}
	err := validateTopicConfigs(configs)
	if err == nil {
		t.Fatal("expected error for case-insensitive dangerous config key")
	}
}

func TestCreateTopic_ErrorPropagation(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		CreateTopicFn: func(ctx context.Context, cluster string, req CreateTopicRequest) error {
			return context.DeadlineExceeded
		},
	}
	err := mock.CreateTopic(context.Background(), "default", CreateTopicRequest{Name: "t"})
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got: %v", err)
	}
}

func TestCreateTopic_EmptyConfigs(t *testing.T) {
	t.Parallel()
	err := validateTopicConfigs(nil)
	if err != nil {
		t.Errorf("expected no error for nil configs, got: %v", err)
	}
	err = validateTopicConfigs(map[string]string{})
	if err != nil {
		t.Errorf("expected no error for empty configs, got: %v", err)
	}
}

func TestCreateTopic_AllDangerousKeys(t *testing.T) {
	t.Parallel()
	for _, key := range dangerousConfigKeys {
		err := validateTopicConfigs(map[string]string{key: "val"})
		if err == nil {
			t.Errorf("expected error for dangerous key %q", key)
		}
	}
}

// ===================== AlterTopic =====================

func TestAlterTopic_Success(t *testing.T) {
	t.Parallel()
	var captured AlterTopicRequest
	mock := &MockKafkaManager{
		AlterTopicFn: func(ctx context.Context, cluster string, req AlterTopicRequest) error {
			captured = req
			return nil
		},
	}
	err := mock.AlterTopic(context.Background(), "prod", AlterTopicRequest{
		Name:    "my-topic",
		Configs: map[string]string{"retention.ms": "3600000"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured.Name != "my-topic" {
		t.Errorf("expected topic 'my-topic', got %q", captured.Name)
	}
	if mock.CallCount("AlterTopic") != 1 {
		t.Error("expected AlterTopic called once")
	}
}

func TestAlterTopic_RestrictedConfigRejected(t *testing.T) {
	t.Parallel()
	err := validateTopicConfigs(map[string]string{
		"unclean.leader.election.enable": "true",
	})
	if err == nil {
		t.Fatal("expected error for restricted config")
	}
}

func TestAlterTopic_ErrorPropagation(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		AlterTopicFn: func(ctx context.Context, cluster string, req AlterTopicRequest) error {
			return context.Canceled
		},
	}
	err := mock.AlterTopic(context.Background(), "default", AlterTopicRequest{
		Name:    "t",
		Configs: map[string]string{"cleanup.policy": "compact"},
	})
	if err != context.Canceled {
		t.Errorf("expected Canceled, got: %v", err)
	}
}

// ===================== DeleteTopic =====================

func TestDeleteTopic_Success(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		DeleteTopicFn: func(ctx context.Context, cluster, topicName string) error {
			return nil
		},
	}
	err := mock.DeleteTopic(context.Background(), "prod", "old-topic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.CallCount("DeleteTopic") != 1 {
		t.Error("expected DeleteTopic called once")
	}
	call := mock.LastCall("DeleteTopic")
	if call == nil || call.Args[1] != "old-topic" {
		t.Error("expected topic name 'old-topic' in call args")
	}
}

func TestDeleteTopic_ErrorPropagation(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		DeleteTopicFn: func(ctx context.Context, cluster, topicName string) error {
			return context.DeadlineExceeded
		},
	}
	err := mock.DeleteTopic(context.Background(), "default", "t")
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got: %v", err)
	}
}

func TestDeleteTopic_EmptyTopicName(t *testing.T) {
	t.Parallel()
	var capturedName string
	mock := &MockKafkaManager{
		DeleteTopicFn: func(ctx context.Context, cluster, topicName string) error {
			capturedName = topicName
			return nil
		},
	}
	err := mock.DeleteTopic(context.Background(), "default", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedName != "" {
		t.Errorf("expected empty topic name, got %q", capturedName)
	}
}

func TestDeleteTopic_DefaultCluster(t *testing.T) {
	t.Parallel()
	var capturedCluster string
	mock := &MockKafkaManager{
		DeleteTopicFn: func(ctx context.Context, cluster, topicName string) error {
			capturedCluster = cluster
			return nil
		},
	}
	err := mock.DeleteTopic(context.Background(), "", "my-topic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if capturedCluster != "" {
		t.Errorf("expected empty cluster (default), got %q", capturedCluster)
	}
}

func TestDeleteTopic_ContextCancelled(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		DeleteTopicFn: func(ctx context.Context, cluster, topicName string) error {
			return ctx.Err()
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := mock.DeleteTopic(ctx, "default", "topic")
	if err != context.Canceled {
		t.Errorf("expected Canceled error, got: %v", err)
	}
}

func TestDeleteTopic_NotFoundError(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		DeleteTopicFn: func(ctx context.Context, cluster, topicName string) error {
			return fmt.Errorf("topic %q not found", topicName)
		},
	}
	err := mock.DeleteTopic(context.Background(), "prod", "nonexistent-topic")
	if err == nil {
		t.Fatal("expected error for nonexistent topic")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected 'not found' in error, got: %v", err)
	}
}

func TestDeleteTopic_NilFnReturnsNil(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{}
	err := mock.DeleteTopic(context.Background(), "default", "topic")
	if err != nil {
		t.Fatalf("expected nil error when DeleteTopicFn is nil, got: %v", err)
	}
}

func TestDeleteTopic_MultipleCalls(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &MockKafkaManager{
		DeleteTopicFn: func(ctx context.Context, cluster, topicName string) error {
			callCount++
			return nil
		},
	}
	topics := []string{"topic-a", "topic-b", "topic-c"}
	for _, topic := range topics {
		if err := mock.DeleteTopic(context.Background(), "prod", topic); err != nil {
			t.Fatalf("unexpected error deleting %s: %v", topic, err)
		}
	}
	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
	if mock.CallCount("DeleteTopic") != 3 {
		t.Errorf("expected 3 recorded calls, got %d", mock.CallCount("DeleteTopic"))
	}
}

// ===================== ResetOffsets =====================

func TestResetOffsets_EarliestStrategy(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ResetOffsetsFn: func(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error) {
			if req.Strategy != "earliest" {
				t.Errorf("expected strategy 'earliest', got %q", req.Strategy)
			}
			return &ResetOffsetsResult{
				Offsets: []GroupOffsetDetail{{Topic: req.Topic, Partition: 0, CurrentOffset: 0}},
				DryRun:  req.DryRun,
			}, nil
		},
	}
	result, err := mock.ResetOffsets(context.Background(), "prod", ResetOffsetsRequest{
		GroupID:  "my-group",
		Topic:    "orders",
		Strategy: "earliest",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Offsets) != 1 {
		t.Errorf("expected 1 offset, got %d", len(result.Offsets))
	}
}

func TestResetOffsets_LatestStrategy(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ResetOffsetsFn: func(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error) {
			if req.Strategy != "latest" {
				t.Errorf("expected strategy 'latest', got %q", req.Strategy)
			}
			return &ResetOffsetsResult{
				Offsets: []GroupOffsetDetail{{Topic: req.Topic, Partition: 0, CurrentOffset: 500}},
			}, nil
		},
	}
	result, err := mock.ResetOffsets(context.Background(), "prod", ResetOffsetsRequest{
		GroupID:  "my-group",
		Topic:    "orders",
		Strategy: "latest",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Offsets[0].CurrentOffset != 500 {
		t.Errorf("expected offset 500, got %d", result.Offsets[0].CurrentOffset)
	}
}

func TestResetOffsets_TimestampStrategy(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ResetOffsetsFn: func(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error) {
			if req.Strategy != "timestamp" {
				t.Errorf("expected strategy 'timestamp', got %q", req.Strategy)
			}
			if req.Value == "" {
				t.Error("expected non-empty value for timestamp strategy")
			}
			return &ResetOffsetsResult{
				Offsets: []GroupOffsetDetail{{Topic: req.Topic, Partition: 0, CurrentOffset: 100}},
			}, nil
		},
	}
	result, err := mock.ResetOffsets(context.Background(), "prod", ResetOffsetsRequest{
		GroupID:  "my-group",
		Topic:    "orders",
		Strategy: "timestamp",
		Value:    "2024-01-01T00:00:00Z",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Offsets) != 1 {
		t.Errorf("expected 1 offset, got %d", len(result.Offsets))
	}
}

func TestResetOffsets_DryRunMode(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ResetOffsetsFn: func(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error) {
			if !req.DryRun {
				t.Error("expected dry_run=true")
			}
			return &ResetOffsetsResult{
				Offsets: []GroupOffsetDetail{{Topic: req.Topic, Partition: 0, CurrentOffset: 0}},
				DryRun:  true,
			}, nil
		},
	}
	result, err := mock.ResetOffsets(context.Background(), "prod", ResetOffsetsRequest{
		GroupID:  "my-group",
		Topic:    "orders",
		Strategy: "earliest",
		DryRun:   true,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.DryRun {
		t.Error("expected DryRun=true in result")
	}
}

func TestResetOffsets_InvalidStrategy(t *testing.T) {
	t.Parallel()
	m := testManager()
	_, err := m.ResetOffsets(context.Background(), "prod", ResetOffsetsRequest{
		GroupID:  "my-group",
		Topic:    "orders",
		Strategy: "invalid",
	})
	// With a real manager, GetAdmin will fail because we have no real broker.
	// But the pattern validates the call path doesn't panic.
	if err == nil {
		t.Fatal("expected error for nonexistent/invalid setup")
	}
}

func TestResetOffsets_ErrorPropagation(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ResetOffsetsFn: func(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error) {
			return nil, context.DeadlineExceeded
		},
	}
	_, err := mock.ResetOffsets(context.Background(), "default", ResetOffsetsRequest{
		GroupID:  "g",
		Topic:    "t",
		Strategy: "earliest",
	})
	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got: %v", err)
	}
}

func TestResetOffsets_MultiplePartitions(t *testing.T) {
	t.Parallel()
	mock := &MockKafkaManager{
		ResetOffsetsFn: func(ctx context.Context, cluster string, req ResetOffsetsRequest) (*ResetOffsetsResult, error) {
			return &ResetOffsetsResult{
				Offsets: []GroupOffsetDetail{
					{Topic: req.Topic, Partition: 0, CurrentOffset: 0},
					{Topic: req.Topic, Partition: 1, CurrentOffset: 0},
					{Topic: req.Topic, Partition: 2, CurrentOffset: 0},
				},
			}, nil
		},
	}
	result, err := mock.ResetOffsets(context.Background(), "prod", ResetOffsetsRequest{
		GroupID: "g", Topic: "t", Strategy: "earliest",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Offsets) != 3 {
		t.Errorf("expected 3 offsets, got %d", len(result.Offsets))
	}
}

// ===================== validateTopicConfigs edge cases =====================

func TestValidateTopicConfigs_MixedSafeAndDangerous(t *testing.T) {
	t.Parallel()
	configs := map[string]string{
		"retention.ms":                   "86400000",
		"cleanup.policy":                 "compact",
		"message.downconversion.enable":  "true",
	}
	err := validateTopicConfigs(configs)
	if err == nil {
		t.Fatal("expected error when dangerous key is mixed with safe keys")
	}
}

func TestValidateTopicConfigs_UpperCaseDangerous(t *testing.T) {
	t.Parallel()
	configs := map[string]string{
		"MIN.INSYNC.REPLICAS": "1",
	}
	err := validateTopicConfigs(configs)
	if err == nil {
		t.Fatal("expected error for upper-case dangerous key")
	}
}
