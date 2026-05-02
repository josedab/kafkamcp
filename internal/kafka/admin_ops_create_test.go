package kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestCreateTopic_DuplicateTopicError(t *testing.T) {
	t.Parallel()
	callCount := 0
	mock := &MockKafkaManager{
		CreateTopicFn: func(ctx context.Context, cluster string, req CreateTopicRequest) error {
			callCount++
			if callCount > 1 {
				return fmt.Errorf("topic %q already exists", req.Name)
			}
			return nil
		},
	}

	ctx := context.Background()
	err := mock.CreateTopic(ctx, "prod", CreateTopicRequest{Name: "orders", Partitions: 3, ReplicationFactor: 1})
	if err != nil {
		t.Fatalf("first create should succeed: %v", err)
	}

	err = mock.CreateTopic(ctx, "prod", CreateTopicRequest{Name: "orders", Partitions: 3, ReplicationFactor: 1})
	if err == nil {
		t.Fatal("expected error for duplicate topic creation")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("expected 'already exists' error, got: %v", err)
	}
}

func TestCreateTopic_CustomRetentionAndCompression(t *testing.T) {
	t.Parallel()
	var captured CreateTopicRequest
	mock := &MockKafkaManager{
		CreateTopicFn: func(ctx context.Context, cluster string, req CreateTopicRequest) error {
			captured = req
			return nil
		},
	}

	err := mock.CreateTopic(context.Background(), "prod", CreateTopicRequest{
		Name:              "events",
		Partitions:        12,
		ReplicationFactor: 3,
		Configs: map[string]string{
			"retention.ms":     "604800000",
			"compression.type": "snappy",
			"cleanup.policy":   "compact",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured.Configs["retention.ms"] != "604800000" {
		t.Error("expected retention.ms config preserved")
	}
	if captured.Configs["compression.type"] != "snappy" {
		t.Error("expected compression.type config preserved")
	}
}

func TestCreateTopic_SpecialCharsInName(t *testing.T) {
	t.Parallel()
	var capturedName string
	mock := &MockKafkaManager{
		CreateTopicFn: func(ctx context.Context, cluster string, req CreateTopicRequest) error {
			capturedName = req.Name
			return nil
		},
	}

	// Kafka allows dots, underscores, hyphens in topic names
	validNames := []string{
		"my.topic",
		"my-topic",
		"my_topic",
		"my.topic.v2",
		"namespace.team.events",
	}
	for _, name := range validNames {
		err := mock.CreateTopic(context.Background(), "prod", CreateTopicRequest{Name: name})
		if err != nil {
			t.Errorf("expected valid name %q to succeed: %v", name, err)
		}
		if capturedName != name {
			t.Errorf("expected captured name %q, got %q", name, capturedName)
		}
	}
}

func TestCreateTopic_ZeroPartitionsAndRF(t *testing.T) {
	t.Parallel()
	var captured CreateTopicRequest
	mock := &MockKafkaManager{
		CreateTopicFn: func(ctx context.Context, cluster string, req CreateTopicRequest) error {
			captured = req
			return nil
		},
	}

	err := mock.CreateTopic(context.Background(), "prod", CreateTopicRequest{
		Name:              "zero-defaults",
		Partitions:        0,
		ReplicationFactor: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Mock doesn't apply defaults; verify the request was passed through
	if captured.Partitions != 0 {
		t.Errorf("expected 0 partitions in mock pass-through, got %d", captured.Partitions)
	}
}

func TestValidateTopicConfigs_SingleSafeConfig(t *testing.T) {
	t.Parallel()
	configs := map[string]string{
		"retention.ms": "3600000",
	}
	err := validateTopicConfigs(configs)
	if err != nil {
		t.Errorf("expected no error for safe config, got: %v", err)
	}
}

func TestValidateTopicConfigs_MultipleUnsafe(t *testing.T) {
	t.Parallel()
	configs := map[string]string{
		"min.insync.replicas":            "1",
		"unclean.leader.election.enable": "true",
	}
	err := validateTopicConfigs(configs)
	if err == nil {
		t.Fatal("expected error for multiple dangerous keys")
	}
}

func TestCreateTopicRequest_FieldValues(t *testing.T) {
	t.Parallel()
	req := CreateTopicRequest{
		Name:              "test-topic",
		Partitions:        6,
		ReplicationFactor: 3,
		Configs:           map[string]string{"retention.ms": "86400000"},
	}

	if req.Name != "test-topic" {
		t.Errorf("expected name 'test-topic', got %q", req.Name)
	}
	if req.Partitions != 6 {
		t.Errorf("expected 6 partitions, got %d", req.Partitions)
	}
	if req.ReplicationFactor != 3 {
		t.Errorf("expected RF=3, got %d", req.ReplicationFactor)
	}
	if req.Configs["retention.ms"] != "86400000" {
		t.Error("expected retention.ms in configs")
	}
}
