package kafka

import (
	"context"
	"strings"
	"testing"
)

// --------------- validateTopicConfigs ---------------

func TestValidateTopicConfigs_DangerousKeys(t *testing.T) {
	dangerous := []string{
		"min.insync.replicas",
		"unclean.leader.election.enable",
		"message.downconversion.enable",
	}
	for _, key := range dangerous {
		err := validateTopicConfigs(map[string]string{key: "1"})
		if err == nil {
			t.Errorf("expected error for dangerous key %q", key)
		}
		if !strings.Contains(err.Error(), "restricted") {
			t.Errorf("expected 'restricted' in error for key %q, got: %v", key, err)
		}
	}
}

func TestValidateTopicConfigs_SafeKeys(t *testing.T) {
	safe := map[string]string{
		"retention.ms":   "86400000",
		"cleanup.policy": "compact",
		"segment.bytes":  "1073741824",
	}
	if err := validateTopicConfigs(safe); err != nil {
		t.Errorf("expected no error for safe configs, got: %v", err)
	}
}

// --------------- SearchMessages ---------------

func TestSearchMessages_NonexistentCluster(t *testing.T) {
	m := testManager()
	_, _, err := m.SearchMessages(context.Background(), "nonexistent", SearchRequest{
		Topic: "test-topic",
		Limit: 10,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

// --------------- AnalyzeDLQ ---------------

func TestAnalyzeDLQ_NonexistentCluster(t *testing.T) {
	m := testManager()
	_, err := m.AnalyzeDLQ(context.Background(), "nonexistent", "dlq-topic", 100)
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

// --------------- ResetOffsets ---------------

func TestResetOffsets_NonexistentCluster(t *testing.T) {
	m := testManager()
	_, err := m.ResetOffsets(context.Background(), "nonexistent", ResetOffsetsRequest{
		GroupID:  "test-group",
		Topic:    "test-topic",
		Strategy: "earliest",
	})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

// --------------- CreateTopic ---------------

func TestCreateTopic_NonexistentCluster(t *testing.T) {
	m := testManager()
	err := m.CreateTopic(context.Background(), "nonexistent", CreateTopicRequest{
		Name:       "new-topic",
		Partitions: 3,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

func TestCreateTopic_DangerousConfig(t *testing.T) {
	err := validateTopicConfigs(map[string]string{"min.insync.replicas": "1"})
	if err == nil {
		t.Fatal("expected error for dangerous config key")
	}
}

// --------------- AlterTopic ---------------

func TestAlterTopic_NonexistentCluster(t *testing.T) {
	m := testManager()
	err := m.AlterTopic(context.Background(), "nonexistent", AlterTopicRequest{
		Name:    "my-topic",
		Configs: map[string]string{"retention.ms": "1000"},
	})
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}

// --------------- DeleteTopic ---------------

func TestDeleteTopic_NonexistentCluster(t *testing.T) {
	m := testManager()
	err := m.DeleteTopic(context.Background(), "nonexistent", "my-topic")
	if err == nil {
		t.Fatal("expected error for nonexistent cluster")
	}
}
