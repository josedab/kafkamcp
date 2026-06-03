//go:build integration

package kafka

// ProviderContractTests defines behavior contract tests that verify a Kafka-
// compatible broker satisfies the core behavioral contracts expected by KafkaMCP.
//
// These tests are vendor-neutral: they run against Apache Kafka or Redpanda
// depending on the KAFKA_PROVIDER and KAFKA_BROKERS environment variables.
//
// Profiles:
//
//	KAFKA_PROVIDER=kafka     (default) — Apache Kafka
//	KAFKA_PROVIDER=redpanda  — Redpanda (Kafka-compatible)
//	KAFKA_BROKERS=host:port  — override broker address (default: localhost:9092)
//
// Usage:
//
//	# Against Apache Kafka (default docker-compose):
//	go test -tags=integration -run TestKafkaProviderContract ./internal/kafka/
//
//	# Against Redpanda:
//	KAFKA_PROVIDER=redpanda go test -tags=integration -run TestKafkaProviderContract ./internal/kafka/

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/josedab/kafkamcp/internal/config"
)

// contractBrokers returns the bootstrap server for contract tests.
func contractBrokers() string {
	if b := os.Getenv("KAFKA_BROKERS"); b != "" {
		return b
	}
	return "localhost:9092"
}

// contractProvider returns the provider name for display.
func contractProvider() string {
	p := strings.ToLower(os.Getenv("KAFKA_PROVIDER"))
	if p == "" {
		return "kafka"
	}
	return p
}

// contractManager creates a Manager connected to the contract-test broker.
func contractManager(t *testing.T) *Manager {
	t.Helper()
	brokers := contractBrokers()
	provider := contractProvider()
	if provider != "kafka" && provider != "redpanda" {
		t.Fatalf("unsupported KAFKA_PROVIDER %q; expected kafka or redpanda", provider)
	}
	cfg := &config.Config{
		Clusters: []config.ClusterConfig{
			{
				Name:             "contract",
				BootstrapServers: brokers,
				SecurityProtocol: "PLAINTEXT",
				Default:          true,
			},
		},
	}
	m, err := NewManager(cfg, slog.Default())
	if err != nil {
		t.Skipf("skipping contract test: cannot connect to %s at %s: %v",
			contractProvider(), brokers, err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

// contractTopic returns a unique topic name for this test run to avoid collisions
// between parallel runs or leftover data from previous runs.
func contractTopic(t *testing.T, suffix string) string {
	t.Helper()
	// Use t.Name() to make it unique; sanitize special chars.
	sanitized := strings.NewReplacer("/", "-", " ", "-", ":", "-").Replace(t.Name())
	if len(sanitized) > 40 {
		sanitized = sanitized[:40]
	}
	return fmt.Sprintf("contract-test.%s.%s.%d", sanitized, suffix, time.Now().UnixMilli())
}

// cleanupTopic deletes a topic, ignoring "not found" errors.
func cleanupTopic(t *testing.T, m *Manager, cluster, topic string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.DeleteTopic(ctx, cluster, topic); err != nil {
		if !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "UNKNOWN_TOPIC") {
			t.Logf("cleanup: could not delete %s: %v", topic, err)
		}
	}
}

func waitForTopic(t *testing.T, m *Manager, cluster, topic string, partitions int) *TopicDetail {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		detail, err := m.DescribeTopic(context.Background(), cluster, topic)
		if err == nil && len(detail.Partitions) == partitions {
			return detail
		}
		lastErr = err
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("[%s] topic %q did not become ready with %d partitions: %v", contractProvider(), topic, partitions, lastErr)
	return nil
}

func waitForTopicListed(t *testing.T, m *Manager, cluster, topic string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		topics, err := m.ListTopics(context.Background(), cluster, false)
		if err == nil {
			for _, candidate := range topics {
				if candidate.Name == topic {
					return
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("[%s] created topic %q did not appear in ListTopics", contractProvider(), topic)
}

func waitForTopicAbsent(t *testing.T, m *Manager, cluster, topic string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		topics, err := m.ListTopics(context.Background(), cluster, false)
		if err == nil {
			found := false
			for _, candidate := range topics {
				found = found || candidate.Name == topic
			}
			if !found {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("[%s] deleted topic %q remained visible", contractProvider(), topic)
}

// ===== Contract: Topic Lifecycle =====

// TestKafkaProviderContract_TopicCreate verifies topics can be created and described.
func TestKafkaProviderContract_TopicCreate(t *testing.T) {
	m := contractManager(t)
	topic := contractTopic(t, "create")
	t.Cleanup(func() { cleanupTopic(t, m, "contract", topic) })

	ctx := context.Background()
	err := m.CreateTopic(ctx, "contract", CreateTopicRequest{
		Name:              topic,
		Partitions:        2,
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("[%s] CreateTopic(%s): %v", contractProvider(), topic, err)
	}

	detail := waitForTopic(t, m, "contract", topic, 2)
	if detail.Name != topic {
		t.Errorf("[%s] DescribeTopic name = %q, want %q", contractProvider(), detail.Name, topic)
	}
	if len(detail.Partitions) != 2 {
		t.Errorf("[%s] DescribeTopic partitions = %d, want 2", contractProvider(), len(detail.Partitions))
	}
}

// TestKafkaProviderContract_TopicList verifies listing topics returns the created topic.
func TestKafkaProviderContract_TopicList(t *testing.T) {
	m := contractManager(t)
	topic := contractTopic(t, "list")
	t.Cleanup(func() { cleanupTopic(t, m, "contract", topic) })

	ctx := context.Background()
	if err := m.CreateTopic(ctx, "contract", CreateTopicRequest{
		Name: topic, Partitions: 1, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("[%s] CreateTopic: %v", contractProvider(), err)
	}
	waitForTopic(t, m, "contract", topic, 1)

	waitForTopicListed(t, m, "contract", topic)
}

// TestKafkaProviderContract_TopicDelete verifies topics can be deleted.
func TestKafkaProviderContract_TopicDelete(t *testing.T) {
	m := contractManager(t)
	topic := contractTopic(t, "delete")

	ctx := context.Background()
	if err := m.CreateTopic(ctx, "contract", CreateTopicRequest{
		Name: topic, Partitions: 1, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("[%s] CreateTopic: %v", contractProvider(), err)
	}
	waitForTopic(t, m, "contract", topic, 1)

	if err := m.DeleteTopic(ctx, "contract", topic); err != nil {
		t.Fatalf("[%s] DeleteTopic(%s): %v", contractProvider(), topic, err)
	}

	waitForTopicAbsent(t, m, "contract", topic)
}

// ===== Contract: Produce / Consume =====

// TestKafkaProviderContract_ProduceConsume verifies a message can be produced and consumed.
func TestKafkaProviderContract_ProduceConsume(t *testing.T) {
	m := contractManager(t)
	topic := contractTopic(t, "pc")
	t.Cleanup(func() { cleanupTopic(t, m, "contract", topic) })

	ctx := context.Background()
	if err := m.CreateTopic(ctx, "contract", CreateTopicRequest{
		Name: topic, Partitions: 1, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("[%s] CreateTopic: %v", contractProvider(), err)
	}
	waitForTopic(t, m, "contract", topic, 1)

	// Produce a message
	msg := ProduceMessage{Key: "k1", Value: `{"hello":"world"}`}
	results, err := m.Produce(ctx, "contract", topic, []ProduceMessage{msg}, ProduceOptions{Acks: "all"})
	if err != nil {
		t.Fatalf("[%s] Produce: %v", contractProvider(), err)
	}
	if len(results) == 0 {
		t.Fatalf("[%s] Produce: no results returned", contractProvider())
	}
	if results[0].Status != "delivered" {
		t.Errorf("[%s] Produce result status = %q, want 'delivered'", contractProvider(), results[0].Status)
	}

	// Consume it back
	msgs, err := m.Consume(ctx, "contract", topic, 0, "earliest", 10, 5000, "auto")
	if err != nil {
		t.Fatalf("[%s] Consume: %v", contractProvider(), err)
	}
	if len(msgs) == 0 {
		t.Errorf("[%s] Consume: no messages returned; expected at least 1", contractProvider())
	}
}

// TestKafkaProviderContract_ProduceHeaders verifies message headers survive round-trip.
func TestKafkaProviderContract_ProduceHeaders(t *testing.T) {
	m := contractManager(t)
	topic := contractTopic(t, "headers")
	t.Cleanup(func() { cleanupTopic(t, m, "contract", topic) })

	ctx := context.Background()
	if err := m.CreateTopic(ctx, "contract", CreateTopicRequest{
		Name: topic, Partitions: 1, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("[%s] CreateTopic: %v", contractProvider(), err)
	}
	waitForTopic(t, m, "contract", topic, 1)

	msg := ProduceMessage{
		Key:   "hdr-test",
		Value: `"payload"`,
		Headers: map[string]string{
			"x-trace-id":     "trace-123",
			"x-content-type": "application/json",
		},
	}
	_, err := m.Produce(ctx, "contract", topic, []ProduceMessage{msg}, ProduceOptions{Acks: "all"})
	if err != nil {
		t.Fatalf("[%s] Produce with headers: %v", contractProvider(), err)
	}

	msgs, err := m.Consume(ctx, "contract", topic, 0, "earliest", 10, 5000, "auto")
	if err != nil {
		t.Fatalf("[%s] Consume: %v", contractProvider(), err)
	}
	if len(msgs) == 0 {
		t.Fatalf("[%s] Consume: no messages returned", contractProvider())
	}
	if msgs[0].Headers == nil {
		t.Fatalf("[%s] Consume: headers are nil", contractProvider())
	}
	if msgs[0].Headers["x-trace-id"] != "trace-123" {
		t.Errorf("[%s] header x-trace-id = %q, want 'trace-123'", contractProvider(), msgs[0].Headers["x-trace-id"])
	}
}

// ===== Contract: Cluster Metadata =====

// TestKafkaProviderContract_ClusterMetadata verifies the broker reports valid metadata.
func TestKafkaProviderContract_ClusterMetadata(t *testing.T) {
	m := contractManager(t)

	ctx := context.Background()
	meta, err := m.GetClusterMetadata(ctx, "contract")
	if err != nil {
		t.Fatalf("[%s] GetClusterMetadata: %v", contractProvider(), err)
	}
	if meta.BrokerCount < 1 {
		t.Errorf("[%s] GetClusterMetadata: BrokerCount = %d, want >= 1", contractProvider(), meta.BrokerCount)
	}
	if meta.ClusterID == "" {
		t.Errorf("[%s] GetClusterMetadata: ClusterID is empty", contractProvider())
	}
	if len(meta.Brokers) == 0 {
		t.Errorf("[%s] GetClusterMetadata: Brokers is empty", contractProvider())
	}
}

// TestKafkaProviderContract_Ping verifies the broker is reachable.
func TestKafkaProviderContract_Ping(t *testing.T) {
	m := contractManager(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.Ping(ctx); err != nil {
		t.Errorf("[%s] Ping: %v", contractProvider(), err)
	}
}

// ===== Contract: Consumer Groups =====

// TestKafkaProviderContract_ConsumerGroupList verifies consumer-group listing
// works (even if no groups are present, the call must not error).
func TestKafkaProviderContract_ConsumerGroupList(t *testing.T) {
	m := contractManager(t)

	ctx := context.Background()
	groups, err := m.ListConsumerGroups(ctx, "contract")
	if err != nil {
		t.Fatalf("[%s] ListConsumerGroups: %v", contractProvider(), err)
	}
	// Empty is fine; we just need the call to succeed.
	t.Logf("[%s] ListConsumerGroups: %d groups", contractProvider(), len(groups))
}

// TestKafkaProviderContract_ConsumerGroupDescribe tests describing a group
// by first subscribing a consumer to register one.
func TestKafkaProviderContract_ConsumerGroupDescribe(t *testing.T) {
	m := contractManager(t)
	topic := contractTopic(t, "cg")
	groupID := fmt.Sprintf("contract-test-cg-%d", time.Now().UnixMilli())
	t.Cleanup(func() { cleanupTopic(t, m, "contract", topic) })

	ctx := context.Background()
	if err := m.CreateTopic(ctx, "contract", CreateTopicRequest{
		Name: topic, Partitions: 1, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("[%s] CreateTopic: %v", contractProvider(), err)
	}
	waitForTopic(t, m, "contract", topic, 1)

	// Produce a message so the topic has content
	_, err := m.Produce(ctx, "contract", topic,
		[]ProduceMessage{{Key: "k", Value: `"v"`}}, ProduceOptions{Acks: "all"})
	if err != nil {
		t.Fatalf("[%s] Produce: %v", contractProvider(), err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(contractBrokers(), ",")...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatalf("[%s] create group consumer: %v", contractProvider(), err)
	}
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	fetches := consumer.PollFetches(pollCtx)
	cancel()
	if errs := fetches.Errors(); len(errs) > 0 {
		consumer.Close()
		t.Fatalf("[%s] group consumer fetch: %v", contractProvider(), errs)
	}
	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		consumer.Close()
		t.Fatalf("[%s] commit group offsets: %v", contractProvider(), err)
	}
	consumer.Close()

	// Give Kafka a moment to register the group
	time.Sleep(500 * time.Millisecond)

	var detail *ConsumerGroupDetail
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		detail, err = m.DescribeConsumerGroup(ctx, "contract", groupID)
		if err == nil {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("[%s] DescribeConsumerGroup(%s) did not become visible: %v", contractProvider(), groupID, err)
	}
	if detail.GroupID != groupID {
		t.Errorf("[%s] DescribeConsumerGroup ID = %q, want %q", contractProvider(), detail.GroupID, groupID)
	}
}

// ===== Contract: Topic Configs =====

// TestKafkaProviderContract_TopicAlterConfig verifies topic configs can be changed.
func TestKafkaProviderContract_TopicAlterConfig(t *testing.T) {
	m := contractManager(t)
	topic := contractTopic(t, "altcfg")
	t.Cleanup(func() { cleanupTopic(t, m, "contract", topic) })

	ctx := context.Background()
	if err := m.CreateTopic(ctx, "contract", CreateTopicRequest{
		Name: topic, Partitions: 1, ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("[%s] CreateTopic: %v", contractProvider(), err)
	}
	waitForTopic(t, m, "contract", topic, 1)

	retention := "7200000"
	err := m.AlterTopic(ctx, "contract", AlterTopicRequest{
		Name:    topic,
		Configs: map[string]string{"retention.ms": retention},
	})
	if err != nil {
		t.Fatalf("[%s] AlterTopic: %v", contractProvider(), err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		detail, describeErr := m.DescribeTopic(ctx, "contract", topic)
		if describeErr == nil && detail.Configs["retention.ms"] == retention {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("[%s] retention.ms did not converge to %q", contractProvider(), retention)
}
