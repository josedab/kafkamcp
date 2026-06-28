// Package main implements the incident-triage demo scenario for KafkaMCP.
//
// It creates a set of prefixed Kafka topics that illustrate real-world
// failure patterns: consumer lag/backlog, DLQ records with error headers,
// and schema/data-shape drift. All operations are scoped to the demo prefix
// so that cleanup is safe and never touches production topics.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/josedab/kafkamcp/internal/config"
	"github.com/josedab/kafkamcp/internal/kafka"
)

const (
	DefaultPrefix           = "incident-demo."
	DefaultBootstrapServers = "localhost:9092"
	OperationTimeout        = 15 * time.Second
)

type TopicSpec struct {
	Name       string
	Partitions int32
	Configs    map[string]string
}

func demoTopics(prefix string) []TopicSpec {
	return []TopicSpec{
		{Name: prefix + "orders", Partitions: 3, Configs: map[string]string{"retention.ms": "86400000", "cleanup.policy": "delete"}},
		{Name: prefix + "orders.dlq", Partitions: 1, Configs: map[string]string{"retention.ms": "604800000", "cleanup.policy": "delete"}},
		{Name: prefix + "payments", Partitions: 2, Configs: map[string]string{"retention.ms": "86400000"}},
		{Name: prefix + "payments.dlq", Partitions: 1, Configs: map[string]string{"retention.ms": "604800000"}},
		{Name: prefix + "notifications", Partitions: 1, Configs: map[string]string{"retention.ms": "86400000"}},
	}
}

type DemoMessage struct {
	ID        string         `json:"id"`
	EventType string         `json:"event_type"`
	Timestamp string         `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

type DLQRecord struct {
	OriginalTopic     string `json:"original_topic"`
	OriginalOffset    int64  `json:"original_offset"`
	OriginalPartition int32  `json:"original_partition"`
	FailureReason     string `json:"failure_reason"`
	RetryCount        int    `json:"retry_count"`
	FirstFailedAt     string `json:"first_failed_at"`
	Payload           any    `json:"payload"`
}

type Scenario struct {
	prefix    string
	brokers   string
	manager   *kafka.Manager
	rawClient *kgo.Client
	logger    *slog.Logger
}

type Config struct {
	Prefix           string
	BootstrapServers string
	Logger           *slog.Logger
}

func New(cfg Config) (*Scenario, error) {
	prefix := cfg.Prefix
	if prefix == "" {
		prefix = DefaultPrefix
	}
	if !strings.HasSuffix(prefix, ".") {
		prefix += "."
	}
	brokers := cfg.BootstrapServers
	if brokers == "" {
		brokers = DefaultBootstrapServers
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	kmcfg := &config.Config{
		Clusters: []config.ClusterConfig{
			{Name: "demo", BootstrapServers: brokers, SecurityProtocol: "PLAINTEXT", Default: true},
		},
	}
	manager, err := kafka.NewManager(kmcfg, logger)
	if err != nil {
		return nil, fmt.Errorf("connecting to kafka at %s: %w", brokers, err)
	}

	rawClient, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...), kgo.ClientID("incident-demo"))
	if err != nil {
		manager.Close()
		return nil, fmt.Errorf("creating raw kafka client: %w", err)
	}

	return &Scenario{prefix: prefix, brokers: brokers, manager: manager, rawClient: rawClient, logger: logger}, nil
}

func (s *Scenario) Close() {
	if s.rawClient != nil {
		s.rawClient.Close()
	}
	if s.manager != nil {
		s.manager.Close()
	}
}

func (s *Scenario) Prefix() string { return s.prefix }

func ValidatePrefix(prefix string) error {
	if prefix == "" {
		return fmt.Errorf("prefix must not be empty")
	}
	if prefix == "." || prefix == "_" || len(prefix) < 3 {
		return fmt.Errorf("prefix %q is too short or ambiguous; use a descriptive prefix like 'incident-demo.'", prefix)
	}
	for _, danger := range []string{"__", "connect-", "schema-", "_schemas"} {
		if strings.HasPrefix(prefix, danger) {
			return fmt.Errorf("prefix %q matches reserved internal prefix %q", prefix, danger)
		}
	}
	return nil
}

func (s *Scenario) Setup(ctx context.Context) error {
	s.logger.Info("setting up incident-demo scenario", "prefix", s.prefix)
	if err := s.createTopics(ctx); err != nil {
		return fmt.Errorf("creating topics: %w", err)
	}
	populated, err := s.populationState(ctx)
	if err != nil {
		return err
	}
	if populated == "complete" {
		return s.ensureLaggingConsumerGroup(ctx)
	}
	if populated == "partial" {
		return fmt.Errorf("demo topics contain partial data; run cleanup before setup to preserve a deterministic scenario")
	}
	if err := s.produceHealthyMessages(ctx); err != nil {
		return fmt.Errorf("producing healthy messages: %w", err)
	}
	if err := s.produceBacklog(ctx); err != nil {
		return fmt.Errorf("producing backlog: %w", err)
	}
	if err := s.produceDLQRecords(ctx); err != nil {
		return fmt.Errorf("producing DLQ records: %w", err)
	}
	if err := s.produceDriftMessages(ctx); err != nil {
		return fmt.Errorf("producing schema-drift messages: %w", err)
	}
	if err := s.ensureLaggingConsumerGroup(ctx); err != nil {
		return fmt.Errorf("creating lagging consumer group: %w", err)
	}
	s.logger.Info("incident-demo scenario setup complete", "prefix", s.prefix)
	return nil
}

func (s *Scenario) populationState(ctx context.Context) (string, error) {
	expected := expectedMessageCounts(s.prefix)
	allZero := true
	complete := true
	for topic, minimum := range expected {
		detail, err := s.manager.DescribeTopic(ctx, "demo", topic)
		if err != nil {
			return "", fmt.Errorf("describing %s: %w", topic, err)
		}
		var count int64
		for _, partition := range detail.Partitions {
			count += partition.LatestOffset - partition.EarliestOffset
		}
		if count > 0 {
			allZero = false
		}
		if count < minimum {
			complete = false
		}
	}
	switch {
	case complete:
		return "complete", nil
	case allZero:
		return "empty", nil
	default:
		return "partial", nil
	}
}

func expectedMessageCounts(prefix string) map[string]int64 {
	return map[string]int64{
		prefix + "orders":        54,
		prefix + "orders.dlq":    2,
		prefix + "payments":      1,
		prefix + "payments.dlq":  1,
		prefix + "notifications": 1,
	}
}

func (s *Scenario) lagGroupID() string {
	return strings.TrimSuffix(s.prefix, ".") + "-lagging-consumer"
}

func (s *Scenario) ensureLaggingConsumerGroup(ctx context.Context) error {
	groupID := s.lagGroupID()
	groups, err := s.manager.ListConsumerGroups(ctx, "demo")
	if err == nil {
		for _, group := range groups {
			if group.GroupID == groupID {
				if detail, describeErr := s.manager.DescribeConsumerGroup(ctx, "demo", groupID); describeErr == nil && detail.TotalLag > 0 {
					return nil
				}
				admin := kadm.NewClient(s.rawClient)
				_, _ = admin.DeleteGroup(ctx, groupID)
				break
			}
		}
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(s.brokers, ",")...),
		kgo.ClientID("incident-demo-lag-seeder"),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(s.prefix+"orders"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("creating lag consumer: %w", err)
	}
	defer consumer.Close()

	pollCtx, cancel := context.WithTimeout(ctx, OperationTimeout)
	defer cancel()
	fetches := consumer.PollRecords(pollCtx, 5)
	if fetches.IsClientClosed() {
		return fmt.Errorf("lag consumer closed before polling")
	}
	if errs := fetches.Errors(); len(errs) > 0 {
		return fmt.Errorf("polling lag seed records: %w", errs[0].Err)
	}
	records := make([]*kgo.Record, 0, fetches.NumRecords())
	fetches.EachRecord(func(record *kgo.Record) {
		records = append(records, record)
	})
	if len(records) == 0 {
		return fmt.Errorf("no records available to establish committed lag")
	}
	if err := consumer.CommitRecords(pollCtx, records...); err != nil {
		return fmt.Errorf("committing lag seed offsets: %w", err)
	}
	return nil
}

func (s *Scenario) createTopics(ctx context.Context) error {
	for _, spec := range demoTopics(s.prefix) {
		tctx, cancel := context.WithTimeout(ctx, OperationTimeout)
		err := s.manager.CreateTopic(tctx, "demo", kafka.CreateTopicRequest{
			Name: spec.Name, Partitions: spec.Partitions, ReplicationFactor: 1, Configs: spec.Configs,
		})
		cancel()
		if err != nil {
			if isAlreadyExistsErr(err) {
				s.logger.Info("topic already exists", "topic", spec.Name)
				continue
			}
			return fmt.Errorf("creating topic %s: %w", spec.Name, err)
		}
		s.logger.Info("created topic", "topic", spec.Name)
	}
	return nil
}

func isAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS") || strings.Contains(err.Error(), "already exists")
}

func (s *Scenario) produceHealthyMessages(ctx context.Context) error {
	now := time.Now().UTC()
	return s.produceRecords(ctx, []*kgo.Record{
		makeRecord(s.prefix+"orders", "order-1", DemoMessage{ID: "ord-001", EventType: "order.created", Timestamp: now.Add(-10 * time.Minute).Format(time.RFC3339), Payload: map[string]any{"customer_id": "cust-42", "amount": 99.99, "currency": "USD"}}),
		makeRecord(s.prefix+"orders", "order-2", DemoMessage{ID: "ord-002", EventType: "order.shipped", Timestamp: now.Add(-5 * time.Minute).Format(time.RFC3339), Payload: map[string]any{"customer_id": "cust-17", "amount": 14.50, "tracking": "TRK-9944"}}),
		makeRecord(s.prefix+"payments", "payment-1", DemoMessage{ID: "pay-001", EventType: "payment.completed", Timestamp: now.Add(-8 * time.Minute).Format(time.RFC3339), Payload: map[string]any{"order_id": "ord-001", "method": "card", "amount": 99.99}}),
		makeRecord(s.prefix+"notifications", "notif-1", DemoMessage{ID: "notif-001", EventType: "notification.sent", Timestamp: now.Add(-3 * time.Minute).Format(time.RFC3339), Payload: map[string]any{"user_id": "cust-42", "channel": "email"}}),
	}, "healthy messages")
}

func (s *Scenario) produceBacklog(ctx context.Context) error {
	now := time.Now().UTC()
	records := make([]*kgo.Record, 0, 50)
	for i := 0; i < 50; i++ {
		msg := DemoMessage{ID: fmt.Sprintf("ord-%03d", 100+i), EventType: "order.created", Timestamp: now.Add(-time.Duration(50-i) * time.Minute).Format(time.RFC3339), Payload: map[string]any{"customer_id": fmt.Sprintf("cust-%d", 100+i), "amount": float64(10+i) * 5}}
		records = append(records, makeRecord(s.prefix+"orders", fmt.Sprintf("order-backlog-%d", i), msg))
	}
	return s.produceRecords(ctx, records, "backlog messages")
}

func (s *Scenario) produceDLQRecords(ctx context.Context) error {
	now := time.Now().UTC()
	type dlqCase struct {
		originalTopic   string
		dlqTopic        string
		reason          string
		retries         int
		originalPayload any
	}
	cases := []dlqCase{
		{s.prefix + "orders", s.prefix + "orders.dlq", "deserialization_failure: unknown field 'legacy_promo_code' in OrderCreated v1", 3, map[string]any{"customer_id": "cust-99", "amount": 42.00, "legacy_promo_code": "PROMO2019"}},
		{s.prefix + "orders", s.prefix + "orders.dlq", "downstream_timeout: payment service unreachable after 3 retries", 3, map[string]any{"customer_id": "cust-77", "amount": 199.99}},
		{s.prefix + "payments", s.prefix + "payments.dlq", "validation_failure: amount must be positive, got -5.00", 1, map[string]any{"order_id": "ord-bad", "amount": -5.00}},
	}
	records := make([]*kgo.Record, 0, len(cases))
	for i, c := range cases {
		rec := makeRecord(c.dlqTopic, fmt.Sprintf("dlq-%d", i), DLQRecord{
			OriginalTopic: c.originalTopic, OriginalOffset: int64(100 + i), OriginalPartition: 0,
			FailureReason: c.reason, RetryCount: c.retries,
			FirstFailedAt: now.Add(-time.Duration(c.retries+1) * time.Minute).Format(time.RFC3339),
			Payload:       c.originalPayload,
		})
		rec.Headers = append(rec.Headers,
			kgo.RecordHeader{Key: "x-failure-reason", Value: []byte(c.reason)},
			kgo.RecordHeader{Key: "x-retry-count", Value: []byte(fmt.Sprintf("%d", c.retries))},
			kgo.RecordHeader{Key: "x-original-topic", Value: []byte(c.originalTopic)},
			kgo.RecordHeader{Key: "x-first-failed-at", Value: []byte(now.Add(-time.Duration(c.retries+1) * time.Minute).Format(time.RFC3339))},
			kgo.RecordHeader{Key: "x-consumer-group", Value: []byte("order-processor")},
		)
		records = append(records, rec)
	}
	return s.produceRecords(ctx, records, "DLQ records")
}

func (s *Scenario) produceDriftMessages(ctx context.Context) error {
	now := time.Now().UTC()
	driftV2, _ := json.Marshal(map[string]any{"id": "ord-drift-001", "event_type": "order.created", "timestamp": now.Format(time.RFC3339), "amount": "149.99", "currency": "EUR", "metadata": map[string]any{"source": "mobile-app", "version": "2.1.0"}})
	driftMissing, _ := json.Marshal(map[string]any{"event_type": "order.created", "timestamp": now.Format(time.RFC3339), "payload": map[string]any{"amount": 50.0}})
	return s.produceRecords(ctx, []*kgo.Record{
		{Topic: s.prefix + "orders", Key: []byte("drift-v2"), Value: driftV2, Headers: []kgo.RecordHeader{{Key: "x-schema-version", Value: []byte("2")}, {Key: "x-content-type", Value: []byte("application/json")}}},
		{Topic: s.prefix + "orders", Key: []byte("drift-missing-id"), Value: driftMissing, Headers: []kgo.RecordHeader{{Key: "x-schema-version", Value: []byte("1")}, {Key: "x-content-type", Value: []byte("application/json")}}},
	}, "schema-drift messages")
}

func (s *Scenario) produceRecords(ctx context.Context, records []*kgo.Record, description string) error {
	tctx, cancel := context.WithTimeout(ctx, OperationTimeout)
	defer cancel()
	if err := s.rawClient.ProduceSync(tctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("producing %s: %w", description, err)
	}
	s.logger.Info("produced records", "description", description, "count", len(records))
	return nil
}

type VerifyResult struct {
	Prefix        string              `json:"prefix"`
	OK            bool                `json:"ok"`
	Present       []TopicStatus       `json:"present"`
	Missing       []string            `json:"missing,omitempty"`
	MessageCounts []TopicMessageCount `json:"message_counts"`
	LagGroup      string              `json:"lag_group"`
	TotalLag      int64               `json:"total_lag"`
}

type TopicStatus struct {
	Name         string `json:"name"`
	Partitions   int32  `json:"partitions"`
	MessageCount int64  `json:"message_count"`
}

type TopicMessageCount struct {
	Topic string `json:"topic"`
	Count int64  `json:"count"`
}

func (s *Scenario) Verify(ctx context.Context) (*VerifyResult, error) {
	result := &VerifyResult{Prefix: s.prefix}
	tctx, cancel := context.WithTimeout(ctx, OperationTimeout)
	defer cancel()

	allTopics, err := s.manager.ListTopics(tctx, "demo", false)
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}
	topicMap := make(map[string]kafka.TopicSummary, len(allTopics))
	for _, t := range allTopics {
		topicMap[t.Name] = t
	}
	for _, spec := range demoTopics(s.prefix) {
		if t, ok := topicMap[spec.Name]; ok {
			result.Present = append(result.Present, TopicStatus{Name: spec.Name, Partitions: int32(t.Partitions), MessageCount: t.MessageCount})
			result.MessageCounts = append(result.MessageCounts, TopicMessageCount{Topic: spec.Name, Count: t.MessageCount})
		} else {
			result.Missing = append(result.Missing, spec.Name)
		}
	}
	result.LagGroup = s.lagGroupID()
	if detail, err := s.manager.DescribeConsumerGroup(ctx, "demo", result.LagGroup); err == nil {
		result.TotalLag = detail.TotalLag
	}
	result.OK = len(result.Missing) == 0 && result.TotalLag > 0
	return result, nil
}

func (s *Scenario) Cleanup(ctx context.Context) ([]string, error) {
	if err := ValidatePrefix(s.prefix); err != nil {
		return nil, fmt.Errorf("refusing to cleanup with unsafe prefix: %w", err)
	}
	admin := kadm.NewClient(s.rawClient)
	if responses, err := admin.DeleteGroups(ctx, s.lagGroupID()); err != nil {
		return nil, fmt.Errorf("deleting demo consumer group: %w", err)
	} else if response, ok := responses[s.lagGroupID()]; ok && response.Err != nil &&
		!strings.Contains(response.Err.Error(), "GROUP_ID_NOT_FOUND") {
		return nil, fmt.Errorf("deleting demo consumer group: %w", response.Err)
	}
	var deleted []string
	for _, spec := range demoTopics(s.prefix) {
		if !strings.HasPrefix(spec.Name, s.prefix) {
			return nil, fmt.Errorf("safety check failed: topic %q does not start with prefix %q", spec.Name, s.prefix)
		}
		tctx, cancel := context.WithTimeout(ctx, OperationTimeout)
		err := s.manager.DeleteTopic(tctx, "demo", spec.Name)
		cancel()
		if err != nil {
			if strings.Contains(err.Error(), "UNKNOWN_TOPIC") || strings.Contains(err.Error(), "not found") {
				s.logger.Info("topic not found (already deleted)", "topic", spec.Name)
				continue
			}
			return deleted, fmt.Errorf("deleting topic %s: %w", spec.Name, err)
		}
		deleted = append(deleted, spec.Name)
		s.logger.Info("deleted topic", "topic", spec.Name)
	}
	return deleted, nil
}

func demoTopicNames(prefix string) []string {
	specs := demoTopics(prefix)
	names := make([]string, 0, len(specs))
	for _, t := range specs {
		names = append(names, t.Name)
	}
	return names
}

func makeRecord(topic, key string, payload any) *kgo.Record {
	data, _ := json.Marshal(payload)
	return &kgo.Record{
		Topic: topic, Key: []byte(key), Value: data,
		Headers: []kgo.RecordHeader{{Key: "x-content-type", Value: []byte("application/json")}, {Key: "x-producer", Value: []byte("incident-demo")}},
	}
}
