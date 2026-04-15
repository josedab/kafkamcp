package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Manager manages Kafka client connections for multiple clusters.
type Manager struct {
	mu       sync.RWMutex
	clients  map[string]*kgo.Client
	admins   map[string]*kadm.Client
	configs  map[string]*config.ClusterConfig
	logger   *slog.Logger
	defClust string
}

// NewManager creates a new Kafka Manager from the provided config.
func NewManager(cfg *config.Config, logger *slog.Logger) (*Manager, error) {
	m := &Manager{
		clients: make(map[string]*kgo.Client),
		admins:  make(map[string]*kadm.Client),
		configs: make(map[string]*config.ClusterConfig),
		logger:  logger,
	}

	for i := range cfg.Clusters {
		cl := &cfg.Clusters[i]
		m.configs[cl.Name] = cl
		if cl.Default {
			m.defClust = cl.Name
		}

		client, err := m.createClient(cl)
		if err != nil {
			m.Close()
			return nil, fmt.Errorf("creating client for cluster %q: %w", cl.Name, err)
		}
		m.clients[cl.Name] = client
		m.admins[cl.Name] = kadm.NewClient(client)
		logger.Info("connected to kafka cluster", "cluster", cl.Name, "brokers", cl.BootstrapServers)
	}

	return m, nil
}

// clientOpts returns common kgo options (SASL, TLS) for a cluster config.
func (m *Manager) clientOpts(cl *config.ClusterConfig) ([]kgo.Opt, error) {
	brokers := strings.Split(cl.BootstrapServers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.WithLogger(kgo.BasicLogger(nil, kgo.LogLevelError, nil)),
	}

	// TLS
	if strings.Contains(strings.ToUpper(cl.SecurityProtocol), "SSL") {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if cl.SSLCALocation != "" {
			caCert, err := os.ReadFile(cl.SSLCALocation)
			if err != nil {
				return nil, fmt.Errorf("reading CA certificate failed")
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA certificate")
			}
			tlsCfg.RootCAs = pool
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	// SASL
	switch strings.ToUpper(cl.SASLMechanism) {
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: cl.SASLUsername, Pass: cl.SASLPassword}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: cl.SASLUsername, Pass: cl.SASLPassword}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: cl.SASLUsername, Pass: cl.SASLPassword}.AsSha512Mechanism()))
	case "":
		// No SASL
	default:
		return nil, fmt.Errorf("unsupported SASL mechanism: %s", cl.SASLMechanism)
	}

	return opts, nil
}

func (m *Manager) createClient(cl *config.ClusterConfig) (*kgo.Client, error) {
	opts, err := m.clientOpts(cl)
	if err != nil {
		return nil, err
	}

	// Producer-specific options
	opts = append(opts,
		kgo.ProducerBatchMaxBytes(1048576),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)

	return kgo.NewClient(opts...)
}

// GetClient returns the Kafka client for the named cluster, or the default.
func (m *Manager) GetClient(clusterName string) (*kgo.Client, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name := clusterName
	if name == "" {
		name = m.defClust
	}
	client, ok := m.clients[name]
	if !ok {
		return nil, fmt.Errorf("cluster %q not found", name)
	}
	return client, nil
}

// GetAdmin returns the admin client for the named cluster, or the default.
func (m *Manager) GetAdmin(clusterName string) (*kadm.Client, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name := clusterName
	if name == "" {
		name = m.defClust
	}
	admin, ok := m.admins[name]
	if !ok {
		return nil, fmt.Errorf("cluster %q not found", name)
	}
	return admin, nil
}

// GetClusterConfig returns the config for the named cluster, or the default.
func (m *Manager) GetClusterConfig(clusterName string) (*config.ClusterConfig, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	name := clusterName
	if name == "" {
		name = m.defClust
	}
	cfg, ok := m.configs[name]
	if !ok {
		return nil, fmt.Errorf("cluster %q not found", name)
	}
	return cfg, nil
}

// DefaultClusterName returns the name of the default cluster.
func (m *Manager) DefaultClusterName() string {
	return m.defClust
}

// ClusterNames returns all configured cluster names.
func (m *Manager) ClusterNames() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.configs))
	for name := range m.configs {
		names = append(names, name)
	}
	return names
}

// Close shuts down all Kafka clients.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for name, client := range m.clients {
		client.Close()
		m.logger.Info("closed kafka client", "cluster", name)
	}
}

// TopicSummary contains summarized information about a Kafka topic.
type TopicSummary struct {
	Name             string `json:"name"`
	Partitions       int    `json:"partitions"`
	ReplicationFactor int   `json:"replication_factor"`
	RetentionMs      int64  `json:"retention_ms,omitempty"`
	IsInternal       bool   `json:"is_internal"`
	MessageCount     int64  `json:"message_count_estimate"`
	SchemaSubject    string `json:"schema_subject,omitempty"`
}

// TopicDetail contains full topic metadata.
type TopicDetail struct {
	Name             string            `json:"name"`
	Partitions       []PartitionDetail `json:"partitions"`
	ReplicationFactor int              `json:"replication_factor"`
	Configs          map[string]string `json:"configs"`
	IsInternal       bool              `json:"is_internal"`
	MessageCount     int64             `json:"message_count_estimate"`
}

// PartitionDetail describes a single topic partition.
type PartitionDetail struct {
	ID             int32   `json:"id"`
	Leader         int32   `json:"leader"`
	Replicas       []int32 `json:"replicas"`
	ISR            []int32 `json:"isr"`
	EarliestOffset int64   `json:"earliest_offset"`
	LatestOffset   int64   `json:"latest_offset"`
}

// ConsumerGroupSummary is a summary of a consumer group.
type ConsumerGroupSummary struct {
	GroupID       string `json:"group_id"`
	State         string `json:"state"`
	MemberCount   int    `json:"member_count"`
	Protocol      string `json:"protocol"`
	TotalLag      int64  `json:"total_lag"`
}

// ConsumerGroupDetail is full consumer group metadata.
type ConsumerGroupDetail struct {
	GroupID      string              `json:"group_id"`
	State        string              `json:"state"`
	ProtocolType string              `json:"protocol_type"`
	Protocol     string              `json:"protocol"`
	Coordinator  BrokerInfo          `json:"coordinator"`
	Members      []GroupMemberDetail `json:"members"`
	Offsets      []GroupOffsetDetail `json:"offsets"`
	TotalLag     int64               `json:"total_lag"`
}

// BrokerInfo describes a Kafka broker.
type BrokerInfo struct {
	ID   int32  `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
	Rack string `json:"rack,omitempty"`
}

// GroupMemberDetail describes a member of a consumer group.
type GroupMemberDetail struct {
	MemberID   string            `json:"member_id"`
	ClientID   string            `json:"client_id"`
	Host       string            `json:"host"`
	Assignment []TopicPartitions `json:"assignment"`
}

// TopicPartitions maps a topic to its assigned partitions.
type TopicPartitions struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

// GroupOffsetDetail describes offsets for a partition within a consumer group.
type GroupOffsetDetail struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	CurrentOffset int64  `json:"current_offset"`
	LogEndOffset  int64  `json:"log_end_offset"`
	Lag           int64  `json:"lag"`
}

// ClusterMetadata contains cluster-level information.
type ClusterMetadata struct {
	ClusterID      string       `json:"cluster_id"`
	Controller     BrokerInfo   `json:"controller"`
	Brokers        []BrokerInfo `json:"brokers"`
	BrokerCount    int          `json:"broker_count"`
	TopicCount     int          `json:"topic_count"`
	PartitionCount int          `json:"partition_count"`
	KraftMode      bool         `json:"kraft_mode"`
}

// Message represents a consumed Kafka message.
type Message struct {
	Partition    int32             `json:"partition"`
	Offset       int64             `json:"offset"`
	Timestamp    string            `json:"timestamp"`
	Key          any               `json:"key"`
	Value        any               `json:"value"`
	Headers      map[string]string `json:"headers,omitempty"`
}

// DeliveryResult is the result of producing a message.
type DeliveryResult struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Status    string `json:"status"`
}

// ListTopics lists topics in the cluster, optionally filtering by pattern.
func (m *Manager) ListTopics(ctx context.Context, cluster string, includeInternal bool) ([]TopicSummary, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	topics, err := admin.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}

	startOffsets, err := admin.ListStartOffsets(ctx)
	if err != nil {
		m.logger.Warn("failed to list start offsets", "error", err)
	}
	endOffsets, err := admin.ListEndOffsets(ctx)
	if err != nil {
		m.logger.Warn("failed to list end offsets", "error", err)
	}

	var result []TopicSummary
	for _, t := range topics.Sorted() {
		if !includeInternal && t.IsInternal {
			continue
		}

		var msgCount int64
		if startOffsets != nil && endOffsets != nil {
			for _, p := range t.Partitions.Sorted() {
				so, ok1 := startOffsets.Lookup(t.Topic, p.Partition)
				eo, ok2 := endOffsets.Lookup(t.Topic, p.Partition)
				if ok1 && ok2 {
					msgCount += eo.Offset - so.Offset
				}
			}
		}

		replFactor := 0
		if len(t.Partitions) > 0 {
			replFactor = len(t.Partitions.Sorted()[0].Replicas)
		}

		result = append(result, TopicSummary{
			Name:              t.Topic,
			Partitions:        len(t.Partitions),
			ReplicationFactor: replFactor,
			IsInternal:        t.IsInternal,
			MessageCount:      msgCount,
		})
	}

	return result, nil
}

// DescribeTopic returns detailed topic metadata.
func (m *Manager) DescribeTopic(ctx context.Context, cluster, topicName string) (*TopicDetail, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	topics, err := admin.ListTopics(ctx, topicName)
	if err != nil {
		return nil, fmt.Errorf("describing topic: %w", err)
	}

	t, ok := topics[topicName]
	if !ok {
		return nil, fmt.Errorf("topic %q not found", topicName)
	}

	startOffsets, err := admin.ListStartOffsets(ctx, topicName)
	if err != nil {
		m.logger.Warn("failed to list start offsets", "topic", topicName, "error", err)
	}
	endOffsets, err := admin.ListEndOffsets(ctx, topicName)
	if err != nil {
		m.logger.Warn("failed to list end offsets", "topic", topicName, "error", err)
	}

	// Fetch topic configs via admin API
	configs := make(map[string]string)
	topicConfigs, err := admin.DescribeTopicConfigs(ctx, topicName)
	if err == nil {
		for _, rc := range topicConfigs {
			if rc.Err == nil {
				for _, entry := range rc.Configs {
					if entry.Value != nil {
						configs[entry.Key] = *entry.Value
					}
				}
			}
		}
	}

	replFactor := 0
	var partitions []PartitionDetail
	var totalMessages int64

	for _, p := range t.Partitions.Sorted() {
		if replFactor == 0 {
			replFactor = len(p.Replicas)
		}
		earliest := int64(0)
		latest := int64(0)
		if startOffsets != nil {
			if so, ok := startOffsets.Lookup(topicName, p.Partition); ok {
				earliest = so.Offset
			}
		}
		if endOffsets != nil {
			if eo, ok := endOffsets.Lookup(topicName, p.Partition); ok {
				latest = eo.Offset
			}
		}
		totalMessages += latest - earliest

		partitions = append(partitions, PartitionDetail{
			ID:             p.Partition,
			Leader:         p.Leader,
			Replicas:       p.Replicas,
			ISR:            p.ISR,
			EarliestOffset: earliest,
			LatestOffset:   latest,
		})
	}

	return &TopicDetail{
		Name:              topicName,
		Partitions:        partitions,
		ReplicationFactor: replFactor,
		Configs:           configs,
		IsInternal:        t.IsInternal,
		MessageCount:      totalMessages,
	}, nil
}

// Consume reads messages from a topic using direct partition fetch.
func (m *Manager) Consume(ctx context.Context, cluster, topic string, partition int32, offset string, limit int, timeoutMs int, deserializer string) ([]Message, error) {
	clCfg, err := m.GetClusterConfig(cluster)
	if err != nil {
		return nil, err
	}

	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	// Determine partitions to read from
	topics, err := admin.ListTopics(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("listing topic for consume: %w", err)
	}
	td, ok := topics[topic]
	if !ok {
		return nil, fmt.Errorf("topic %q not found", topic)
	}

	var targetPartitions []int32
	if partition >= 0 {
		targetPartitions = []int32{partition}
	} else {
		for _, p := range td.Partitions.Sorted() {
			targetPartitions = append(targetPartitions, p.Partition)
		}
	}

	perPartitionLimit := limit
	if partition < 0 && len(targetPartitions) > 0 {
		perPartitionLimit = (limit / len(targetPartitions)) + 1
	}

	// Build consumer offsets
	consumeOffsets := make(map[string]map[int32]kgo.Offset)
	consumeOffsets[topic] = make(map[int32]kgo.Offset)

	for _, p := range targetPartitions {
		var kgoOffset kgo.Offset
		switch offset {
		case "earliest":
			kgoOffset = kgo.NewOffset().AtStart()
		case "latest":
			endOffsets, err := admin.ListEndOffsets(ctx, topic)
			if err != nil {
				return nil, fmt.Errorf("listing end offsets: %w", err)
			}
			eo, ok := endOffsets.Lookup(topic, p)
			if !ok {
				continue
			}
			startAt := eo.Offset - int64(perPartitionLimit)
			if startAt < 0 {
				startAt = 0
			}
			kgoOffset = kgo.NewOffset().At(startAt)
		default:
			if off, err := strconv.ParseInt(offset, 10, 64); err == nil {
				kgoOffset = kgo.NewOffset().At(off)
			} else if ts, tsErr := parseTimestamp(offset); tsErr == nil {
				listed, lErr := admin.ListOffsetsAfterMilli(ctx, ts.UnixMilli(), topic)
				if lErr == nil {
					if lo, ok := listed.Lookup(topic, p); ok && lo.Offset >= 0 {
						kgoOffset = kgo.NewOffset().At(lo.Offset)
					} else {
						kgoOffset = kgo.NewOffset().AtEnd()
					}
				} else {
					kgoOffset = kgo.NewOffset().AtEnd()
				}
			} else {
				kgoOffset = kgo.NewOffset().AtEnd()
			}
		}
		consumeOffsets[topic][p] = kgoOffset
	}

	// Reuse shared client options (SASL, TLS) then add consumer-specific opts
	opts, err := m.clientOpts(clCfg)
	if err != nil {
		return nil, fmt.Errorf("building client options: %w", err)
	}
	opts = append(opts, kgo.ConsumePartitions(consumeOffsets))

	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating consumer: %w", err)
	}
	defer consumer.Close()

	var messages []Message
	ctx, cancel := context.WithTimeout(ctx, parseDurationMs(timeoutMs))
	defer cancel()

	for len(messages) < limit {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != context.DeadlineExceeded {
					m.logger.Warn("fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
				}
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if len(messages) >= limit {
				return
			}
			headers := make(map[string]string)
			for _, h := range r.Headers {
				headers[h.Key] = string(h.Value)
			}

			msg := Message{
				Partition: r.Partition,
				Offset:    r.Offset,
				Timestamp: r.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
				Key:       decodeValue(r.Key, deserializer),
				Value:     decodeValue(r.Value, deserializer),
				Headers:   headers,
			}
			messages = append(messages, msg)
		})

		if fetches.NumRecords() == 0 {
			break
		}
	}

	if len(messages) > limit {
		messages = messages[:limit]
	}

	return messages, nil
}

// ProduceOptions are options for the Produce call.
type ProduceOptions struct {
	Acks       string `json:"acks"`       // "0", "1", "all"
	Serializer string `json:"serializer"` // "auto", "json", "string"
	SchemaID   int    `json:"schema_id"`  // -1 for none
}

// Produce writes messages to a topic.
func (m *Manager) Produce(ctx context.Context, cluster, topic string, messages []ProduceMessage, opts ProduceOptions) ([]DeliveryResult, error) {
	// For non-default acks, create a temporary client
	var client *kgo.Client
	var needsClose bool

	if opts.Acks == "0" || opts.Acks == "1" {
		clCfg, err := m.GetClusterConfig(cluster)
		if err != nil {
			return nil, err
		}
		clientOpts, err := m.clientOpts(clCfg)
		if err != nil {
			return nil, err
		}
		switch opts.Acks {
		case "0":
			clientOpts = append(clientOpts, kgo.RequiredAcks(kgo.NoAck()))
		case "1":
			clientOpts = append(clientOpts, kgo.RequiredAcks(kgo.LeaderAck()))
		}
		client, err = kgo.NewClient(clientOpts...)
		if err != nil {
			return nil, fmt.Errorf("creating producer client: %w", err)
		}
		needsClose = true
	} else {
		var err error
		client, err = m.GetClient(cluster)
		if err != nil {
			return nil, err
		}
	}
	if needsClose {
		defer client.Close()
	}

	// Pre-allocate results to preserve input order
	results := make([]DeliveryResult, len(messages))
	var wg sync.WaitGroup

	for i, msg := range messages {
		value := serializeValue(msg.Value)
		record := &kgo.Record{
			Topic: topic,
			Value: []byte(value),
		}
		if msg.Key != "" {
			record.Key = []byte(msg.Key)
		}
		if msg.Partition != nil {
			record.Partition = *msg.Partition
		}
		for k, v := range msg.Headers {
			record.Headers = append(record.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}

		wg.Add(1)
		idx := i
		client.Produce(ctx, record, func(r *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				results[idx] = DeliveryResult{Status: fmt.Sprintf("error: %v", err)}
				return
			}
			results[idx] = DeliveryResult{
				Partition: r.Partition,
				Offset:    r.Offset,
				Timestamp: r.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
				Status:    "delivered",
			}
		})
	}

	wg.Wait()
	return results, nil
}

// ProduceMessage is an input message for producing.
type ProduceMessage struct {
	Key       string            `json:"key"`
	Value     any               `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Partition *int32            `json:"partition,omitempty"`
}

// SubscribeRequest defines parameters for a streaming subscription.
type SubscribeRequest struct {
	Topic          string `json:"topic"`
	GroupID        string `json:"group_id"`
	AutoCommit     bool   `json:"auto_commit"`
	MaxMessages    int    `json:"max_messages"`
	TimeoutMs      int    `json:"timeout_ms"`
	Deserializer   string `json:"deserializer"`
}

// SubscribeResult is the result of a subscription poll.
type SubscribeResult struct {
	Messages    []Message `json:"messages"`
	Count       int       `json:"count"`
	Topic       string    `json:"topic"`
	GroupID     string    `json:"group_id"`
	AutoCommit  bool      `json:"auto_committed"`
}

// Subscribe consumes from a topic using consumer group semantics.
// It polls for up to timeoutMs and returns up to maxMessages.
func (m *Manager) Subscribe(ctx context.Context, cluster string, req SubscribeRequest) (*SubscribeResult, error) {
	clCfg, err := m.GetClusterConfig(cluster)
	if err != nil {
		return nil, err
	}

	opts, err := m.clientOpts(clCfg)
	if err != nil {
		return nil, fmt.Errorf("building client options: %w", err)
	}

	groupID := req.GroupID
	if groupID == "" {
		groupID = fmt.Sprintf("kafkamcp-subscribe-%d", time.Now().UnixNano())
	}

	maxMessages := req.MaxMessages
	if maxMessages <= 0 {
		maxMessages = 100
	}
	if maxMessages > 1000 {
		maxMessages = 1000
	}

	timeoutMs := req.TimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 10000
	}
	if timeoutMs > 30000 {
		timeoutMs = 30000
	}

	deserializer := req.Deserializer
	if deserializer == "" {
		deserializer = "auto"
	}

	opts = append(opts,
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(req.Topic),
		kgo.DisableAutoCommit(),
	)

	consumer, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating group consumer: %w", err)
	}
	defer consumer.Close()

	var messages []Message
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	for len(messages) < maxMessages {
		fetches := consumer.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err != context.DeadlineExceeded {
					m.logger.Warn("subscribe fetch error", "topic", e.Topic, "partition", e.Partition, "error", e.Err)
				}
			}
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if len(messages) >= maxMessages {
				return
			}
			headers := make(map[string]string)
			for _, h := range r.Headers {
				headers[h.Key] = string(h.Value)
			}
			messages = append(messages, Message{
				Partition: r.Partition,
				Offset:    r.Offset,
				Timestamp: r.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
				Key:       decodeValue(r.Key, deserializer),
				Value:     decodeValue(r.Value, deserializer),
				Headers:   headers,
			})
		})

		if fetches.NumRecords() == 0 {
			break
		}
	}

	if len(messages) > maxMessages {
		messages = messages[:maxMessages]
	}

	// Auto-commit offsets if requested
	if req.AutoCommit && len(messages) > 0 {
		if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
			m.logger.Warn("failed to commit offsets", "group", groupID, "error", err)
		}
	}

	return &SubscribeResult{
		Messages:   messages,
		Count:      len(messages),
		Topic:      req.Topic,
		GroupID:    groupID,
		AutoCommit: req.AutoCommit,
	}, nil
}

// ListConsumerGroups lists all consumer groups in the cluster.
func (m *Manager) ListConsumerGroups(ctx context.Context, cluster string) ([]ConsumerGroupSummary, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	groups, err := admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing consumer groups: %w", err)
	}

	var result []ConsumerGroupSummary
	for _, g := range groups.Sorted() {
		result = append(result, ConsumerGroupSummary{
			GroupID:     g.Group,
			State:       g.State,
			Protocol:    g.ProtocolType,
		})
	}

	// Try to enrich with lag info and member count
	if len(result) > 0 {
		groupIDs := make([]string, len(result))
		for i, g := range result {
			groupIDs[i] = g.GroupID
		}
		described, err := admin.DescribeGroups(ctx, groupIDs...)
		if err == nil {
			for i := range result {
				if dg, ok := described[result[i].GroupID]; ok {
					result[i].State = dg.State
					result[i].MemberCount = len(dg.Members)
				}
			}
		}
		lags, err := admin.Lag(ctx, groupIDs...)
		if err == nil {
			lags.Each(func(l kadm.DescribedGroupLag) {
				for i := range result {
					if result[i].GroupID == l.Group {
						result[i].TotalLag = l.Lag.Total()
						break
					}
				}
			})
		}
	}

	return result, nil
}

// DescribeConsumerGroup returns detailed consumer group info.
func (m *Manager) DescribeConsumerGroup(ctx context.Context, cluster, groupID string) (*ConsumerGroupDetail, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	described, err := admin.DescribeGroups(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("describing consumer group: %w", err)
	}

	dg, ok := described[groupID]
	if !ok {
		return nil, fmt.Errorf("consumer group %q not found", groupID)
	}
	if dg.Err != nil {
		return nil, fmt.Errorf("describing consumer group %q: %w", groupID, dg.Err)
	}

	detail := &ConsumerGroupDetail{
		GroupID:      groupID,
		State:        dg.State,
		ProtocolType: dg.ProtocolType,
		Protocol:     dg.Protocol,
		Coordinator: BrokerInfo{
			ID:   int32(dg.Coordinator.NodeID),
			Host: dg.Coordinator.Host,
			Port: dg.Coordinator.Port,
		},
	}

	// Members
	for _, member := range dg.Members {
		md := GroupMemberDetail{
			MemberID: member.MemberID,
			ClientID: member.ClientID,
			Host:     member.ClientHost,
		}
		if consumerAssign, ok := member.Assigned.AsConsumer(); ok {
			for _, ta := range consumerAssign.Topics {
				md.Assignment = append(md.Assignment, TopicPartitions{
					Topic:      ta.Topic,
					Partitions: ta.Partitions,
				})
			}
		}
		detail.Members = append(detail.Members, md)
	}

	// Offsets and lag
	lags, err := admin.Lag(ctx, groupID)
	if err == nil {
		if gl, ok := lags[groupID]; ok {
			for _, memberLag := range gl.Lag.Sorted() {
				detail.Offsets = append(detail.Offsets, GroupOffsetDetail{
					Topic:         memberLag.Topic,
					Partition:     memberLag.Partition,
					CurrentOffset: memberLag.Commit.At,
					LogEndOffset:  memberLag.End.Offset,
					Lag:           memberLag.Lag,
				})
			}
			detail.TotalLag = gl.Lag.Total()
		}
	}

	return detail, nil
}

// GetClusterMetadata returns cluster-level metadata.
func (m *Manager) GetClusterMetadata(ctx context.Context, cluster string) (*ClusterMetadata, error) {
	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	meta, err := admin.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting metadata: %w", err)
	}

	var brokers []BrokerInfo
	for _, b := range meta.Brokers {
		bi := BrokerInfo{
			ID:   b.NodeID,
			Host: b.Host,
			Port: b.Port,
		}
		if b.Rack != nil {
			bi.Rack = *b.Rack
		}
		brokers = append(brokers, bi)
	}

	var controller BrokerInfo
	for _, b := range meta.Brokers {
		if b.NodeID == meta.Controller {
			controller = BrokerInfo{
				ID:   b.NodeID,
				Host: b.Host,
				Port: b.Port,
			}
			if b.Rack != nil {
				controller.Rack = *b.Rack
			}
			break
		}
	}

	totalPartitions := 0
	for _, t := range meta.Topics {
		totalPartitions += len(t.Partitions)
	}

	// KRaft mode: cluster ID is non-empty in KRaft clusters
	kraftMode := meta.Cluster != ""

	return &ClusterMetadata{
		ClusterID:      meta.Cluster,
		Controller:     controller,
		Brokers:        brokers,
		BrokerCount:    len(brokers),
		TopicCount:     len(meta.Topics),
		PartitionCount: totalPartitions,
		KraftMode:      kraftMode,
	}, nil
}
