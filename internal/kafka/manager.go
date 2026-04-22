package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Manager manages Kafka client connections for multiple clusters.
type Manager struct {
	mu        sync.RWMutex
	clients   map[string]*kgo.Client
	admins    map[string]*kadm.Client
	configs   map[string]*config.ClusterConfig
	logger    *slog.Logger
	defClust  string
	streamMgr *streamSessionManager
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
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13}
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
		if !strings.Contains(strings.ToUpper(cl.SecurityProtocol), "SSL") {
			return nil, fmt.Errorf("SASL PLAIN requires TLS (security_protocol must be SSL or SASL_SSL) for cluster %q — refusing to send credentials in plaintext", cl.Name)
		}
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

// Ping checks connectivity to the default Kafka cluster by requesting metadata.
func (m *Manager) Ping(ctx context.Context) error {
	admin, err := m.GetAdmin("")
	if err != nil {
		return fmt.Errorf("getting admin client: %w", err)
	}
	_, err = admin.Metadata(ctx)
	if err != nil {
		return fmt.Errorf("kafka metadata request failed: %w", err)
	}
	return nil
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
		} else {
			m.logger.Warn("failed to describe consumer groups for enrichment", "error", err)
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
		} else {
			m.logger.Warn("failed to fetch consumer group lag for enrichment", "error", err)
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
