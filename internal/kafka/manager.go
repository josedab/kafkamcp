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


