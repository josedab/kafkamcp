package metrics

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const maxUniqueAgents = 1000
const maxUniqueTopics = 500

// SanitizeAgentLabel caps unique agent_id label values to prevent unbounded cardinality.
// If the agent is already known or the allowlist has capacity, the original ID is returned.
// Otherwise, the ID is hashed into a fixed number of overflow buckets.
func SanitizeAgentLabel(agentID string) string {
	return ensureDefaultMetrics().SanitizeAgentLabel(agentID)
}

// SanitizeTopicLabel caps unique topic label values to prevent leaking topic names
// through the unauthenticated metrics endpoint.
func SanitizeTopicLabel(topic string) string {
	return ensureDefaultMetrics().SanitizeTopicLabel(topic)
}

// Metrics holds all KafkaMCP Prometheus metrics.
type Metrics struct {
	ToolCallsTotal       *prometheus.CounterVec
	ToolLatencySeconds   *prometheus.HistogramVec
	MessagesConsumed     *prometheus.CounterVec
	MessagesProduced     *prometheus.CounterVec
	SchemaCacheHits      *prometheus.CounterVec
	SchemaCacheMisses    *prometheus.CounterVec
	ActiveConnections    *prometheus.GaugeVec
	KafkaConnErrors      *prometheus.CounterVec
	AuthzDenials         *prometheus.CounterVec
	RateLimitDenials     *prometheus.CounterVec
	Registry             *prometheus.Registry

	agentMu     sync.RWMutex
	knownAgents map[string]struct{}

	topicMu     sync.RWMutex
	knownTopics map[string]struct{}
}

// New creates and registers all KafkaMCP metrics on a dedicated registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)

	m := &Metrics{
		Registry:    reg,
		knownAgents: make(map[string]struct{}),
		knownTopics: make(map[string]struct{}),

		ToolCallsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafkamcp_tool_calls_total",
			Help: "Total MCP tool invocations",
		}, []string{"tool", "agent_id", "cluster", "status"}),

		ToolLatencySeconds: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kafkamcp_tool_latency_seconds",
			Help:    "Tool call latency distribution",
			Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 120},
		}, []string{"tool", "agent_id", "cluster"}),

		MessagesConsumed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafkamcp_messages_consumed_total",
			Help: "Messages consumed via kafka_consume",
		}, []string{"topic", "agent_id", "cluster"}),

		MessagesProduced: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafkamcp_messages_produced_total",
			Help: "Messages produced via kafka_produce",
		}, []string{"topic", "agent_id", "cluster"}),

		SchemaCacheHits: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafkamcp_schema_cache_hits_total",
			Help: "Schema cache hit count",
		}, []string{"cluster"}),

		SchemaCacheMisses: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafkamcp_schema_cache_misses_total",
			Help: "Schema cache miss count",
		}, []string{"cluster"}),

		ActiveConnections: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafkamcp_active_connections",
			Help: "Currently connected agents",
		}, []string{"transport"}),

		KafkaConnErrors: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafkamcp_kafka_connection_errors_total",
			Help: "Kafka connection failures",
		}, []string{"cluster", "error_type"}),

		AuthzDenials: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafkamcp_authz_denials_total",
			Help: "Authorization denials",
		}, []string{"agent_id", "tool", "reason"}),

		RateLimitDenials: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "kafkamcp_rate_limit_denials_total",
			Help: "Rate limit denials",
		}, []string{"agent_id", "tool"}),
	}

	defaultMetrics.Store(m)
	return m
}

// SanitizeTopicLabel on a Metrics instance caps unique topic label values.
// Topics beyond the limit are hashed into overflow buckets to prevent
// leaking topic names via the unauthenticated metrics endpoint.
func (m *Metrics) SanitizeTopicLabel(topic string) string {
	m.topicMu.Lock()
	defer m.topicMu.Unlock()

	if _, ok := m.knownTopics[topic]; ok {
		return topic
	}
	if len(m.knownTopics) < maxUniqueTopics {
		m.knownTopics[topic] = struct{}{}
		return topic
	}
	h := sha256.Sum256([]byte(topic))
	bucket := h[0] % 32
	return fmt.Sprintf("topic_overflow_%02d", bucket)
}

// SanitizeAgentLabel on a Metrics instance caps unique agent_id label values.
func (m *Metrics) SanitizeAgentLabel(agentID string) string {
	m.agentMu.Lock()
	defer m.agentMu.Unlock()

	if _, ok := m.knownAgents[agentID]; ok {
		return agentID
	}
	if len(m.knownAgents) < maxUniqueAgents {
		m.knownAgents[agentID] = struct{}{}
		return agentID
	}
	h := sha256.Sum256([]byte(agentID))
	bucket := h[0] % 16
	return fmt.Sprintf("overflow_%02d", bucket)
}

// ResetAgents clears the known agents set (for testing).
func (m *Metrics) ResetAgents() {
	m.agentMu.Lock()
	m.knownAgents = make(map[string]struct{})
	m.agentMu.Unlock()
}

// SetKnownAgents replaces the known agents set (for testing).
func (m *Metrics) SetKnownAgents(agents map[string]struct{}) {
	m.agentMu.Lock()
	m.knownAgents = agents
	m.agentMu.Unlock()
}

// defaultMetrics is a package-level Metrics instance used by the
// package-level SanitizeAgentLabel function for backward compatibility.
// Lazily initialized on first use via ensureDefaultMetrics.
var defaultMetrics atomic.Pointer[Metrics]

func ensureDefaultMetrics() *Metrics {
	if m := defaultMetrics.Load(); m != nil {
		return m
	}
	m := &Metrics{knownAgents: make(map[string]struct{}), knownTopics: make(map[string]struct{})}
	defaultMetrics.CompareAndSwap(nil, m)
	return defaultMetrics.Load()
}
