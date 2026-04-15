package metrics

import (
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const maxUniqueAgents = 1000

var (
	agentMu       sync.RWMutex
	knownAgents   = make(map[string]struct{})
)

// SanitizeAgentLabel caps unique agent_id label values to prevent unbounded cardinality.
// If the agent is already known or the allowlist has capacity, the original ID is returned.
// Otherwise, the ID is hashed into a fixed number of overflow buckets.
func SanitizeAgentLabel(agentID string) string {
	agentMu.RLock()
	_, ok := knownAgents[agentID]
	n := len(knownAgents)
	agentMu.RUnlock()

	if ok {
		return agentID
	}
	if n < maxUniqueAgents {
		agentMu.Lock()
		knownAgents[agentID] = struct{}{}
		agentMu.Unlock()
		return agentID
	}
	// Hash-bucket overflow agents into 16 buckets
	h := sha256.Sum256([]byte(agentID))
	bucket := h[0] % 16
	return fmt.Sprintf("overflow_%02d", bucket)
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
}

// New creates and registers all KafkaMCP metrics on a dedicated registry.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)

	return &Metrics{
		Registry: reg,

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
}
