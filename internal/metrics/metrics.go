package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

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
			Buckets: prometheus.DefBuckets,
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
