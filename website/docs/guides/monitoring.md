---
sidebar_position: 5
title: Monitoring
---

# Monitoring

KafkaMCP exposes Prometheus metrics for tool activity, throughput, denials, and connection health.

## Metrics endpoint

When KafkaMCP runs with `sse` or `streamable-http`, it starts a separate
metrics server on `server.metrics_host:server.metrics_port` (defaults:
`127.0.0.1` and `server.port + 1`).

Example:

- KafkaMCP API: `http://localhost:3100`
- Prometheus metrics: `http://localhost:3101/metrics`
- Process liveness: `http://localhost:3101/livez`
- Kafka health: `http://localhost:3101/healthz`
- Full readiness: `http://localhost:3101/ready`

```bash
curl -s http://localhost:3101/metrics
curl -s http://localhost:3101/livez
curl -s http://localhost:3101/ready
```

:::info
The metrics server is not started for `stdio` transport.
:::

## Metric reference

KafkaMCP defines these 10 metric families.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `kafkamcp_tool_calls_total` | Counter | `tool`, `agent_id`, `cluster`, `status` | Total MCP tool invocations |
| `kafkamcp_tool_latency_seconds` | Histogram | `tool`, `agent_id`, `cluster` | Tool call latency distribution |
| `kafkamcp_messages_consumed_total` | Counter | `topic`, `agent_id`, `cluster` | Messages consumed via `kafka_consume` |
| `kafkamcp_messages_produced_total` | Counter | `topic`, `agent_id`, `cluster` | Messages produced via `kafka_produce` |
| `kafkamcp_schema_cache_hits_total` | Counter | `cluster` | Schema cache hit count |
| `kafkamcp_schema_cache_misses_total` | Counter | `cluster` | Schema cache miss count |
| `kafkamcp_active_connections` | Gauge | `transport` | Currently connected agents |
| `kafkamcp_kafka_connection_errors_total` | Counter | `cluster`, `error_type` | Kafka connection failures |
| `kafkamcp_authz_denials_total` | Counter | `agent_id`, `tool`, `reason` | Authorization denials |
| `kafkamcp_rate_limit_denials_total` | Counter | `agent_id`, `tool` | Rate limit denials |

## What to watch first

### Tool traffic

```promql
sum by (tool, status) (rate(kafkamcp_tool_calls_total[5m]))
```

### P95 tool latency

```promql
histogram_quantile(
  0.95,
  sum by (le, tool) (rate(kafkamcp_tool_latency_seconds_bucket[5m]))
)
```

### Message throughput by topic

```promql
sum by (topic) (rate(kafkamcp_messages_consumed_total[5m]))
```

```promql
sum by (topic) (rate(kafkamcp_messages_produced_total[5m]))
```

### Authorization denials

```promql
sum by (agent_id, tool, reason) (increase(kafkamcp_authz_denials_total[15m]))
```

### Rate-limit pressure

```promql
sum by (agent_id, tool) (increase(kafkamcp_rate_limit_denials_total[15m]))
```

### Kafka connectivity errors

```promql
sum by (cluster, error_type) (increase(kafkamcp_kafka_connection_errors_total[15m]))
```

## Sample Grafana panel snippet

Use this in a Grafana dashboard JSON model to visualize per-tool request rate:

```json
{
  "title": "KafkaMCP tool calls by tool",
  "type": "timeseries",
  "targets": [
    {
      "expr": "sum by (tool) (rate(kafkamcp_tool_calls_total[5m]))",
      "legendFormat": "{{tool}}",
      "refId": "A"
    }
  ],
  "fieldConfig": {
    "defaults": {
      "unit": "reqps"
    },
    "overrides": []
  }
}
```

## Suggested dashboard layout

Build a first-pass dashboard with these panels:

1. request rate by tool
2. error rate by tool and status
3. p95 latency by tool
4. messages consumed by topic
5. messages produced by topic
6. auth denials by agent
7. rate-limit denials by agent
8. Kafka connection errors by cluster

## Alert ideas

### Sudden authorization spikes

Trigger when denied requests jump unexpectedly:

```promql
sum(increase(kafkamcp_authz_denials_total[5m])) > 20
```

### Sustained rate limiting

```promql
sum(increase(kafkamcp_rate_limit_denials_total[10m])) > 50
```

### Tool latency regression

```promql
histogram_quantile(
  0.95,
  sum by (le) (rate(kafkamcp_tool_latency_seconds_bucket[10m]))
) > 2
```

## Operational guidance

- scrape `/metrics` from the metrics port, not the main MCP port
- use `/livez` for liveness and `/ready` for readiness; Kafka outages must not
  cause process restarts
- set `metrics_host: 0.0.0.0` only inside a protected container/pod network
- label clusters clearly so dashboards stay readable in multi-cluster deployments
- alert on denials separately from Kafka errors; they mean different problems
- track `status="error"` on `kafkamcp_tool_calls_total` to spot failing tools early

## Concurrency limits

KafkaMCP limits concurrent execution of expensive tools (e.g. `kafka_search`) to **5 simultaneous operations**. Additional requests are queued until a slot becomes available. Factor this into capacity planning when multiple agents run heavy analytics concurrently.

:::tip
Start with latency, denials, and throughput. Those three views answer most production questions quickly.
:::

## Building a Grafana dashboard

KafkaMCP exposes standard Prometheus metrics but does not bundle a versioned
Grafana dashboard. Build a dashboard from the queries in this guide so it
matches your labels, retention, and alerting conventions.

Useful panels include:

| Section | Panels |
|---|---|
| **Overview** | Tool Calls (rate/min), Active Connections, Errors (rate/min), Avg Tool Latency (p50) |
| **Tool Performance** | Call Rate by Tool, Latency (p95) by Tool, Errors by Tool, Error Rate by Tool |
| **Kafka Messages** | Messages Consumed (rate) by Topic, Messages Produced (rate) by Topic |
| **Schema Registry** | Cache Hit/Miss Rate, Cache Hit Rate |
| **Security & Rate Limiting** | Kafka Connection Errors, Authorization Denials, Rate Limit Denials |
| **Connections** | Active Connections by Transport, Tool Call Rate by Agent |

Use the default `kafkamcp_*` metric names shown above and select the Prometheus
data source that scrapes KafkaMCP's metrics endpoint.
