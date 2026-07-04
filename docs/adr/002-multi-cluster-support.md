# ADR-002: Multi-Cluster Architecture

## Status

Accepted

## Context

Production Kafka environments commonly involve multiple clusters — separate clusters for dev/staging/prod, regional clusters for data locality, or dedicated clusters for different teams. KafkaMCP must support agents that need to interact with more than one cluster.

Design options considered:

1. **Single-cluster only** — One KafkaMCP instance per cluster. Agents connect to multiple instances.
2. **Multi-cluster with named routing** — One KafkaMCP instance connects to multiple clusters. Tools accept an optional `cluster` parameter.
3. **Proxy/federation model** — A central KafkaMCP federates requests to per-cluster KafkaMCP instances.

## Decision

We adopt **multi-cluster with named routing** — a single KafkaMCP server connects to multiple Kafka clusters, configured in `kafkamcp.yaml` under the `clusters[]` array. Each cluster has a unique `name`, and one cluster is marked as `default: true`.

Tool and resource requests accept an optional `cluster` parameter. When omitted, the default cluster is used.

```yaml
clusters:
  - name: production
    bootstrap_servers: "prod-kafka:9092"
    default: true
  - name: staging
    bootstrap_servers: "staging-kafka:9092"
```

Each cluster has independent configuration for:
- Bootstrap servers and security protocol
- SASL credentials
- Schema Registry URL and auth
- Kafka Connect URL

## Consequences

### Positive

- Agents can query across clusters in a single conversation ("compare topic configs between prod and staging").
- Single deployment simplifies operations — one instance, one config file.
- Per-cluster Schema Registry and Connect clients are isolated.
- Config drift detection and cross-cluster inspection work across clusters natively.

### Negative

- A single KafkaMCP instance must maintain connections to all clusters, increasing resource usage.
- Credential management is more complex — each cluster may have different auth mechanisms.
- Network partitions affecting one cluster can impact the instance's overall health (mitigated by per-cluster health checks).

### Neutral

- The `resolveClusterName()` helper validates cluster names and falls back to the default, preventing silent misrouting.
- Access control policies apply uniformly across clusters — per-cluster policies may be needed in the future.
