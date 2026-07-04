# KafkaMCP Helm Chart

Production-oriented Kubernetes deployment for KafkaMCP.

## Safety model

- Metrics and probes bind to `0.0.0.0:3101` inside the pod and are restricted by
  the chart NetworkPolicy.
- `/livez` is process-local; `/ready` checks Kafka and persistence.
- `ops-write`, `governance`, and SQLite persistence require exactly one replica.
  The chart fails rendering if those modes are combined with `replicaCount > 1`
  or HPA.
- SQLite uses a `ReadWriteOnce` PVC and `Recreate` rollout strategy. Never share
  the database over NFS/EFS or run two active writers.

## Install

Create a secret containing the required static identity and Kafka credentials:

```bash
kubectl create secret generic kafkamcp-production \
  --from-literal=KAFKAMCP_AGENT_TOKEN='replace-with-32-plus-random-bytes' \
  --from-literal=KAFKA_SASL_USERNAME='kafkamcp' \
  --from-literal=KAFKA_SASL_PASSWORD='replace-me'
```

Install with an immutable release image tag and real broker address:

```bash
helm upgrade --install kafkamcp ./deploy/helm/kafkamcp \
  --set image.tag='<version>' \
  --set existingSecret=kafkamcp-production \
  --set 'config.clusters[0].bootstrap_servers=broker-1.kafka:9093,broker-2.kafka:9093'
```

The default values intentionally contain `REPLACE_ME:9093`; an unattended
installation should not accidentally connect to a development broker.

## Durable guarded operations

Enable SQLite and provide the approval signing key before enabling guarded
packs:

```bash
kubectl patch secret kafkamcp-production --type merge -p \
  '{"stringData":{"KAFKAMCP_APPROVAL_SIGNING_KEY":"replace-with-32-plus-random-bytes"}}'

helm upgrade --install kafkamcp ./deploy/helm/kafkamcp \
  --set image.tag='<version>' \
  --set existingSecret=kafkamcp-production \
  --set persistence.enabled=true \
  --set persistence.size=5Gi \
  --set 'config.server.tool_packs[0]=core-read' \
  --set 'config.server.tool_packs[1]=ops-write' \
  --set 'config.clusters[0].bootstrap_servers=broker-1.kafka:9093,broker-2.kafka:9093'
```

For an existing claim, set `persistence.existingClaim`. The chart injects:

```yaml
server:
  persistence:
    backend: sqlite
    path: /var/lib/kafkamcp/state.db
```

## Key values

| Parameter | Description | Default |
| --- | --- | --- |
| `replicaCount` | Pod count; must be `1` for guarded packs/SQLite | `1` |
| `persistence.enabled` | Provision/mount durable SQLite storage | `false` |
| `service.sessionAffinity` | Optional `None` or `ClientIP` affinity | `None` |
| `config.server.metrics_host` | Probe/metrics bind host inside pod | `0.0.0.0` |
| `serviceMonitor.enabled` | Create Prometheus ServiceMonitor | `false` |
| `networkPolicy.telemetry.enabled` | Permit OTLP ports | `false` |
| `ingress.stickySessions.enabled` | Add nginx cookie-affinity annotations | `false` |

## Telemetry egress

When `config.telemetry.enabled=true`, also enable the OTLP NetworkPolicy rule:

```yaml
networkPolicy:
  telemetry:
    enabled: true
    ports: [4317, 4318]
    cidr: "10.0.0.0/8" # optional restriction
```

For non-standard collector ports, use `networkPolicy.additionalEgress`.

## Validate

```bash
make helm-check
```

The check lints the chart, renders default/persistent/telemetry/session-affinity
scenarios, and proves unsafe guarded multi-replica settings are rejected.
