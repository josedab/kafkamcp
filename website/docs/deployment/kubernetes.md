---
sidebar_position: 3
title: Kubernetes Deployment
---

# Kubernetes Deployment

KafkaMCP runs well on Kubernetes when you need a shared, network-accessible MCP endpoint.

:::warning Pre-release image
The Helm chart is linted and ready for local validation, but the referenced
GHCR image is not published until the first tagged release.
:::

## Helm values reference

The Helm chart at [`deploy/helm/kafkamcp/`](https://github.com/josedab/kafkamcp/tree/main/deploy/helm/kafkamcp) provides production-oriented deployment defaults. The values below show the target shape — see `deploy/helm/kafkamcp/values.yaml` for all defaults.

```yaml
replicaCount: 1

image:
  repository: ghcr.io/josedab/kafkamcp
  tag: "0.1.0"

service:
  type: ClusterIP
  port: 3100

config:
  server:
    transport: streamable-http
    metrics_host: 0.0.0.0
    metrics_port: 3101
  clusters:
    - name: production
      bootstrap_servers: "kafka-headless.kafka:9092"
      security_protocol: SASL_SSL
      sasl_mechanism: SCRAM-SHA-512
      sasl_username: "${KAFKA_SASL_USERNAME}"
      sasl_password: "${KAFKA_SASL_PASSWORD}"
      tls_min_version: "1.2"

resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: 2000m
    memory: 1Gi

persistence:
  enabled: true
  size: 5Gi
```

:::warning Singleton safety
Guarded packs (`ops-write` / `governance`) and SQLite persistence require one
active replica. The chart rejects `replicaCount > 1` and HPA in those modes and
uses a `Recreate` rollout. Multi-replica deployment is supported only for the
read-only `core-read` surface with in-memory state.
:::

:::info
See [`deploy/helm/kafkamcp/`](https://github.com/josedab/kafkamcp/tree/main/deploy/helm/kafkamcp) for the full chart including templates for Deployment, Service, ConfigMap, HPA, NetworkPolicy, PDB, ServiceMonitor, and more.
:::

## Basic manifest

The example below shows a minimal `ConfigMap`, `Deployment`, and `Service`.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafkamcp-config
  namespace: kafka

data:
  kafkamcp.yaml: |
    server:
      transport: sse
      host: 0.0.0.0
      port: 3100
      metrics_host: 0.0.0.0
      metrics_port: 3101
      log_level: info
      persistence:
        backend: sqlite
        path: /var/lib/kafkamcp/state.db

    clusters:
      - name: production
        bootstrap_servers: "kafka-headless.kafka.svc.cluster.local:9092"
        security_protocol: SASL_SSL
        sasl_mechanism: SCRAM-SHA-512
        sasl_username: "${KAFKA_USERNAME}"
        sasl_password: "${KAFKA_PASSWORD}"
        tls_min_version: "1.2"
        schema_registry:
          url: "${SCHEMA_REGISTRY_URL}"
        default: true

    policies:
      default_deny: true
      identity:
        provider: static
        allow_header_fallback: false
        static:
          tokens:
            - token: "${KAFKAMCP_AGENT_TOKEN}"
              agent_id: incident-agent
      agents:
        - id: incident-agent
          role: reader
          topics:
            - pattern: "orders.*"
              permissions: [read]
            - pattern: "*.dlq"
              permissions: [read]
          consumer_groups:
            permissions: [describe]
          schemas:
            permissions: [read]
          rate_limit:
            requests_per_minute: 120

    schema_cache:
      max_entries: 10000
      ttl_seconds: 300

    audit:
      enabled: true
      max_entries: 10000
      log_file: /var/log/kafkamcp/audit.jsonl
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: kafkamcp-state
  namespace: kafka
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 5Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafkamcp
  namespace: kafka
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: kafkamcp
  template:
    metadata:
      labels:
        app: kafkamcp
    spec:
      containers:
        - name: kafkamcp
          image: ghcr.io/josedab/kafkamcp:VERSION
          args: ["--config", "/etc/kafkamcp/kafkamcp.yaml"]
          ports:
            - name: http
              containerPort: 3100
            - name: metrics
              containerPort: 3101
          env:
            - name: KAFKAMCP_AGENT_TOKEN
              valueFrom:
                secretKeyRef:
                  name: kafkamcp-secrets
                  key: agent-token
            - name: KAFKA_USERNAME
              valueFrom:
                secretKeyRef:
                  name: kafkamcp-secrets
                  key: kafka-username
            - name: KAFKA_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: kafkamcp-secrets
                  key: kafka-password
            - name: SCHEMA_REGISTRY_URL
              valueFrom:
                secretKeyRef:
                  name: kafkamcp-secrets
                  key: schema-registry-url
          volumeMounts:
            - name: config
              mountPath: /etc/kafkamcp
              readOnly: true
            - name: audit-logs
              mountPath: /var/log/kafkamcp
            - name: state
              mountPath: /var/lib/kafkamcp
          readinessProbe:
            httpGet:
              path: /ready
              port: metrics
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /livez
              port: metrics
            initialDelaySeconds: 15
            periodSeconds: 20
      volumes:
        - name: config
          configMap:
            name: kafkamcp-config
        - name: audit-logs
          emptyDir: {}
        - name: state
          persistentVolumeClaim:
            claimName: kafkamcp-state
---
apiVersion: v1
kind: Service
metadata:
  name: kafkamcp
  namespace: kafka
spec:
  selector:
    app: kafkamcp
  ports:
    - name: http
      port: 3100
      targetPort: http
    - name: metrics
      port: 3101
      targetPort: metrics
  type: ClusterIP
```

## Deployment guidance

### Expose the right transport

Use `sse` or `streamable-http` in Kubernetes. Those transports are designed for networked access.

### Keep secrets out of the ConfigMap

Use Kubernetes `Secret` objects for:

- SASL usernames and passwords
- Schema Registry credentials
- TLS materials

### Scrape the metrics port

Prometheus should scrape `3101`, not `3100`.

The metrics listener defaults to loopback in direct binary deployments. In a
pod, set `server.metrics_host: 0.0.0.0` and restrict port `3101` with a
NetworkPolicy. Use `/livez` for liveness and `/ready` for readiness.

### Make the default cluster explicit

In multi-cluster configurations, set exactly one cluster to `default: true`.

## Recommended production extras

- NetworkPolicy rules scoped to your ingress and monitoring namespaces
- a `ReadWriteOnce` PVC for SQLite when durable approval/idempotency state is required
- persistent audit-log shipping to your SIEM
- ingress or gateway rules with TLS when exposing KafkaMCP beyond the cluster
- OTLP egress rules (`4317`/`4318`) when telemetry is enabled

Do not add an HPA or multiple active replicas to a guarded/SQLite deployment.

Kubernetes deployment works best when KafkaMCP is treated like shared infrastructure: explicit config, strong policies, and standard platform observability.

## Graceful shutdown

KafkaMCP handles `SIGINT` and `SIGTERM` for graceful shutdown. When a signal is received:

1. The server stops accepting new connections.
2. In-flight requests are given up to **10 seconds** to complete (the drain period).
3. The metrics/health server is shut down with the same 10-second drain window.
4. The process exits cleanly.

Set `terminationGracePeriodSeconds` in your pod spec to at least **15 seconds** (10 s drain + margin) to allow the shutdown to complete before Kubernetes force-kills the container:

```yaml
spec:
  terminationGracePeriodSeconds: 30
```
