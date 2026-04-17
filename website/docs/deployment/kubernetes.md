---
sidebar_position: 3
title: Kubernetes Deployment
---

# Kubernetes Deployment

KafkaMCP runs well on Kubernetes when you need a shared, network-accessible MCP endpoint.

## Helm values reference

The repository PRD defines the target Helm values shape below. Use it as the reference contract for chart-based deployments.

```yaml
replicaCount: 3

image:
  repository: ghcr.io/kafkamcp/kafkamcp
  tag: "0.1.0"

service:
  type: ClusterIP
  port: 3100

config:
  transport: sse
  clusters:
    - name: production
      bootstrapServers: "kafka-headless.kafka:9092"
      securityProtocol: SASL_SSL
      saslMechanism: SCRAM-SHA-512
  policyConfigMap: kafkamcp-policies

resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: 2000m
    memory: 1Gi

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilization: 70

podDisruptionBudget:
  minAvailable: 1
```

:::info
That values block comes from the PRD. If you are building your own chart today, keep your chart values aligned with this shape.
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
      log_level: info

    clusters:
      - name: production
        bootstrap_servers: "kafka-headless.kafka.svc.cluster.local:9092"
        security_protocol: SASL_SSL
        sasl_mechanism: SCRAM-SHA-512
        sasl_username: "${KAFKA_USERNAME}"
        sasl_password: "${KAFKA_PASSWORD}"
        schema_registry:
          url: "${SCHEMA_REGISTRY_URL}"
        default: true

    policies:
      default_deny: true
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
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafkamcp
  namespace: kafka
spec:
  replicas: 2
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
          image: ghcr.io/kafkamcp/kafkamcp:latest
          args: ["--config", "/etc/kafkamcp/kafkamcp.yaml"]
          ports:
            - name: http
              containerPort: 3100
            - name: metrics
              containerPort: 3101
          env:
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
          readinessProbe:
            httpGet:
              path: /healthz
              port: metrics
            initialDelaySeconds: 5
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /healthz
              port: metrics
            initialDelaySeconds: 15
            periodSeconds: 20
      volumes:
        - name: config
          configMap:
            name: kafkamcp-config
        - name: audit-logs
          emptyDir: {}
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

### Make the default cluster explicit

In multi-cluster configurations, set exactly one cluster to `default: true`.

## Recommended production extras

- `PodDisruptionBudget`
- `HorizontalPodAutoscaler`
- NetworkPolicy
- persistent audit log storage if you need durable logs inside the cluster
- ingress or gateway rules when exposing KafkaMCP beyond the cluster

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
