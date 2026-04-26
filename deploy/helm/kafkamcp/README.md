# KafkaMCP Helm Chart

A Helm chart for deploying KafkaMCP — the MCP server for Apache Kafka.

## Installation

```bash
helm install kafkamcp ./deploy/helm/kafkamcp \
  --set config.clusters[0].bootstrap_servers=kafka:9092
```

## Configuration

See `values.yaml` for all configurable parameters.

### Key Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `config.clusters[0].bootstrap_servers` | Kafka broker addresses | `kafka:9092` |
| `config.server.transport` | MCP transport protocol | `streamable-http` |
| `autoscaling.enabled` | Enable HPA | `false` |
| `podDisruptionBudget.enabled` | Enable PDB | `false` |
| `serviceMonitor.enabled` | Enable Prometheus ServiceMonitor | `false` |
| `ingress.enabled` | Enable Ingress | `false` |
| `config.masking.enabled` | Enable PII masking | `false` |

### Using Secrets for Kafka Credentials

```bash
kubectl create secret generic kafka-creds \
  --from-literal=KAFKA_SASL_USERNAME=user \
  --from-literal=KAFKA_SASL_PASSWORD=pass

helm install kafkamcp ./deploy/helm/kafkamcp \
  --set existingSecret=kafka-creds \
  --set 'config.clusters[0].bootstrap_servers=kafka:9092' \
  --set 'config.clusters[0].security_protocol=SASL_SSL' \
  --set 'config.clusters[0].sasl_mechanism=PLAIN' \
  --set 'config.clusters[0].sasl_username=${KAFKA_SASL_USERNAME}' \
  --set 'config.clusters[0].sasl_password=${KAFKA_SASL_PASSWORD}'
```

## CRD: KafkaMCPCluster

The chart includes a `KafkaMCPCluster` CRD for operator-style cluster registration:

```yaml
apiVersion: kafkamcp.io/v1alpha1
kind: KafkaMCPCluster
metadata:
  name: production
spec:
  bootstrapServers: "kafka-prod:9092"
  securityProtocol: SASL_SSL
  saslMechanism: SCRAM-SHA-256
  credentialsSecret: kafka-prod-creds
  schemaRegistryURL: "https://sr-prod:8081"
  replicas: 3
  transport: streamable-http
```
