---
sidebar_position: 3
title: Multi-Cluster
---

# Multi-Cluster

KafkaMCP can connect to multiple Kafka clusters at the same time.

This is useful when you need one MCP server to route traffic across environments such as:

- production and staging
- multiple business units
- regional Kafka deployments
- a managed cluster plus a local development cluster

## Configure more than one cluster

Define each cluster under `clusters:` and give each one a unique `name`.

```yaml
server:
  transport: sse
  host: 0.0.0.0
  port: 3100
  log_level: info

clusters:
  - name: production
    bootstrap_servers: "prod-broker-1:9092,prod-broker-2:9092,prod-broker-3:9092"
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: "${PROD_KAFKA_USERNAME}"
    sasl_password: "${PROD_KAFKA_PASSWORD}"
    schema_registry:
      url: "https://prod-schema-registry:8081"
      auth:
        username: "${PROD_SR_USERNAME}"
        password: "${PROD_SR_PASSWORD}"
    default: true

  - name: staging
    bootstrap_servers: "staging-broker-1:9092"
    security_protocol: PLAINTEXT
    schema_registry:
      url: "http://staging-schema-registry:8081"
```

## Default cluster selection

KafkaMCP resolves the target cluster like this:

1. if a tool call includes `cluster`, that named cluster is used
2. otherwise KafkaMCP uses the cluster marked `default: true`
3. if no cluster is explicitly marked as default, KafkaMCP uses the first cluster in the list

:::tip
Always set exactly one cluster as `default: true`. It removes ambiguity for agents and for human operators reading the config.
:::

## Use the `cluster` parameter on tools

Most Kafka tools accept an optional `cluster` parameter.

### Read from staging

```json
{
  "name": "kafka_consume",
  "arguments": {
    "cluster": "staging",
    "topic": "orders.created",
    "limit": 5,
    "offset": "latest"
  }
}
```

### Produce to production

```json
{
  "name": "kafka_produce",
  "arguments": {
    "cluster": "production",
    "topic": "ops.events",
    "messages": [
      {
        "key": "deploy-123",
        "value": {
          "status": "started",
          "service": "billing"
        }
      }
    ]
  }
}
```

If you omit `cluster`, the default cluster is used.

## Discover clusters with `kafka://clusters`

KafkaMCP exposes a discovery resource for configured clusters:

```text
kafka://clusters
```

Example response shape:

```json
{
  "clusters": [
    {
      "name": "production",
      "bootstrap_servers": "prod-broker-1:9092,prod-broker-2:9092,prod-broker-3:9092",
      "has_schema_registry": true,
      "is_default": true
    },
    {
      "name": "staging",
      "bootstrap_servers": "staging-broker-1:9092",
      "has_schema_registry": true,
      "is_default": false
    }
  ],
  "total_count": 2
}
```

That resource gives agents a safe way to inspect routing options before making tool calls.

## Multi-cluster behavior notes

### Schema Registry is cluster-aware

KafkaMCP builds a Schema Registry client per configured cluster when `schema_registry.url` is present.

### Kafka Connect is cluster-aware

Connector tools use the `connect_url` defined on the selected cluster.

### Audit records include the cluster name

Every successful or failed tool call records the resolved cluster in the audit log.

### Policies are agent-centric

Policies define what an agent can do, while the `cluster` parameter decides where it happens.

## Recommended operating model

- use descriptive names such as `production`, `staging`, `eu-west-1`, or `redpanda-lab`
- keep one and only one default cluster
- require explicit `cluster` values in automation that touches production
- keep credentials isolated per cluster with environment variable expansion

:::warning
If two environments expose similar topic names, an omitted `cluster` value can send a request to the wrong place. Use explicit `cluster` values for anything sensitive.
:::
