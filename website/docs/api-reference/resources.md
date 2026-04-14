---
sidebar_position: 2
title: "API Reference: Resources"
---

# API Reference: Resources

KafkaMCP exposes 10 read-only MCP resources. They return JSON documents that agents can browse and reason over.

:::info
Resource reads are served from the default cluster where cluster-specific data is involved. Use `kafka://clusters` to discover configured cluster names and use tool calls with the `cluster` parameter when you need explicit routing.
:::

## Resource index

| URI | Description |
|---|---|
| `kafka://cluster/metadata` | Cluster brokers, controller, and topic counts |
| `kafka://clusters` | Configured cluster discovery resource |
| `kafka://topics` | Topic catalog |
| `kafka://topics/{name}` | Detailed topic metadata |
| `kafka://schemas` | Schema Registry subject catalog |
| `kafka://schemas/{subject}` | Full schema detail |
| `kafka://groups` | Consumer group catalog |
| `kafka://groups/{id}` | Detailed consumer group state and lag |
| `kafka://audit/log` | Recent audit log entries |
| `kafka://topic_graph` | Topic and consumer-group topology graph |

## `kafka://cluster/metadata`

**Description:** Returns cluster-wide Kafka metadata.

**Example response shape:**

```json
{
  "cluster_id": "q1w2e3r4t5",
  "controller": {
    "id": 1,
    "host": "broker-1",
    "port": 9092,
    "rack": "use1-az1"
  },
  "brokers": [
    {
      "id": 1,
      "host": "broker-1",
      "port": 9092,
      "rack": "use1-az1"
    },
    {
      "id": 2,
      "host": "broker-2",
      "port": 9092,
      "rack": "use1-az2"
    }
  ],
  "broker_count": 2,
  "topic_count": 48,
  "partition_count": 216,
  "kraft_mode": true
}
```

## `kafka://clusters`

**Description:** Lists all configured Kafka clusters for discovery.

**Example response shape:**

```json
{
  "clusters": [
    {
      "name": "production",
      "bootstrap_servers": "prod-broker-1:9092,prod-broker-2:9092",
      "has_schema_registry": true,
      "is_default": true
    },
    {
      "name": "staging",
      "bootstrap_servers": "staging-broker-1:9092",
      "has_schema_registry": false,
      "is_default": false
    }
  ],
  "total_count": 2
}
```

## `kafka://topics`

**Description:** Lists topics visible to the caller.

**Example response shape:**

```json
{
  "topics": [
    {
      "name": "orders.created",
      "partitions": 12,
      "replication_factor": 3,
      "retention_ms": 604800000,
      "is_internal": false,
      "message_count_estimate": 1452033,
      "schema_subject": "orders.created-value"
    }
  ],
  "total_count": 1
}
```

## `kafka://topics/{name}`

**Description:** Returns detailed metadata for a single topic.

**Example URI:**

```text
kafka://topics/orders.created
```

**Example response shape:**

```json
{
  "name": "orders.created",
  "partitions": [
    {
      "id": 0,
      "leader": 1,
      "replicas": [1, 2, 3],
      "isr": [1, 2, 3],
      "earliest_offset": 0,
      "latest_offset": 182034
    }
  ],
  "replication_factor": 3,
  "configs": {
    "cleanup.policy": "delete",
    "retention.ms": "604800000"
  },
  "is_internal": false,
  "message_count_estimate": 1452033
}
```

## `kafka://schemas`

**Description:** Lists Schema Registry subjects from the default cluster.

**Example response shape:**

```json
{
  "subjects": [
    {
      "subject_name": "orders.created-value",
      "latest_version": 7,
      "schema_type": "AVRO",
      "compatibility_level": "BACKWARD"
    }
  ],
  "total_count": 1
}
```

## `kafka://schemas/{subject}`

**Description:** Returns a single schema subject with parsed schema metadata.

**Example URI:**

```text
kafka://schemas/orders.created-value
```

**Example response shape:**

```json
{
  "subject": "orders.created-value",
  "version": 7,
  "schema_id": 42,
  "schema_type": "AVRO",
  "compatibility_level": "BACKWARD",
  "schema": {
    "type": "record",
    "name": "OrderCreated",
    "fields": [
      {"name": "order_id", "type": "string"},
      {"name": "amount", "type": "double"}
    ]
  },
  "field_summary": "order_id:string, amount:double",
  "references": []
}
```

## `kafka://groups`

**Description:** Lists consumer groups from the default cluster.

**Example response shape:**

```json
{
  "groups": [
    {
      "group_id": "orders-processor",
      "state": "Stable",
      "member_count": 3,
      "protocol": "range",
      "total_lag": 18
    }
  ],
  "total_count": 1
}
```

## `kafka://groups/{id}`

**Description:** Returns a detailed consumer group view.

**Example URI:**

```text
kafka://groups/orders-processor
```

**Example response shape:**

```json
{
  "group_id": "orders-processor",
  "state": "Stable",
  "protocol_type": "consumer",
  "protocol": "range",
  "coordinator": {
    "id": 1,
    "host": "broker-1",
    "port": 9092
  },
  "members": [
    {
      "member_id": "consumer-1",
      "client_id": "orders-service",
      "host": "/10.0.0.12",
      "assignment": [
        {
          "topic": "orders.created",
          "partitions": [0, 1, 2]
        }
      ]
    }
  ],
  "offsets": [
    {
      "topic": "orders.created",
      "partition": 0,
      "current_offset": 100,
      "log_end_offset": 106,
      "lag": 6
    }
  ],
  "total_lag": 18
}
```

## `kafka://audit/log`

**Description:** Returns recent in-memory audit entries.

**Example response shape:**

```json
{
  "entries": [
    {
      "timestamp": "2025-01-01T12:00:00Z",
      "agent_id": "incident-agent",
      "tool": "kafka_consume",
      "parameters": {
        "topic": "orders.created",
        "limit": 10
      },
      "cluster": "production",
      "result": {
        "status": "success",
        "message_count": 10
      },
      "latency_ms": 48
    }
  ],
  "count": 1
}
```

## `kafka://topic_graph`

**Description:** Returns a topic and consumer-group topology graph.

**Example response shape:**

```json
{
  "nodes": [
    {
      "id": "topic:orders.created",
      "type": "topic",
      "name": "orders.created"
    },
    {
      "id": "group:orders-processor",
      "type": "consumer_group",
      "name": "orders-processor"
    }
  ],
  "edges": [
    {
      "source": "topic:orders.created",
      "target": "group:orders-processor",
      "type": "consumes"
    }
  ]
}
```

## Usage notes

- resources return `application/json`
- topic, schema, and group resources respect policy checks where applicable
- `kafka://audit/log` is especially useful for debugging autonomous workflows
- `kafka://clusters` is the discovery entry point for multi-cluster setups
