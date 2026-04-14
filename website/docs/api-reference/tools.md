---
sidebar_position: 1
title: "API Reference: Tools"
---

# API Reference: Tools

KafkaMCP exposes 22 MCP tools grouped into three tiers:

- **Core (P0)** — discovery, read, write, schemas, consumer groups
- **Advanced (P1)** — subscriptions, admin operations, search, offset resets, DLQ analysis
- **Intelligence (P2)** — aggregation, joins, schema advice, connectors, natural-language queries

:::info
The request snippets below show the **tool name and arguments payload**. In a real MCP session, the server wraps the JSON result inside the standard tool response envelope.
:::

## Tool index

### Core (P0)

| Tool | Purpose |
|---|---|
| `kafka_list_topics` | List topics with summary metadata |
| `kafka_describe_topic` | Get detailed topic metadata |
| `kafka_consume` | Read messages from a topic |
| `kafka_produce` | Publish messages to a topic |
| `kafka_list_schemas` | List Schema Registry subjects |
| `kafka_get_schema` | Get a full schema definition |
| `kafka_describe_consumer_group` | Inspect one consumer group |
| `kafka_list_consumer_groups` | List consumer groups |

### Advanced (P1)

| Tool | Purpose |
|---|---|
| `kafka_subscribe` | Poll new messages with consumer-group semantics |
| `kafka_search` | Search messages by key, header, value, or time |
| `kafka_create_topic` | Create a topic |
| `kafka_alter_topic` | Change topic configs |
| `kafka_delete_topic` | Delete a topic |
| `kafka_reset_offsets` | Reset consumer-group offsets |
| `kafka_check_schema_compatibility` | Test a proposed schema against Schema Registry rules |
| `kafka_analyze_dlq` | Analyze dead-letter queues |

### Intelligence (P2)

| Tool | Purpose |
|---|---|
| `kafka_aggregate` | Compute windowed aggregations |
| `kafka_suggest_schema_evolution` | Suggest safe schema evolution strategies |
| `kafka_list_connectors` | List Kafka Connect connectors |
| `kafka_describe_connector` | Inspect a Kafka Connect connector |
| `kafka_join` | Perform a time-windowed join across two topics |
| `kafka_query` | Translate a natural-language query into Kafka reads |

## Core (P0)

### `kafka_list_topics`

List Kafka topics in a cluster with partition counts, replication, retention, and estimated message counts.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `filter` | string | No | — | Glob filter such as `orders.*` or `*-dlq`. |
| `include_internal` | boolean | No | `false` | Include internal topics like `__consumer_offsets`. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_list_topics",
  "arguments": {
    "filter": "orders.*",
    "cluster": "production"
  }
}
```

**Example response**

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
  "total_count": 1,
  "cluster": "production"
}
```

### `kafka_describe_topic`

Get detailed metadata for a single topic, including partitions, replicas, ISR, offsets, and config.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | Topic name. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_describe_topic",
  "arguments": {
    "topic": "orders.created",
    "cluster": "production"
  }
}
```

**Example response**

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

### `kafka_consume`

Read messages from a topic using `latest`, `earliest`, a numeric offset, or an ISO 8601 timestamp.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | Topic name. |
| `partition` | number | No | all partitions | Specific partition. Use `-1` or omit for all partitions. |
| `offset` | string | No | `latest` | `earliest`, `latest`, numeric offset, or ISO 8601 timestamp. |
| `limit` | number | No | `10` | Maximum messages to return. Range: `1-1000`. |
| `deserializer` | string | No | `auto` | `auto`, `avro`, `protobuf`, `json`, `string`, or `bytes`. |
| `timeout_ms` | number | No | `5000` | Maximum wait time in milliseconds. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_consume",
  "arguments": {
    "topic": "orders.created",
    "offset": "latest",
    "limit": 2,
    "deserializer": "json",
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "messages": [
    {
      "partition": 2,
      "offset": 182034,
      "timestamp": "2025-01-01T12:00:00.000Z",
      "key": "ord_123",
      "value": {
        "order_id": "ord_123",
        "amount": 42.5,
        "status": "created"
      },
      "headers": {
        "content-type": "application/json"
      }
    }
  ],
  "count": 1,
  "topic": "orders.created",
  "deserialization": "json"
}
```

### `kafka_produce`

Publish one or more messages to a Kafka topic and return delivery metadata.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | Topic name. |
| `messages` | array of objects or single object | Yes | — | One or more messages with `key`, `value`, optional `headers`, and optional `partition`. Max 100 messages. |
| `serializer` | string | No | `auto` | `auto`, `avro`, `protobuf`, `json`, or `string`. |
| `schema_id` | number | No | — | Explicit Schema Registry schema ID to use during serialization. |
| `acks` | string | No | `all` | Producer ack level: `0`, `1`, or `all`. |
| `cluster` | string | No | default cluster | Named cluster to target. |

**Example request**

```json
{
  "name": "kafka_produce",
  "arguments": {
    "topic": "ops.events",
    "messages": [
      {
        "key": "deploy-123",
        "value": {
          "status": "started",
          "service": "billing"
        },
        "headers": {
          "source": "kafkamcp"
        }
      }
    ],
    "serializer": "json",
    "acks": "all",
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "deliveries": [
    {
      "partition": 1,
      "offset": 99012,
      "timestamp": "2025-01-01T12:01:05.000Z",
      "status": "delivered"
    }
  ],
  "count": 1,
  "topic": "ops.events",
  "serialization": "json",
  "acks": "all"
}
```

### `kafka_list_schemas`

List Schema Registry subjects with latest version and compatibility information.

:::tip
This tool is available when the selected deployment has Schema Registry configured.
:::

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `filter` | string | No | — | Glob filter for subject names. |

**Example request**

```json
{
  "name": "kafka_list_schemas",
  "arguments": {
    "filter": "orders.*"
  }
}
```

**Example response**

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

### `kafka_get_schema`

Fetch a full schema definition for a subject.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `subject` | string | Yes | — | Schema subject name such as `orders.created-value`. |
| `version` | string | No | `latest` | Schema version number or `latest`. |

**Example request**

```json
{
  "name": "kafka_get_schema",
  "arguments": {
    "subject": "orders.created-value",
    "version": "latest"
  }
}
```

**Example response**

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

### `kafka_describe_consumer_group`

Describe the state, members, assignments, offsets, and lag for one consumer group.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `group_id` | string | Yes | — | Consumer group ID. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_describe_consumer_group",
  "arguments": {
    "group_id": "orders-processor",
    "cluster": "production"
  }
}
```

**Example response**

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

### `kafka_list_consumer_groups`

List consumer groups with state, member count, protocol, and lag.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `state_filter` | string | No | — | Optional state filter such as `Stable`, `Empty`, or `Dead`. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_list_consumer_groups",
  "arguments": {
    "state_filter": "Stable",
    "cluster": "production"
  }
}
```

**Example response**

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
  "total_count": 1,
  "cluster": "production"
}
```

## Advanced (P1)

### `kafka_subscribe`

Poll for new messages using consumer-group semantics and return them as a batch.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | Topic name. |
| `group_id` | string | No | ephemeral group | Consumer group ID. If omitted, KafkaMCP creates a temporary group. |
| `auto_commit` | boolean | No | `true` | Commit offsets after consumption. |
| `max_messages` | number | No | `100` | Maximum messages to return. Range: `1-1000`. |
| `timeout_ms` | number | No | `10000` | Poll duration in milliseconds. Max: `30000`. |
| `deserializer` | string | No | `auto` | `auto`, `json`, `string`, or `bytes`. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_subscribe",
  "arguments": {
    "topic": "orders.created",
    "group_id": "agent-orders-monitor",
    "auto_commit": true,
    "max_messages": 25,
    "timeout_ms": 10000,
    "deserializer": "json",
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "messages": [
    {
      "partition": 0,
      "offset": 200,
      "timestamp": "2025-01-01T12:03:00.000Z",
      "key": "ord_555",
      "value": {
        "order_id": "ord_555",
        "status": "created"
      }
    }
  ],
  "count": 1,
  "topic": "orders.created",
  "group_id": "agent-orders-monitor",
  "auto_committed": true
}
```

### `kafka_search`

Search a topic by key, header, value content, or time range using a bounded scan.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | Topic name. |
| `key_filter` | string | No | — | Match messages whose key contains this string. |
| `header_key` | string | No | — | Require a specific header key. |
| `header_value` | string | No | — | Require a specific header value. |
| `value_contains` | string | No | — | Match messages whose value contains this string. |
| `start_time` | string | No | — | ISO 8601 lower bound for message timestamps. |
| `end_time` | string | No | — | ISO 8601 upper bound for message timestamps. |
| `max_scan` | number | No | `10000` | Maximum messages to scan. |
| `limit` | number | No | `10` | Maximum matching messages to return. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_search",
  "arguments": {
    "topic": "payments.events",
    "value_contains": "failed",
    "start_time": "2025-01-01T00:00:00Z",
    "limit": 5,
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "messages": [
    {
      "partition": 1,
      "offset": 5011,
      "timestamp": "2025-01-01T12:10:00.000Z",
      "key": "pay_777",
      "value": {
        "payment_id": "pay_777",
        "status": "failed"
      }
    }
  ],
  "matched_count": 1,
  "scanned_count": 250,
  "topic": "payments.events"
}
```

### `kafka_create_topic`

Create a new Kafka topic with partitions, replication factor, and optional configs.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | string | Yes | — | Topic name. |
| `partitions` | number | No | `1` | Number of partitions. |
| `replication_factor` | number | No | `1` | Replication factor. |
| `configs` | object | No | — | Topic configs such as `{"retention.ms": "604800000"}`. |
| `cluster` | string | No | default cluster | Named cluster to target. |

**Example request**

```json
{
  "name": "kafka_create_topic",
  "arguments": {
    "name": "ops.audit",
    "partitions": 6,
    "replication_factor": 3,
    "configs": {
      "retention.ms": "604800000"
    },
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "status": "created",
  "topic": "ops.audit",
  "partitions": 6,
  "replication_factor": 3
}
```

### `kafka_alter_topic`

Change one or more configuration values on an existing topic.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | string | Yes | — | Topic name. |
| `configs` | object | Yes | — | Configs to change. |
| `cluster` | string | No | default cluster | Named cluster to target. |

**Example request**

```json
{
  "name": "kafka_alter_topic",
  "arguments": {
    "name": "orders.created",
    "configs": {
      "retention.ms": "86400000"
    },
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "status": "altered",
  "topic": "orders.created",
  "configs": {
    "retention.ms": "86400000"
  }
}
```

### `kafka_delete_topic`

Delete a Kafka topic.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | string | Yes | — | Topic name to delete. |
| `cluster` | string | No | default cluster | Named cluster to target. |

**Example request**

```json
{
  "name": "kafka_delete_topic",
  "arguments": {
    "name": "scratch.test-topic",
    "cluster": "staging"
  }
}
```

**Example response**

```json
{
  "status": "deleted",
  "topic": "scratch.test-topic"
}
```

### `kafka_reset_offsets`

Reset offsets for a consumer group using `earliest`, `latest`, or `timestamp` strategies.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `group_id` | string | Yes | — | Consumer group ID. |
| `topic` | string | Yes | — | Topic whose offsets will be reset. |
| `strategy` | string | Yes | — | `earliest`, `latest`, or `timestamp`. |
| `value` | string | No | — | ISO 8601 timestamp when `strategy` is `timestamp`. |
| `dry_run` | boolean | No | `false` | Preview offsets without applying the reset. |
| `cluster` | string | No | default cluster | Named cluster to target. |

**Example request**

```json
{
  "name": "kafka_reset_offsets",
  "arguments": {
    "group_id": "orders-processor",
    "topic": "orders.created",
    "strategy": "timestamp",
    "value": "2025-01-01T00:00:00Z",
    "dry_run": true,
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "offsets": [
    {
      "topic": "orders.created",
      "partition": 0,
      "current_offset": 180000
    },
    {
      "topic": "orders.created",
      "partition": 1,
      "current_offset": 179800
    }
  ],
  "dry_run": true
}
```

### `kafka_check_schema_compatibility`

Check whether a proposed schema is compatible with the subject's configured compatibility policy.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `subject` | string | Yes | — | Schema subject name. |
| `schema` | string | Yes | — | Proposed schema as a JSON string. |
| `schema_type` | string | No | `AVRO` | `AVRO`, `PROTOBUF`, or `JSON`. |
| `version` | string | No | `latest` | Version to check against. |

**Example request**

```json
{
  "name": "kafka_check_schema_compatibility",
  "arguments": {
    "subject": "orders.created-value",
    "schema_type": "AVRO",
    "schema": "{\"type\":\"record\",\"name\":\"OrderCreated\",\"fields\":[{\"name\":\"order_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":[\"null\",\"string\"],\"default\":null}]}",
    "version": "latest"
  }
}
```

**Example response**

```json
{
  "is_compatible": true,
  "messages": []
}
```

### `kafka_analyze_dlq`

Consume recent messages from a dead-letter topic and summarize error categories.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | DLQ topic name. |
| `limit` | number | No | `100` | Maximum messages to analyze. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_analyze_dlq",
  "arguments": {
    "topic": "payments.failed.dlq",
    "limit": 100,
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "topic": "payments.failed.dlq",
  "total_messages": 100,
  "error_types": {
    "validation_error": 61,
    "processing_error": 24,
    "unknown": 15
  },
  "samples": [
    {
      "partition": 0,
      "offset": 50,
      "timestamp": "2025-01-01T12:15:00.000Z",
      "key": "pay_999",
      "value": {
        "payment_id": "pay_999"
      },
      "headers": {
        "error.type": "validation_error"
      }
    }
  ],
  "time_range": {
    "earliest": "2025-01-01T11:00:00.000Z",
    "latest": "2025-01-01T12:15:00.000Z"
  }
}
```

## Intelligence (P2)

### `kafka_aggregate`

Compute `count`, `sum`, `avg`, `min`, or `max` over a time window on a numeric field.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | Topic name. |
| `field` | string | No | empty | Dot-path to a numeric field such as `amount` or `metrics.latency_ms`. Empty means use the whole value. |
| `function` | string | Yes | — | `count`, `sum`, `avg`, `min`, or `max`. |
| `window_ms` | number | No | `300000` | Lookback window in milliseconds. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_aggregate",
  "arguments": {
    "topic": "payments.events",
    "field": "amount",
    "function": "sum",
    "window_ms": 300000,
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "topic": "payments.events",
  "field": "amount",
  "function": "sum",
  "window_ms": 300000,
  "value": 18450.75,
  "message_count": 250,
  "window_start": "2025-01-01T12:00:00Z",
  "window_end": "2025-01-01T12:05:00Z"
}
```

### `kafka_suggest_schema_evolution`

Suggest compatibility-friendly approaches for a requested schema change.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `subject` | string | Yes | — | Schema subject name. |
| `change` | string | Yes | — | Plain-English change request such as `add a new field for user email`. |

**Example request**

```json
{
  "name": "kafka_suggest_schema_evolution",
  "arguments": {
    "subject": "orders.created-value",
    "change": "rename field from name to full_name"
  }
}
```

**Example response**

```json
{
  "subject": "orders.created-value",
  "change": "rename field from name to full_name",
  "suggestions": [
    {
      "strategy": "add_alias",
      "description": "Use Avro aliases to rename a field while maintaining compatibility.",
      "compatible": true,
      "steps": [
        "Add the new field name with the old name as an alias",
        "Register the new schema version",
        "Update producers to use the new field name",
        "Consumers will map the old name to the new name via the alias"
      ]
    }
  ],
  "count": 1
}
```

### `kafka_list_connectors`

List Kafka Connect connectors with type, state, and worker placement.

:::tip
This tool requires `connect_url` on the selected cluster configuration.
:::

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `cluster` | string | No | default cluster | Named cluster whose Connect REST API should be queried. |

**Example request**

```json
{
  "name": "kafka_list_connectors",
  "arguments": {
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "connectors": [
    {
      "name": "jdbc-orders-sink",
      "type": "sink",
      "state": "RUNNING",
      "worker": "connect-1:8083"
    }
  ],
  "total_count": 1
}
```

### `kafka_describe_connector`

Return the full config and task status for a Kafka Connect connector.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | string | Yes | — | Connector name. |
| `cluster` | string | No | default cluster | Named cluster whose Connect REST API should be queried. |

**Example request**

```json
{
  "name": "kafka_describe_connector",
  "arguments": {
    "name": "jdbc-orders-sink",
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "name": "jdbc-orders-sink",
  "type": "sink",
  "state": "RUNNING",
  "worker": "connect-1:8083",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "topics": "orders.created"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker": "connect-1:8083"
    }
  ]
}
```

### `kafka_join`

Perform a time-windowed join across two topics on either the message key or a field inside the message value.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `left_topic` | string | Yes | — | Left topic name. |
| `right_topic` | string | Yes | — | Right topic name. |
| `key_field` | string | No | empty | Dot-path to join on. If omitted, KafkaMCP joins on the Kafka message key. |
| `window_ms` | number | No | `60000` | Join lookback window in milliseconds. |
| `limit` | number | No | `100` | Maximum joined rows to return. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_join",
  "arguments": {
    "left_topic": "orders.created",
    "right_topic": "payments.completed",
    "key_field": "order_id",
    "window_ms": 600000,
    "limit": 20,
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "joined": [
    {
      "key": "ord_123",
      "left_value": {
        "order_id": "ord_123",
        "amount": 42.5
      },
      "right_value": {
        "order_id": "ord_123",
        "payment_id": "pay_123",
        "status": "completed"
      },
      "left_topic": "orders.created",
      "right_topic": "payments.completed",
      "timestamp": "2025-01-01T12:20:00.000Z"
    }
  ],
  "left_count": 200,
  "right_count": 180,
  "match_count": 1,
  "window_ms": 600000
}
```

### `kafka_query`

Interpret a natural-language request, choose a likely topic, and return matching or recent messages.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `query` | string | Yes | — | Natural-language description of the data to retrieve. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_query",
  "arguments": {
    "query": "last 10 failed payments",
    "cluster": "production"
  }
}
```

**Example response**

```json
{
  "interpreted_as": "Consume 10 messages from topic \"payments.events\" filtered by \"failed\"",
  "topic": "payments.events",
  "filter": "failed",
  "messages": [
    {
      "partition": 1,
      "offset": 5011,
      "timestamp": "2025-01-01T12:10:00.000Z",
      "key": "pay_777",
      "value": {
        "payment_id": "pay_777",
        "status": "failed"
      }
    }
  ],
  "count": 1
}
```

## Practical notes

- omit `cluster` to use the default cluster
- schema tools require `schema_registry.url`
- connector tools require `connect_url`
- topic list and resource reads may be filtered by your policy rules when `default_deny` is enabled
- every tool call is subject to audit logging and rate limiting
