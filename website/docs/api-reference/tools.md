---
sidebar_position: 1
title: "API Reference: Tools"
toc_max_heading_level: 3
---

# API Reference: Tools

KafkaMCP exposes 154 MCP tools across the following categories:

- **Core (P0)** — discovery, read, write, schemas, consumer groups
- **Advanced (P1)** — subscriptions, admin operations, search, offset resets, DLQ analysis
- **Intelligence (P2)** — aggregation, joins, schema advice, connectors, natural-language queries
- **Event-Driven (P3)** — watch subscriptions, DLQ remediation, schema diffs
- **Connector & Operations (Advanced)** — connector lifecycle, time travel, recommendations, tracing, ACLs, drift, playground
- **AI & Analytics (Feature)** — anomaly detection, NL queries, cost analysis, workflows, GitOps, knowledge graph, contracts, lag diagnosis, migration, lineage, personas, forecasting
- **Streaming** — real-time streaming sessions, multi-agent collaboration
- **Advisor** — best-practice scanning and advisory reports
- **Agent Profiling** — agent performance profiling and optimization
- **Alerting** — alerting rules for Kafka metrics
- **Capacity** — capacity planning and forecasting
- **Catalog** — data catalog search, tagging, and statistics
- **Chaos Engineering** — controlled fault injection and chaos experiments
- **Compliance** — GDPR, HIPAA, SOC2, PCI-DSS compliance scanning
- **Config VCS** — configuration version control and rollback
- **Cost Attribution** — real-time cost metrics, allocation, and budgets
- **Contracts** — data contract enforcement and inference
- **Dashboard** — auto-provisioned monitoring dashboards
- **Federation** — multi-instance federation and cross-instance queries
- **Flow Debug** — live message flow debugging and bottleneck detection
- **Memory** — agent memory storage, recall, and semantic search
- **Migration** — migration execution, status tracking, and rollback
- **OpenAPI** — OpenAPI spec generation from MCP tools
- **Plugin** — plugin marketplace search and installation
- **Quota** — adaptive rate limit status and quota adjustments
- **Replay** — message bookmarking, diff, and replay simulation
- **Replication** — cross-cluster replication status and failover readiness
- **Sandbox** — isolated sandbox environments for experimentation
- **Schema Workflow** — schema proposal, approval, and rollback workflows
- **StreamSQL** — streaming SQL queries against Kafka topics
- **Task Queue** — priority task queues for multi-agent coordination
- **TimeSeries** — advanced time-series analysis and trend detection
- **Tracing Flow** — distributed tracing with W3C Trace Context
- **Usage** — usage analytics and chargeback reports
- **View Store** — materialized views over Kafka topics
- **DAG Workflow** — reusable DAG-based workflow templates
- **Pipeline** — streaming function pipelines
- **Semantic Search** — semantic indexing and RAG context building
- **Onboarding** — guided onboarding and cluster discovery
- **OPA Policy** — OPA Rego policy evaluation and management
- **Auto-Heal** — consumer group auto-healing and remediation
- **Collaboration** — distributed locks, vector clocks, and agent messaging
- **Gateway** — multi-protocol streaming backend gateway
- **Kubernetes Operator** — Kubernetes operator management

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

:::note Schema tools availability
`kafka_list_schemas`, `kafka_get_schema`, and `kafka_check_schema_compatibility` are only registered when `schema_registry.url` is configured for at least one cluster. If no Schema Registry URL is set, these tools will not appear in the MCP tool list.
:::

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

### Event-Driven (P3)

| Tool | Purpose |
|---|---|
| `kafka_watch` | Register event-driven watch subscriptions on topic patterns |
| `kafka_remediate_dlq` | Auto-remediate DLQ messages: retry, route, or suggest fixes |
| `kafka_schema_diff` | Field-by-field diff between two schema versions |

### Connector & Operations (Advanced)

| Tool | Purpose |
|---|---|
| `kafka_create_connector` | Create a new Kafka Connect connector |
| `kafka_update_connector` | Update an existing connector configuration |
| `kafka_delete_connector` | Delete a Kafka Connect connector |
| `kafka_restart_connector` | Restart a connector or specific failed tasks |
| `kafka_connector_health` | Health score and diagnostics for a connector |
| `kafka_time_travel` | Point-in-time topic queries: snapshot or diff state |
| `kafka_recommend` | Intelligent topic optimization recommendations |
| `kafka_trace_message` | End-to-end message tracing via correlation IDs |
| `kafka_list_acls` | List Kafka ACLs with security analysis |
| `kafka_config_drift` | Detect configuration drift across clusters |
| `kafka_playground` | Sandboxed environment for safe Kafka experimentation |

### AI & Analytics (Feature)

| Tool | Purpose |
|---|---|
| `kafka_anomalies` | AI-detected anomalies in lag, throughput, and error rates |
| `kafka_anomaly_ingest` | Manually ingest data points for anomaly detection |
| `kafka_nl_query` | Query Kafka with plain English (NL → pipeline) |
| `kafka_cost_analyze` | Analyze storage and compute costs for a topic |
| `kafka_cost_waste` | Detect cost waste: idle topics, over-replication, etc. |
| `kafka_cost_whatif` | Simulate config changes and see cost impact |
| `kafka_workflows` | Pre-built workflow templates for common operations |
| `kafka_gitops_diff` | Compare declarative YAML config against live cluster state |
| `kafka_knowledge_graph` | Query topic relationships and schema dependencies |
| `kafka_platform_info` | Get info about configured Kafka-compatible platforms |
| `kafka_validate_contract` | Validate messages against data contracts |
| `kafka_diagnose_lag` | Deep diagnostic analysis of consumer group lag |
| `kafka_migration_plan` | Plan and validate Kafka migrations between clusters |
| `kafka_trace_lineage` | Track data lineage and PII/compliance annotations |
| `kafka_group_health` | Consumer group health scoring with auto-remediation hints |
| `kafka_persona` | Declarative agent personas (SRE, data-engineer, etc.) |
| `kafka_forecast` | ML-based capacity forecasting (throughput, storage, lag) |

### Streaming

| Tool | Purpose |
|---|---|
| `kafka_stream` | Manage streaming consumer sessions for real-time delivery |
| `kafka_agent_registry` | Multi-agent collaboration: register, claim partitions, handoff |

### Advisor

| Tool | Purpose |
|---|---|
| `kafka_advisor_scan` | Scan topics for best-practice violations and improvement opportunities |
| `kafka_advisor_report` | Generate a detailed advisory report with health scores and recommendations |

### Agent Profiling

| Tool | Purpose |
|---|---|
| `kafka_agent_profile` | Profile agent performance: latency, tool call patterns, error rates |
| `kafka_agent_optimize` | Get optimization suggestions based on agent call patterns |
| `kafka_agent_report` | Generate a comprehensive agent efficiency report |

### Alerting

| Tool | Purpose |
|---|---|
| `kafka_dashboard_create` | Auto-provision Kafka monitoring dashboards for Grafana or Datadog |
| `kafka_alert_create` | Create an alerting rule for Kafka metrics with configurable thresholds |
| `kafka_alert_list` | List all configured alerting rules |
| `kafka_alert_delete` | Delete an alerting rule by ID |

### Capacity

| Tool | Purpose |
|---|---|
| `kafka_capacity_plan` | Generate a comprehensive capacity plan with ML-based forecasting |
| `kafka_capacity_forecast` | Forecast capacity for a specific topic using exponential smoothing |

### Catalog

| Tool | Purpose |
|---|---|
| `kafka_catalog_search` | Search the data catalog by name, tag, owner, or quality score |
| `kafka_catalog_tag` | Add tags to a catalog entry or set lineage information |
| `kafka_catalog_stats` | Get catalog statistics: entry counts, freshness, quality scores |

### Chaos Engineering

| Tool | Purpose |
|---|---|
| `kafka_chaos_inject` | Inject controlled chaos: broker outage, network partition, slow consumer, etc. |
| `kafka_chaos_status` | Get status of chaos experiments |
| `kafka_chaos_stop` | Stop a running chaos experiment and trigger rollback |

### Compliance

| Tool | Purpose |
|---|---|
| `kafka_compliance_scan` | Scan topics for GDPR, HIPAA, SOC2, or PCI-DSS compliance |
| `kafka_compliance_report` | Generate a comprehensive compliance report with scores and violations |
| `kafka_compliance_violations` | List detected compliance violations with filters |

### Config VCS

| Tool | Purpose |
|---|---|
| `kafka_config_history` | Get configuration change history with full diff details |
| `kafka_config_rollback` | Rollback a resource configuration to a previous version |
| `kafka_config_compare` | Compare two versions of a resource configuration |

### Cost Attribution

| Tool | Purpose |
|---|---|
| `kafka_cost_live` | Get real-time cost metrics with per-hour/day/month breakdowns |
| `kafka_cost_allocate` | Create or view cost allocation groups for teams or services |
| `kafka_cost_budget` | Create spending budgets with alert thresholds |

### Contracts

| Tool | Purpose |
|---|---|
| `kafka_contract_violations` | View contract violations, enforce contracts, or infer from samples |

### Federation

| Tool | Purpose |
|---|---|
| `kafka_federation_join` | Manage federation membership: join, leave, heartbeat |
| `kafka_federation_query` | Execute a query across all federated instances |
| `kafka_federation_status` | Get federation status: instances, policies, health |

### Flow Debug

| Tool | Purpose |
|---|---|
| `kafka_flow_debug` | Live message flow debugging with real-time throughput/latency graphs |
| `kafka_flow_bottlenecks` | Detect bottlenecks: high lag, low throughput, high error rates |

### Memory

| Tool | Purpose |
|---|---|
| `kafka_memory_store` | Store a memory entry for the current agent with TTL and tags |
| `kafka_memory_recall` | Recall a specific memory entry by key or list all entries |
| `kafka_memory_search` | Search agent memory using TF-IDF semantic search |

### Migration

| Tool | Purpose |
|---|---|
| `kafka_migrate_execute` | Execute a migration plan with dry-run and progress tracking |
| `kafka_migrate_status` | Get the status and progress of a migration execution |
| `kafka_migrate_rollback` | Rollback a completed or failed migration |

### OpenAPI

| Tool | Purpose |
|---|---|
| `kafka_openapi_spec` | Generate an OpenAPI 3.0 specification from registered MCP tools |

### Plugin

| Tool | Purpose |
|---|---|
| `kafka_plugin_search` | Search the plugin marketplace for extensions |
| `kafka_plugin_install` | Install a plugin from the marketplace |
| `kafka_plugin_list` | List installed plugins with status and version |

### Quota

| Tool | Purpose |
|---|---|
| `kafka_quota_status` | Get adaptive rate limit status for an agent |
| `kafka_quota_request` | Request a quota adjustment: increase, decrease, reset, or burst |

### Replay

| Tool | Purpose |
|---|---|
| `kafka_replay_create` | Create a bookmark capturing message state for later replay |
| `kafka_replay_diff` | Compare message payloads between two bookmarks |
| `kafka_replay_simulate` | Simulate replaying messages through a consumer group |

### Replication

| Tool | Purpose |
|---|---|
| `kafka_replication_status` | Get real-time cross-cluster replication status |
| `kafka_replication_lag` | Get partition-level replication lag for a link |
| `kafka_failover_readiness` | Evaluate failover readiness with RPO/RTO estimates |

### Sandbox

| Tool | Purpose |
|---|---|
| `kafka_sandbox_create` | Create an isolated sandbox environment for safe experimentation |
| `kafka_sandbox_exec` | Execute a Kafka tool within a sandbox |
| `kafka_sandbox_destroy` | Destroy a sandbox or generate a promotion report |

### Schema Workflow

| Tool | Purpose |
|---|---|
| `kafka_schema_propose` | Propose a schema change with migration plan and compatibility validation |
| `kafka_schema_apply` | Apply, review, or rollback a schema proposal |

### StreamSQL

| Tool | Purpose |
|---|---|
| `kafka_sql_execute` | Execute a streaming SQL query against Kafka topics |
| `kafka_sql_explain` | Explain a streaming SQL query plan without executing it |
| `kafka_sql_manage` | Manage SQL queries: list, get, delete |

### Task Queue

| Tool | Purpose |
|---|---|
| `kafka_task_publish` | Publish a task to a priority queue for other agents |
| `kafka_task_claim` | Claim the highest-priority pending task from a queue |
| `kafka_task_complete` | Mark a claimed task as completed or failed |
| `kafka_task_status` | Get task status, queue statistics, or list tasks |

### TimeSeries

| Tool | Purpose |
|---|---|
| `kafka_timeseries_analyze` | Advanced time-series analysis: trend detection, seasonality, outliers |

### Tracing Flow

| Tool | Purpose |
|---|---|
| `kafka_trace_flow` | Distributed tracing for Kafka messages with W3C Trace Context |

### Usage

| Tool | Purpose |
|---|---|
| `kafka_usage_report` | Generate usage analytics and chargeback reports |

### View Store

| Tool | Purpose |
|---|---|
| `kafka_view_create` | Create a materialized view over a Kafka topic |
| `kafka_view_query` | Query a materialized view with point lookups or range scans |
| `kafka_view_status` | Manage materialized views: list, get, delete, stats |

### DAG Workflow

| Tool | Purpose |
|---|---|
| `kafka_workflow_create` | Create a reusable DAG-based workflow with conditional branching |
| `kafka_workflow_run` | Start a workflow execution from a template |
| `kafka_workflow_status` | Get workflow status, manage workflows, or advance a run |

### Pipeline

| Tool | Purpose |
|---|---|
| `kafka_pipeline_create` | Create a streaming function pipeline with filter/map/enrich steps |
| `kafka_pipeline_execute` | Execute a pipeline on messages from a source topic |
| `kafka_pipeline_manage` | Manage streaming function pipelines |

### Semantic Search

| Tool | Purpose |
|---|---|
| `kafka_semantic_index` | Index Kafka messages for semantic search |
| `kafka_semantic_search` | Search indexed messages by meaning using semantic similarity |
| `kafka_rag_context` | Build RAG context from indexed Kafka messages |
| `kafka_semantic_stats` | Get statistics about the semantic search index |

### Onboarding

| Tool | Purpose |
|---|---|
| `kafka_quickstart` | Guided onboarding: auto-discovers clusters and generates getting started guide |
| `kafka_discover_clusters` | Auto-discover Kafka clusters by scanning common ports and DNS |
| `kafka_suggest_rbac` | Generate recommended RBAC configuration based on topology |

### OPA Policy

| Tool | Purpose |
|---|---|
| `kafka_policy_evaluate` | Evaluate an OPA Rego policy for an authorization decision |
| `kafka_policy_simulate` | Simulate a policy evaluation showing rule matching |
| `kafka_policy_manage` | Manage OPA Rego policies: list, register, delete, export |

### Auto-Heal

| Tool | Purpose |
|---|---|
| `kafka_group_heal` | Auto-heal consumer groups: diagnose root cause and generate remediation |

### Collaboration

| Tool | Purpose |
|---|---|
| `agent_lock_acquire` | Acquire a distributed lock on a shared resource |
| `agent_lock_release` | Release a previously acquired distributed lock |
| `agent_lock_list` | List all active distributed locks |
| `agent_clock_get` | Get the vector clock for a resource |
| `agent_clock_increment` | Increment the vector clock for a resource |
| `agent_message_send` | Send a message to another agent for coordination |
| `agent_message_inbox` | Read messages sent to this agent |
| `agent_coordinate_register` | Register agent capabilities for coordination |
| `agent_coordinate_list` | List all registered agents with status and capabilities |

### Gateway

| Tool | Purpose |
|---|---|
| `gateway_list_backends` | List registered multi-protocol streaming backends |
| `gateway_produce` | Produce a message through a named streaming backend |
| `gateway_consume` | Consume messages from a named streaming backend |
| `gateway_list_topics` | List topics/queues/subjects on a backend |
| `gateway_create_topic` | Create a topic/queue/subject on a backend |

### Kubernetes Operator

| Tool | Purpose |
|---|---|
| `k8s_operator_manage` | Manage KafkaMCP Kubernetes operator instances |

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

## Event-Driven (P3)

### `kafka_watch`

Register or manage event-driven watch subscriptions on topic patterns. When matching messages arrive, notifications are pushed via webhooks or MCP sampling.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | No | `subscribe` | Action: `subscribe`, `unsubscribe`, `list`. |
| `topic_pattern` | string | Conditionally | — | Glob pattern to match topics (e.g., `orders.*`, `*-dlq`). Required for `subscribe`. |
| `key_filter` | string | No | — | Filter messages where key contains this string. |
| `value_filter` | string | No | — | Filter messages where value contains this string. |
| `webhook_url` | string | No | — | URL to POST notifications to. |
| `subscription_id` | string | Conditionally | — | Subscription ID. Required for `unsubscribe`. |

**Example request**

```json
{
  "name": "kafka_watch",
  "arguments": {
    "action": "subscribe",
    "topic_pattern": "orders.*",
    "value_filter": "failed",
    "webhook_url": "https://hooks.example.com/alerts"
  }
}
```

### `kafka_remediate_dlq`

Auto-remediate DLQ messages: retry with backoff, route to correction topic, or generate fix suggestions using schema context.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | DLQ topic name. |
| `action` | string | No | `analyze` | Remediation action: `analyze`, `retry`, `route`, `suggest`. |
| `target_topic` | string | No | — | Target topic for routing remediation. |
| `limit` | number | No | `100` | Max messages to process. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_remediate_dlq",
  "arguments": {
    "topic": "payments.failed.dlq",
    "action": "suggest",
    "limit": 50,
    "cluster": "production"
  }
}
```

### `kafka_schema_diff`

Compute a field-by-field diff between two schema versions showing additions, removals, and changes.

:::tip
This tool requires `schema_registry.url` on the selected cluster configuration.
:::

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `subject` | string | Yes | — | Schema subject name. |
| `version_from` | string | Yes | — | Source version number. |
| `version_to` | string | No | `latest` | Target version number. |

**Example request**

```json
{
  "name": "kafka_schema_diff",
  "arguments": {
    "subject": "orders.created-value",
    "version_from": "1",
    "version_to": "3"
  }
}
```

## Connector & Operations (Advanced)

### `kafka_create_connector`

Create a new Kafka Connect connector with the specified configuration.

:::tip
This tool requires `connect_url` on the selected cluster configuration.
:::

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | string | Yes | — | Connector name. |
| `config` | object | Yes | — | Connector configuration map. |
| `cluster` | string | No | default cluster | Named cluster to target. |

**Example request**

```json
{
  "name": "kafka_create_connector",
  "arguments": {
    "name": "jdbc-orders-sink",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
      "topics": "orders.created",
      "connection.url": "jdbc:postgresql://db:5432/orders"
    },
    "cluster": "production"
  }
}
```

### `kafka_update_connector`

Update an existing Kafka Connect connector configuration.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | string | Yes | — | Connector name. |
| `config` | object | Yes | — | New connector configuration map. |
| `cluster` | string | No | default cluster | Named cluster to target. |

**Example request**

```json
{
  "name": "kafka_update_connector",
  "arguments": {
    "name": "jdbc-orders-sink",
    "config": {
      "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
      "topics": "orders.created,orders.updated"
    },
    "cluster": "production"
  }
}
```

### `kafka_delete_connector`

Delete a Kafka Connect connector.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | string | Yes | — | Connector name. |
| `cluster` | string | No | default cluster | Named cluster to target. |

**Example request**

```json
{
  "name": "kafka_delete_connector",
  "arguments": {
    "name": "jdbc-orders-sink",
    "cluster": "production"
  }
}
```

### `kafka_restart_connector`

Restart a Kafka Connect connector or specific tasks. Supports bulk restart of failed tasks.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | string | Yes | — | Connector name. |
| `task_id` | number | No | `-1` | Specific task ID to restart (`-1` for all). |
| `failed_only` | boolean | No | `false` | Only restart failed tasks. |
| `cluster` | string | No | default cluster | Named cluster to target. |

**Example request**

```json
{
  "name": "kafka_restart_connector",
  "arguments": {
    "name": "jdbc-orders-sink",
    "failed_only": true,
    "cluster": "production"
  }
}
```

### `kafka_connector_health`

Get health score and diagnostics for a Kafka Connect connector including task states, error rates, and recommendations.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | string | Yes | — | Connector name. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_connector_health",
  "arguments": {
    "name": "jdbc-orders-sink",
    "cluster": "production"
  }
}
```

### `kafka_time_travel`

Point-in-time topic queries: snapshot state at a timestamp, reconstruct compacted topic state, or diff state between two timestamps.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | Yes | — | Action: `snapshot`, `diff`. |
| `topic` | string | Yes | — | Topic name. |
| `timestamp` | string | Conditionally | — | Point-in-time timestamp (ISO 8601). Required for `snapshot`. |
| `timestamp_from` | string | No | — | Start timestamp for `diff` (ISO 8601). |
| `timestamp_to` | string | No | — | End timestamp for `diff` (ISO 8601). |
| `max_keys` | number | No | `10000` | Max unique keys to track. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_time_travel",
  "arguments": {
    "action": "snapshot",
    "topic": "orders.created",
    "timestamp": "2025-01-01T00:00:00Z",
    "cluster": "production"
  }
}
```

### `kafka_recommend`

Get intelligent recommendations for topic optimization: detect under-replicated partitions, idle topics, consumer lag trends, partition hotspots, retention tuning, and compression analysis.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | No | — | Specific topic to analyze. Analyzes all topics if empty. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_recommend",
  "arguments": {
    "topic": "orders.created",
    "cluster": "production"
  }
}
```

### `kafka_trace_message`

Trace a message across topics using correlation IDs. Auto-detects `correlation-id`, `trace-id`, `x-request-id` headers and builds a message flow graph.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `correlation_id` | string | Yes | — | Correlation ID to trace. |
| `header_name` | string | No | auto-detect | Header name containing correlation ID. |
| `topics` | object | No | all topics | List of topic names to search. |
| `max_scan` | number | No | `10000` | Max messages to scan per topic. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_trace_message",
  "arguments": {
    "correlation_id": "req-abc-123",
    "cluster": "production"
  }
}
```

### `kafka_list_acls`

List all Kafka ACLs with principal, resource, and operation breakdown. Includes security analysis for over-permissioned principals and wildcard exposure.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `principal` | string | No | — | Filter by principal (e.g., `User:alice`). |
| `resource_type` | string | No | — | Filter by resource type: `topic`, `group`, `cluster`, `transactional_id`. |
| `include_analysis` | boolean | No | `true` | Include security analysis. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_list_acls",
  "arguments": {
    "principal": "User:alice",
    "include_analysis": true,
    "cluster": "production"
  }
}
```

### `kafka_config_drift`

Detect configuration drift across clusters. Compare topic configs between clusters, check against golden templates, and generate remediation commands.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | No | `compare` | Action: `compare`, `audit`, `template`. |
| `topic` | string | No | — | Specific topic to compare. |
| `source_cluster` | string | No | — | Source cluster for comparison. |
| `target_cluster` | string | No | — | Target cluster for comparison. |
| `template` | object | No | — | Golden template configs for audit. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_config_drift",
  "arguments": {
    "action": "compare",
    "topic": "orders.created",
    "source_cluster": "production",
    "target_cluster": "staging"
  }
}
```

### `kafka_playground`

Sandboxed Kafka environment for safe experimentation. Create temporary topics, generate sample data, and auto-cleanup.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | Yes | — | Action: `create`, `produce_sample`, `list`, `cleanup`. |
| `name` | string | No | — | Playground topic name suffix (auto-prefixed with `_playground_`). |
| `partitions` | number | No | `3` | Partitions for playground topic. |
| `data_pattern` | string | No | `ecommerce` | Sample data pattern: `ecommerce`, `iot`, `user_activity`, `logs`. |
| `count` | number | No | `10` | Number of sample messages to produce. |
| `cluster` | string | No | default cluster | Named cluster to target. |

**Example request**

```json
{
  "name": "kafka_playground",
  "arguments": {
    "action": "create",
    "name": "test-orders",
    "data_pattern": "ecommerce",
    "cluster": "staging"
  }
}
```

## AI & Analytics (Feature)

### `kafka_anomalies`

Get AI-detected anomalies in consumer lag, throughput, and error rates. Uses z-score and IQR statistical models with configurable sensitivity.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `severity` | string | No | — | Filter by severity: `warning`, `critical`. |
| `metric` | string | No | — | Filter by metric type: `consumer_lag`, `throughput`, `error_rate`. |
| `limit` | number | No | `50` | Max alerts to return. |
| `since_hours` | number | No | `24` | Look back period in hours. |

**Example request**

```json
{
  "name": "kafka_anomalies",
  "arguments": {
    "severity": "critical",
    "metric": "consumer_lag",
    "limit": 10
  }
}
```

### `kafka_anomaly_ingest`

Manually ingest a data point for anomaly detection analysis.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `metric` | string | Yes | — | Metric type: `consumer_lag`, `throughput`, `error_rate`. |
| `source` | string | Yes | — | Source name (topic or consumer group). |
| `value` | number | Yes | — | Metric value. |

**Example request**

```json
{
  "name": "kafka_anomaly_ingest",
  "arguments": {
    "metric": "consumer_lag",
    "source": "orders-processor",
    "value": 15000
  }
}
```

### `kafka_nl_query`

Query Kafka with plain English. Parses natural language into consume + filter + aggregate pipelines.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `query` | string | Yes | — | Natural language query. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_nl_query",
  "arguments": {
    "query": "show me all failed orders in the last hour",
    "cluster": "production"
  }
}
```

### `kafka_cost_analyze`

Analyze storage and compute costs for a Kafka topic. Estimates monthly cost based on retention, partitions, throughput, and replication.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | Topic name. |
| `provider` | string | No | `aws-msk` | Cost model: `aws-msk`, `confluent-cloud`, `self-hosted`. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_cost_analyze",
  "arguments": {
    "topic": "orders.created",
    "provider": "aws-msk",
    "cluster": "production"
  }
}
```

### `kafka_cost_waste`

Detect cost waste across all topics: idle topics, over-replication, compression opportunities, excessive retention.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `provider` | string | No | `aws-msk` | Cost model: `aws-msk`, `confluent-cloud`, `self-hosted`. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_cost_waste",
  "arguments": {
    "provider": "confluent-cloud",
    "cluster": "production"
  }
}
```

### `kafka_cost_whatif`

Simulate configuration changes and see cost impact. What-if analysis for retention, compression, partitions, and replication changes.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | Topic name. |
| `new_retention_ms` | number | No | — | New retention period in ms. |
| `new_compression` | string | No | — | New compression type: `gzip`, `snappy`, `lz4`, `zstd`, `none`. |
| `new_partitions` | number | No | — | New partition count. |
| `new_replication_factor` | number | No | — | New replication factor. |
| `provider` | string | No | `aws-msk` | Cost model. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_cost_whatif",
  "arguments": {
    "topic": "orders.created",
    "new_retention_ms": 86400000,
    "new_compression": "zstd",
    "cluster": "production"
  }
}
```

### `kafka_workflows`

Manage pre-built workflow templates for common Kafka operations.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | Yes | — | Action: `list`, `describe`, `execute`. |
| `template` | string | Conditionally | — | Template name. Required for `describe`/`execute`. |
| `category` | string | No | — | Filter by category: `operations`, `planning`, `development`, `monitoring`, `discovery`, `quality`, `debugging`. |
| `params` | object | No | — | Parameters for template execution. |
| `topic` | string | No | — | Topic parameter for workflow. |
| `group_id` | string | No | — | Consumer group parameter for workflow. |
| `subject` | string | No | — | Schema subject parameter for workflow. |

**Example request**

```json
{
  "name": "kafka_workflows",
  "arguments": {
    "action": "list",
    "category": "operations"
  }
}
```

### `kafka_gitops_diff`

Compare desired Kafka configuration (declarative YAML) against live cluster state. Generates a diff and migration plan with dry-run support.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topics` | object | No | — | Array of desired topic specs: `[{name, partitions, replication_factor, configs}]`. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_gitops_diff",
  "arguments": {
    "topics": [
      {"name": "orders.created", "partitions": 12, "replication_factor": 3}
    ],
    "cluster": "production"
  }
}
```

### `kafka_knowledge_graph`

Query the embedded knowledge graph of topic relationships, consumer group topologies, and schema dependencies.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `query` | string | Yes | — | Graph query (e.g., `depends_on orders`, `impacts user-events`, `type consumer_group`). |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_knowledge_graph",
  "arguments": {
    "query": "depends_on orders.created",
    "cluster": "production"
  }
}
```

### `kafka_platform_info`

Get information about configured Kafka-compatible platforms (Apache Kafka, Redpanda, Confluent Cloud, MSK Serverless) and their capabilities.

*No parameters.*

**Example request**

```json
{
  "name": "kafka_platform_info",
  "arguments": {}
}
```

### `kafka_validate_contract`

Validate messages against data contracts, register contracts, or get compliance reports.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | Yes | — | Action: `validate`, `register`, `get`, `list`, `compliance`. |
| `topic` | string | No | — | Topic name. |
| `contract` | object | No | — | Contract definition for `register` action. |
| `message` | object | No | — | Message value to validate. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_validate_contract",
  "arguments": {
    "action": "list",
    "topic": "orders.created"
  }
}
```

### `kafka_diagnose_lag`

Deep diagnostic analysis of consumer group lag. Traces root cause: slow consumers, producer spikes, partition imbalance, or GC pressure.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `group_id` | string | Yes | — | Consumer group ID. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_diagnose_lag",
  "arguments": {
    "group_id": "orders-processor",
    "cluster": "production"
  }
}
```

### `kafka_migration_plan`

Plan and validate Kafka migrations between clusters.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | Yes | — | Action: `plan`, `validate`, `list`. |
| `source_cluster` | string | No | — | Source cluster name. |
| `target_cluster` | string | No | — | Target cluster name. |

**Example request**

```json
{
  "name": "kafka_migration_plan",
  "arguments": {
    "action": "plan",
    "source_cluster": "production",
    "target_cluster": "production-v2"
  }
}
```

### `kafka_trace_lineage`

Track data lineage through Kafka topics. Shows producer→topic→consumer relationships, PII annotations, and GDPR/CCPA compliance reports.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | Yes | — | Action: `trace`, `annotate_pii`, `compliance_report`, `detect_pii`. |
| `topic` | string | No | — | Topic name. |
| `framework` | string | No | — | Compliance framework: `GDPR`, `CCPA`. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_trace_lineage",
  "arguments": {
    "action": "compliance_report",
    "framework": "GDPR",
    "cluster": "production"
  }
}
```

### `kafka_group_health`

Evaluate consumer group health with scoring (0–100), detect zombies, rebalance storms, stuck partitions, and suggest auto-remediation.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `group_id` | string | Yes | — | Consumer group ID. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_group_health",
  "arguments": {
    "group_id": "orders-processor",
    "cluster": "production"
  }
}
```

### `kafka_persona`

Manage declarative agent personas.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | Yes | — | Action: `list`, `get`, `activate`. |
| `persona` | string | No | — | Persona name: `sre`, `data-engineer`, `compliance`, `developer`, `admin`. |

**Example request**

```json
{
  "name": "kafka_persona",
  "arguments": {
    "action": "activate",
    "persona": "sre"
  }
}
```

### `kafka_forecast`

ML-based capacity forecasting using linear regression and seasonal decomposition. Predicts throughput growth and storage needs.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `topic` | string | Yes | — | Topic name. |
| `metric` | string | No | `throughput` | Metric to forecast: `throughput`, `storage`, `lag`. |
| `horizon_days` | number | No | `7` | Forecast horizon in days. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_forecast",
  "arguments": {
    "topic": "orders.created",
    "metric": "storage",
    "horizon_days": 30,
    "cluster": "production"
  }
}
```

## Streaming

### `kafka_stream`

Manage streaming consumer sessions for real-time message delivery.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | No | `start` | Action: `start`, `poll`, `pause`, `resume`, `stop`, `list`. |
| `topic` | string | Conditionally | — | Topic name. Required for `start`. |
| `group_id` | string | No | auto-generated | Consumer group ID. |
| `session_id` | string | Conditionally | — | Session ID. Required for `poll`/`pause`/`resume`/`stop`. |
| `batch_size` | number | No | `10` | Messages per batch (`1`–`100`). |
| `max_msg_per_sec` | number | No | `1000` | Throttle: max messages/sec. |
| `max_messages` | number | No | `1000` | Max total messages before auto-stop. |
| `timeout_ms` | number | No | `30000` | Session timeout in ms (max `60000`). |
| `poll_timeout_ms` | number | No | `5000` | Poll wait time in ms. |
| `deserializer` | string | No | `auto` | Deserialization: `auto`, `json`, `string`, `bytes`. |
| `auto_commit` | boolean | No | `true` | Auto-commit offsets. |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_stream",
  "arguments": {
    "action": "start",
    "topic": "orders.created",
    "batch_size": 25,
    "max_messages": 500,
    "cluster": "production"
  }
}
```

### `kafka_agent_registry`

Multi-agent collaboration: register agents, claim partitions, share cursors, and handoff messages.

| Parameter | Type | Required | Default | Description |
|---|---|---:|---|---|
| `action` | string | Yes | — | Action: `register`, `heartbeat`, `claim`, `release`, `share_cursor`, `handoff`, `list_agents`. |
| `agent_name` | string | No | — | Agent name for registration. |
| `topic` | string | No | — | Topic for partition operations. |
| `partition` | number | No | — | Partition number to claim/release. |
| `offset` | number | No | — | Offset for cursor sharing. |
| `target_agent` | string | No | — | Target agent for handoff. |
| `message_data` | string | No | — | Message data for handoff (JSON string). |
| `cluster` | string | No | default cluster | Named cluster to query. |

**Example request**

```json
{
  "name": "kafka_agent_registry",
  "arguments": {
    "action": "register",
    "agent_name": "order-processor-agent",
    "cluster": "production"
  }
}
```

## AI & Analytics (Feature)

### `kafka_migrate_execute`

Execute a migration plan. Supports dry-run mode, parallel execution, and progress tracking.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `plan_id` | string | No |  |
| `dry_run` | boolean | No |  |

**Example request**

```json
{
  "name": "kafka_migrate_execute",
  "arguments": {
    "plan_id": "example",
    "dry_run": true
  }
}
```

### `kafka_migrate_status`

Get the status and progress of a migration execution.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `plan_id` | string | No |  |
| `view` | string | No |  |

**Example request**

```json
{
  "name": "kafka_migrate_status",
  "arguments": {
    "plan_id": "example",
    "view": "example"
  }
}
```

### `kafka_migrate_rollback`

Rollback a completed or failed migration.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `plan_id` | string | No |  |

**Example request**

```json
{
  "name": "kafka_migrate_rollback",
  "arguments": {
    "plan_id": "example"
  }
}
```

## Advisor

### `kafka_advisor_scan`

Scan topics for best-practice violations, anti-patterns, and improvement opportunities. Checks replication factors, partition counts, retention policies, naming conventions, orphaned topics, and topic sprawl.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `cluster` | string | No |  |
| `severity` | string | No |  |
| `category` | string | No |  |

**Example request**

```json
{
  "name": "kafka_advisor_scan",
  "arguments": {
    "cluster": "example",
    "severity": "example"
  }
}
```

### `kafka_advisor_report`

Generate a detailed advisory report with health scores, category breakdowns, and prioritized recommendations.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_advisor_report",
  "arguments": {
    "cluster": "example"
  }
}
```

## Agent Profiling

### `kafka_agent_profile`

Profile agent performance: latency distributions, tool call patterns, error rates, and efficiency scoring. Actions: profile (agent metrics), slow_queries (find slow calls), list_agents (all agents).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `agent_id` | string | No |  |
| `since_hours` | number | No |  |
| `threshold_ms` | number | No |  |

**Example request**

```json
{
  "name": "kafka_agent_profile",
  "arguments": {
    "action": "example",
    "agent_id": "example"
  }
}
```

### `kafka_agent_optimize`

Get optimization suggestions for an agent based on its call patterns. Detects redundant calls, suboptimal tool sequences, and suggests improvements.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `agent_id` | string | No |  |
| `since_hours` | number | No |  |

**Example request**

```json
{
  "name": "kafka_agent_optimize",
  "arguments": {
    "agent_id": "example",
    "since_hours": 10
  }
}
```

### `kafka_agent_report`

Generate a comprehensive agent efficiency report with trends, comparisons, and scoring.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `agent_id` | string | No |  |
| `since_hours` | number | No |  |

**Example request**

```json
{
  "name": "kafka_agent_report",
  "arguments": {
    "agent_id": "example",
    "since_hours": 10
  }
}
```

## Alerting

### `kafka_dashboard_create`

Auto-provision Kafka monitoring dashboards for Grafana, Datadog, or custom providers. Creates pre-configured panels for cluster, topic, or consumer group metrics.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `name` | string | No |  |
| `provider` | string | No |  |
| `type` | string | No |  |
| `target` | string | No |  |

**Example request**

```json
{
  "name": "kafka_dashboard_create",
  "arguments": {
    "name": "example",
    "provider": "example"
  }
}
```

### `kafka_alert_create`

Create an alerting rule for Kafka metrics. Supports consumer lag, throughput, error rate, and partition metrics with configurable thresholds and notification channels.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `name` | string | No |  |
| `metric` | string | No |  |
| `condition` | string | No |  |
| `threshold` | number | No |  |
| `severity` | string | No |  |
| `duration` | string | No |  |
| `topic` | string | No |  |
| `group_id` | string | No |  |

**Example request**

```json
{
  "name": "kafka_alert_create",
  "arguments": {
    "name": "example",
    "metric": "example"
  }
}
```

### `kafka_alert_list`

List all configured alerting rules.

**Example request**

```json
{
  "name": "kafka_alert_list",
  "arguments": {}
}
```

### `kafka_alert_delete`

Delete an alerting rule by ID.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `alert_id` | string | No |  |

**Example request**

```json
{
  "name": "kafka_alert_delete",
  "arguments": {
    "alert_id": "example"
  }
}
```

## Auto-Heal

### `kafka_group_heal`

Auto-heal consumer groups: diagnose root cause (stuck partition, slow consumer, GC pressure, rebalance storm), generate remediation playbook, and optionally apply fixes. Combines health scoring with rule-based diagnosis.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `group_id` | string | No |  |
| `auto_execute` | boolean | No |  |
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_group_heal",
  "arguments": {
    "group_id": "example",
    "auto_execute": true
  }
}
```

## Capacity

### `kafka_capacity_plan`

Generate a comprehensive capacity plan with ML-based forecasting, storage growth analysis, and scaling recommendations for all tracked topics.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `horizon_days` | number | No |  |
| `max_storage_gb` | number | No |  |
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_capacity_plan",
  "arguments": {
    "horizon_days": 10,
    "max_storage_gb": 10
  }
}
```

### `kafka_capacity_forecast`

Forecast capacity for a specific topic using exponential smoothing and linear regression. Predicts throughput, storage, and lag trends.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `topic` | string | No |  |
| `metric` | string | No |  |
| `horizon_days` | number | No |  |
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_capacity_forecast",
  "arguments": {
    "topic": "example",
    "metric": "example"
  }
}
```

## Catalog

### `kafka_catalog_search`

Search the data catalog for topics, schemas, connectors by name, description, tag, owner, freshness, or quality score.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `query` | string | No |  |
| `type` | string | No |  |
| `tags` | object | No |  |
| `owner` | string | No |  |
| `min_quality` | number | No |  |
| `freshness` | string | No |  |
| `limit` | number | No |  |

**Example request**

```json
{
  "name": "kafka_catalog_search",
  "arguments": {
    "query": "example",
    "type": "example"
  }
}
```

### `kafka_catalog_tag`

Add tags to a catalog entry, index a new entry, or set lineage information.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `entry_id` | string | No |  |
| `tags` | object | No |  |
| `name` | string | No |  |
| `type` | string | No |  |
| `description` | string | No |  |
| `owner` | string | No |  |
| `lineage` | object | No |  |

**Example request**

```json
{
  "name": "kafka_catalog_tag",
  "arguments": {
    "action": "example",
    "entry_id": "example"
  }
}
```

### `kafka_catalog_stats`

Get catalog statistics: entry counts by type, freshness distribution, quality scores, tag usage.

**Example request**

```json
{
  "name": "kafka_catalog_stats",
  "arguments": {}
}
```

## Chaos Engineering

### `kafka_chaos_inject`

Inject controlled chaos into a Kafka environment. Types: broker_outage, network_partition, slow_consumer, schema_corruption, rebalance_storm, producer_flood. Actions: inject (start experiment), scenario (run pre-built scenario), list_scenarios.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `type` | string | No |  |
| `scenario` | string | No |  |
| `cluster` | string | No |  |
| `topics` | object | No |  |
| `groups` | object | No |  |
| `parameters` | object | No |  |
| `max_duration_minutes` | number | No |  |
| `category` | string | No |  |

**Example request**

```json
{
  "name": "kafka_chaos_inject",
  "arguments": {
    "action": "example",
    "type": "example"
  }
}
```

### `kafka_chaos_status`

Get the status of chaos experiments. Provide experiment_id for a specific experiment, or omit for all experiments.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `experiment_id` | string | No |  |
| `status_filter` | string | No |  |

**Example request**

```json
{
  "name": "kafka_chaos_status",
  "arguments": {
    "experiment_id": "example",
    "status_filter": "example"
  }
}
```

### `kafka_chaos_stop`

Stop a running chaos experiment and trigger rollback.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `experiment_id` | string | No |  |

**Example request**

```json
{
  "name": "kafka_chaos_stop",
  "arguments": {
    "experiment_id": "example"
  }
}
```

## Collaboration

### `agent_lock_acquire`

Acquire a distributed lock on a shared resource (topic, consumer group, schema) to prevent concurrent modifications by multiple agents.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `resource` | string | No |  |
| `ttl_seconds` | number | No |  |

**Example request**

```json
{
  "name": "agent_lock_acquire",
  "arguments": {
    "resource": "example",
    "ttl_seconds": 10
  }
}
```

### `agent_lock_release`

Release a previously acquired distributed lock on a resource.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `resource` | string | No |  |

**Example request**

```json
{
  "name": "agent_lock_release",
  "arguments": {
    "resource": "example"
  }
}
```

### `agent_lock_list`

List all active distributed locks showing resource, owner, and expiration.

**Example request**

```json
{
  "name": "agent_lock_list",
  "arguments": {}
}
```

### `agent_clock_get`

Get the vector clock for a resource to check causal ordering of operations across agents.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `resource` | string | No |  |

**Example request**

```json
{
  "name": "agent_clock_get",
  "arguments": {
    "resource": "example"
  }
}
```

### `agent_clock_increment`

Increment the vector clock for a resource, recording an operation by this agent.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `resource` | string | No |  |

**Example request**

```json
{
  "name": "agent_clock_increment",
  "arguments": {
    "resource": "example"
  }
}
```

### `agent_message_send`

Send a message to another agent for coordination. Use '*' as recipient for broadcasts.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `to` | string | No |  |
| `type` | string | No |  |
| `subject` | string | No |  |
| `body` | string | No |  |

**Example request**

```json
{
  "name": "agent_message_send",
  "arguments": {
    "to": "example",
    "type": "example"
  }
}
```

### `agent_message_inbox`

Read messages sent to this agent. Supports filtering to unread-only.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `unread_only` | boolean | No |  |

**Example request**

```json
{
  "name": "agent_message_inbox",
  "arguments": {
    "unread_only": true
  }
}
```

### `agent_coordinate_register`

Register this agent's capabilities and status for coordination with other agents.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `status` | string | No |  |
| `capabilities` | object | No |  |

**Example request**

```json
{
  "name": "agent_coordinate_register",
  "arguments": {
    "status": "example",
    "capabilities": {}
  }
}
```

### `agent_coordinate_list`

List all registered agents with their status, capabilities, and last seen time.

**Example request**

```json
{
  "name": "agent_coordinate_list",
  "arguments": {}
}
```

## Compliance

### `kafka_compliance_scan`

Scan topics for compliance with GDPR, HIPAA, SOC2, or PCI-DSS. Detects PII exposure, retention policy violations, and missing data masking.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `regulation` | string | No |  |
| `cluster` | string | No |  |
| `topic` | string | No |  |

**Example request**

```json
{
  "name": "kafka_compliance_scan",
  "arguments": {
    "regulation": "example",
    "cluster": "example"
  }
}
```

### `kafka_compliance_report`

Generate a comprehensive compliance report with scores, violations, retention checks, and recommendations.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `regulation` | string | No |  |
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_compliance_report",
  "arguments": {
    "regulation": "example",
    "cluster": "example"
  }
}
```

### `kafka_compliance_violations`

List detected compliance violations with optional filters.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `regulation` | string | No |  |
| `severity` | string | No |  |

**Example request**

```json
{
  "name": "kafka_compliance_violations",
  "arguments": {
    "regulation": "example",
    "severity": "example"
  }
}
```

## Config VCS

### `kafka_config_history`

Get configuration change history for a resource with full diff details. Supports SOC2-ready audit reports.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `resource` | string | No |  |
| `resource_id` | string | No |  |
| `limit` | number | No |  |
| `audit_report` | boolean | No |  |

**Example request**

```json
{
  "name": "kafka_config_history",
  "arguments": {
    "resource": "example",
    "resource_id": "example"
  }
}
```

### `kafka_config_rollback`

Rollback a resource configuration to a previous version.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `resource` | string | No |  |
| `resource_id` | string | No |  |
| `target_version` | number | No |  |
| `reason` | string | No |  |

**Example request**

```json
{
  "name": "kafka_config_rollback",
  "arguments": {
    "resource": "example",
    "resource_id": "example"
  }
}
```

### `kafka_config_compare`

Compare two versions of a resource configuration showing a unified diff.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `resource` | string | No |  |
| `resource_id` | string | No |  |
| `version1` | number | No |  |
| `version2` | number | No |  |

**Example request**

```json
{
  "name": "kafka_config_compare",
  "arguments": {
    "resource": "example",
    "resource_id": "example"
  }
}
```

## Contracts

### `kafka_contract_violations`

View contract violations, enforce contracts on messages, set enforcement mode, or infer contracts from samples. Actions: violations (get violation summary), enforce (validate with enforcement), set_mode (set enforcement mode: warn/reject/quarantine), infer (generate contract from samples).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `topic` | string | No |  |
| `message` | object | No |  |
| `mode` | string | No |  |
| `samples` | object | No |  |

**Example request**

```json
{
  "name": "kafka_contract_violations",
  "arguments": {
    "action": "example",
    "topic": "example"
  }
}
```

## Cost Attribution

### `kafka_cost_live`

Get real-time cost metrics for a resource or all resources with per-hour/day/month breakdowns.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `resource_type` | string | No |  |
| `resource_name` | string | No |  |

**Example request**

```json
{
  "name": "kafka_cost_live",
  "arguments": {
    "resource_type": "example",
    "resource_name": "example"
  }
}
```

### `kafka_cost_allocate`

Create or view cost allocation groups that map resources to teams, services, or projects.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `name` | string | No |  |
| `owner` | string | No |  |
| `resources` | object | No |  |
| `tags` | object | No |  |

**Example request**

```json
{
  "name": "kafka_cost_allocate",
  "arguments": {
    "action": "example",
    "name": "example"
  }
}
```

### `kafka_cost_budget`

Create spending budgets with alert thresholds or view budget status.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `name` | string | No |  |
| `monthly_limit` | number | No |  |
| `resources` | object | No |  |
| `alert_thresholds` | object | No |  |

**Example request**

```json
{
  "name": "kafka_cost_budget",
  "arguments": {
    "action": "example",
    "name": "example"
  }
}
```

## DAG Workflow

### `kafka_workflow_create`

Create a reusable DAG-based workflow template for multi-step Kafka operations with conditional branching, parallel fan-out, and error recovery.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `name` | string | No |  |
| `description` | string | No |  |
| `steps` | object | No |  |
| `parameters` | object | No |  |
| `tags` | object | No |  |

**Example request**

```json
{
  "name": "kafka_workflow_create",
  "arguments": {
    "name": "example",
    "description": "example"
  }
}
```

### `kafka_workflow_run`

Start a workflow execution from a template. Returns the run ID and initial ready steps for the agent to execute.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `template_id` | string | No |  |
| `parameters` | object | No |  |

**Example request**

```json
{
  "name": "kafka_workflow_run",
  "arguments": {
    "template_id": "example",
    "parameters": {}
  }
}
```

### `kafka_workflow_status`

Get workflow status, manage workflows, or advance a running workflow. Actions: status, list, versions, cancel, complete_step, ready_steps.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `run_id` | string | No |  |
| `template_id` | string | No |  |
| `step_id` | string | No |  |
| `step_status` | string | No |  |
| `step_output` | object | No |  |
| `step_error` | string | No |  |

**Example request**

```json
{
  "name": "kafka_workflow_status",
  "arguments": {
    "action": "example",
    "run_id": "example"
  }
}
```

## Federation

### `kafka_federation_join`

Manage federation membership. Actions: join (register instance), leave (deregister), heartbeat (update status).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `instance_id` | string | No |  |
| `name` | string | No |  |
| `endpoint` | string | No |  |
| `capabilities` | object | No |  |
| `clusters` | object | No |  |

**Example request**

```json
{
  "name": "kafka_federation_join",
  "arguments": {
    "action": "example",
    "instance_id": "example"
  }
}
```

### `kafka_federation_query`

Execute a query across all federated instances. Routes tool calls to matching instances and aggregates results.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `tool` | string | No |  |
| `args` | object | No |  |
| `targets` | object | No |  |

**Example request**

```json
{
  "name": "kafka_federation_query",
  "arguments": {
    "tool": "example",
    "args": {}
  }
}
```

### `kafka_federation_status`

Get federation status: instances, policies, health. Actions: status (overview), instances (list), policies (list), audit (audit log).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `limit` | number | No |  |

**Example request**

```json
{
  "name": "kafka_federation_status",
  "arguments": {
    "action": "example",
    "limit": 10
  }
}
```

## Flow Debug

### `kafka_flow_debug`

Live message flow debugging. Builds a real-time graph of producers, topics, and consumer groups with throughput/latency metrics. Actions: graph (get current flow graph), snapshots (historical snapshots), config (get/set bottleneck thresholds), collect (trigger metric collection from cluster).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `cluster` | string | No |  |
| `since_minutes` | number | No |  |
| `limit` | number | No |  |
| `thresholds` | object | No |  |

**Example request**

```json
{
  "name": "kafka_flow_debug",
  "arguments": {
    "action": "example",
    "cluster": "example"
  }
}
```

### `kafka_flow_bottlenecks`

Detect bottlenecks in the message flow: high consumer lag, low throughput, high error rates, and high latency. Returns chart-ready JSON with severity levels.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `cluster` | string | No |  |
| `severity` | string | No |  |
| `type` | string | No |  |

**Example request**

```json
{
  "name": "kafka_flow_bottlenecks",
  "arguments": {
    "cluster": "example",
    "severity": "example"
  }
}
```

## Gateway

### `gateway_list_backends`

List all registered multi-protocol streaming backends (Pulsar, RabbitMQ, NATS) and their health status.

**Example request**

```json
{
  "name": "gateway_list_backends",
  "arguments": {}
}
```

### `gateway_produce`

Produce a message through a named streaming backend (Pulsar, RabbitMQ, NATS).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `backend` | string | No |  |
| `topic` | string | No |  |
| `key` | string | No |  |
| `value` | string | No |  |

**Example request**

```json
{
  "name": "gateway_produce",
  "arguments": {
    "backend": "example",
    "topic": "example"
  }
}
```

### `gateway_consume`

Consume messages from a named streaming backend (Pulsar, RabbitMQ, NATS).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `backend` | string | No |  |
| `topic` | string | No |  |
| `group` | string | No |  |
| `limit` | number | No |  |

**Example request**

```json
{
  "name": "gateway_consume",
  "arguments": {
    "backend": "example",
    "topic": "example"
  }
}
```

### `gateway_list_topics`

List topics/queues/subjects on a named streaming backend.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `backend` | string | No |  |

**Example request**

```json
{
  "name": "gateway_list_topics",
  "arguments": {
    "backend": "example"
  }
}
```

### `gateway_create_topic`

Create a topic/queue/subject on a named streaming backend.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `backend` | string | No |  |
| `topic` | string | No |  |
| `partitions` | number | No |  |

**Example request**

```json
{
  "name": "gateway_create_topic",
  "arguments": {
    "backend": "example",
    "topic": "example"
  }
}
```

## Kubernetes Operator

### `k8s_operator_manage`

Manage KafkaMCP Kubernetes operator instances. Actions: create, get, list, update, delete, scale, manifest, crd.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `name` | string | No |  |
| `namespace` | string | No |  |
| `replicas` | number | No |  |
| `image` | string | No |  |
| `version` | string | No |  |
| `transport` | string | No |  |
| `port` | number | No |  |

**Example request**

```json
{
  "name": "k8s_operator_manage",
  "arguments": {
    "action": "example",
    "name": "example"
  }
}
```

## Memory

### `kafka_memory_store`

Store a memory entry for the current agent. Supports categories (preference, schema, query_history, context, note), tags, TTL expiration, and arbitrary key-value data.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `key` | string | No |  |
| `value` | object | No |  |
| `category` | string | No |  |
| `tags` | object | No |  |
| `ttl_sec` | number | No |  |
| `metadata` | object | No |  |

**Example request**

```json
{
  "name": "kafka_memory_store",
  "arguments": {
    "key": "example",
    "value": {}
  }
}
```

### `kafka_memory_recall`

Recall a specific memory entry by key, or list all entries. Actions: get, list, delete.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `key` | string | No |  |
| `category` | string | No |  |

**Example request**

```json
{
  "name": "kafka_memory_recall",
  "arguments": {
    "action": "example",
    "key": "example"
  }
}
```

### `kafka_memory_search`

Search agent memory using TF-IDF semantic search. Returns ranked results by relevance.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `query` | string | No |  |
| `category` | string | No |  |
| `limit` | number | No |  |

**Example request**

```json
{
  "name": "kafka_memory_search",
  "arguments": {
    "query": "example",
    "category": "example"
  }
}
```

## OPA Policy

### `kafka_policy_evaluate`

Evaluate an OPA Rego policy for an authorization decision. Returns whether the action is allowed with reasoning.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `agent` | string | No |  |
| `action` | string | No |  |
| `resource` | string | No |  |
| `resource_name` | string | No |  |

**Example request**

```json
{
  "name": "kafka_policy_evaluate",
  "arguments": {
    "agent": "example",
    "action": "example"
  }
}
```

### `kafka_policy_simulate`

Simulate a policy evaluation showing how each policy rule matches, without recording the decision.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `agent` | string | No |  |
| `action` | string | No |  |
| `resource` | string | No |  |
| `resource_name` | string | No |  |

**Example request**

```json
{
  "name": "kafka_policy_simulate",
  "arguments": {
    "agent": "example",
    "action": "example"
  }
}
```

### `kafka_policy_manage`

Manage OPA Rego policies. Actions: list (show policies), register (add/update), delete (remove), export (JSON dump), decisions (recent log).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `policy_id` | string | No |  |
| `name` | string | No |  |
| `rego` | string | No |  |
| `limit` | number | No |  |

**Example request**

```json
{
  "name": "kafka_policy_manage",
  "arguments": {
    "action": "example",
    "policy_id": "example"
  }
}
```

## Onboarding

### `kafka_quickstart`

Guided onboarding: auto-discovers Kafka clusters, suggests configuration, and generates a step-by-step getting started guide.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |

**Example request**

```json
{
  "name": "kafka_quickstart",
  "arguments": {
    "action": "example"
  }
}
```

### `kafka_discover_clusters`

Auto-discover Kafka clusters by scanning common ports, environment variables, and DNS records.

**Example request**

```json
{
  "name": "kafka_discover_clusters",
  "arguments": {}
}
```

### `kafka_suggest_rbac`

Generate recommended RBAC configuration based on your Kafka topology. Outputs ready-to-use YAML config.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_suggest_rbac",
  "arguments": {
    "cluster": "example"
  }
}
```

## OpenAPI

### `kafka_openapi_spec`

Generate an OpenAPI 3.0 specification from all registered MCP tools. Returns JSON-formatted OpenAPI spec for SDK code generation.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `format` | string | No |  |

**Example request**

```json
{
  "name": "kafka_openapi_spec",
  "arguments": {
    "format": "example"
  }
}
```

## Pipeline

### `kafka_pipeline_create`

Create a streaming function pipeline with filter/map/enrich/transform steps. Pipelines process Kafka messages through a chain of micro-functions.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `name` | string | No |  |
| `source_topic` | string | No |  |
| `sink_topic` | string | No |  |
| `steps` | object | No |  |

**Example request**

```json
{
  "name": "kafka_pipeline_create",
  "arguments": {
    "name": "example",
    "source_topic": "example"
  }
}
```

### `kafka_pipeline_execute`

Execute a pipeline on messages from the source topic. Consumes, processes through the pipeline steps, and returns the results.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `pipeline_id` | string | No |  |
| `limit` | number | No |  |
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_pipeline_execute",
  "arguments": {
    "pipeline_id": "example",
    "limit": 10
  }
}
```

### `kafka_pipeline_manage`

Manage streaming function pipelines. Actions: list, get, delete, runs (execution history).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `pipeline_id` | string | No |  |

**Example request**

```json
{
  "name": "kafka_pipeline_manage",
  "arguments": {
    "action": "example",
    "pipeline_id": "example"
  }
}
```

## Plugin

### `kafka_plugin_search`

Search the plugin marketplace for extensions. Find monitoring, security, analytics, integration, and utility plugins with ratings and verification status.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `query` | string | No |  |
| `category` | string | No |  |
| `verified_only` | boolean | No |  |

**Example request**

```json
{
  "name": "kafka_plugin_search",
  "arguments": {
    "query": "example",
    "category": "example"
  }
}
```

### `kafka_plugin_install`

Install a plugin from the marketplace. Validates dependencies and security capabilities before installation.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `name` | string | No |  |

**Example request**

```json
{
  "name": "kafka_plugin_install",
  "arguments": {
    "name": "example"
  }
}
```

### `kafka_plugin_list`

List installed plugins with their status, version, provided tools, and resources.

**Example request**

```json
{
  "name": "kafka_plugin_list",
  "arguments": {}
}
```

## Quota

### `kafka_quota_status`

Get adaptive rate limit status for an agent or all agents. Shows current rate, dynamic quota, burst credits, utilization, and recommendations.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `agent_id` | string | No |  |

**Example request**

```json
{
  "name": "kafka_quota_status",
  "arguments": {
    "agent_id": "example"
  }
}
```

### `kafka_quota_request`

Request a quota adjustment: increase, decrease, reset, or grant burst credits.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `agent_id` | string | No |  |
| `amount` | number | No |  |
| `reason` | string | No |  |

**Example request**

```json
{
  "name": "kafka_quota_request",
  "arguments": {
    "action": "example",
    "agent_id": "example"
  }
}
```

## Replay

### `kafka_replay_create`

Create a bookmark capturing message state at the current point in time. Stores offsets, sample messages, and metadata for later comparison or replay.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `topic` | string | No |  |
| `name` | string | No |  |
| `cluster` | string | No |  |
| `tags` | object | No |  |
| `action` | string | No |  |
| `bookmark_id` | string | No |  |

**Example request**

```json
{
  "name": "kafka_replay_create",
  "arguments": {
    "topic": "example",
    "name": "example"
  }
}
```

### `kafka_replay_diff`

Compare message payloads between two bookmarks. Detects schema changes, content drift, and message volume differences.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `bookmark_a` | string | No |  |
| `bookmark_b` | string | No |  |

**Example request**

```json
{
  "name": "kafka_replay_diff",
  "arguments": {
    "bookmark_a": "example",
    "bookmark_b": "example"
  }
}
```

### `kafka_replay_simulate`

Simulate replaying messages through a consumer group. Predicts processing impact and estimates reprocessing time.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `topic` | string | No |  |
| `group_id` | string | No |  |
| `from_bookmark` | string | No |  |
| `simulation_id` | string | No |  |
| `name` | string | No |  |

**Example request**

```json
{
  "name": "kafka_replay_simulate",
  "arguments": {
    "action": "example",
    "topic": "example"
  }
}
```

## Replication

### `kafka_replication_status`

Get real-time cross-cluster replication status for MirrorMaker2/Confluent Replicator links. Shows active links, topic-level sync state, and recent alerts.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `source_cluster` | string | No |  |
| `target_cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_replication_status",
  "arguments": {
    "source_cluster": "example",
    "target_cluster": "example"
  }
}
```

### `kafka_replication_lag`

Get detailed partition-level replication lag for a specific replication link.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `source_cluster` | string | No |  |
| `target_cluster` | string | No |  |
| `topic` | string | No |  |

**Example request**

```json
{
  "name": "kafka_replication_lag",
  "arguments": {
    "source_cluster": "example",
    "target_cluster": "example"
  }
}
```

### `kafka_failover_readiness`

Evaluate failover readiness for a replication link. Provides readiness score (0-100), RPO/RTO estimates, and actionable recommendations.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `source_cluster` | string | No |  |
| `target_cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_failover_readiness",
  "arguments": {
    "source_cluster": "example",
    "target_cluster": "example"
  }
}
```

## Sandbox

### `kafka_sandbox_create`

Create an isolated sandbox environment for safe experimentation with Kafka operations. Sandboxes have resource quotas and auto-expire.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `name` | string | No |  |
| `description` | string | No |  |
| `duration_sec` | number | No |  |
| `quotas` | object | No |  |

**Example request**

```json
{
  "name": "kafka_sandbox_create",
  "arguments": {
    "name": "example",
    "description": "example"
  }
}
```

### `kafka_sandbox_exec`

Execute a Kafka tool within a sandbox. All operations are tracked with resource accounting.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `sandbox_id` | string | No |  |
| `tool` | string | No |  |
| `args` | object | No |  |

**Example request**

```json
{
  "name": "kafka_sandbox_exec",
  "arguments": {
    "sandbox_id": "example",
    "tool": "example"
  }
}
```

### `kafka_sandbox_destroy`

Destroy a sandbox, list sandboxes, get details, or generate a promotion report.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `sandbox_id` | string | No |  |

**Example request**

```json
{
  "name": "kafka_sandbox_destroy",
  "arguments": {
    "action": "example",
    "sandbox_id": "example"
  }
}
```

## Schema Workflow

### `kafka_schema_propose`

Propose a schema change with auto-generated migration plan, compatibility validation, and consumer impact analysis. Creates an approval workflow.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `subject` | string | No |  |
| `schema` | string | No |  |
| `schema_type` | string | No |  |
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_schema_propose",
  "arguments": {
    "subject": "example",
    "schema": "example"
  }
}
```

### `kafka_schema_apply`

Apply, review, or rollback a schema proposal. Actions: review (approve/reject), apply (register schema), rollback (revert), list (show proposals), get (proposal details), timeline (version history).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `proposal_id` | string | No |  |
| `review_action` | string | No |  |
| `note` | string | No |  |
| `status_filter` | string | No |  |
| `subject` | string | No |  |
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_schema_apply",
  "arguments": {
    "action": "example",
    "proposal_id": "example"
  }
}
```

## Semantic Search

### `kafka_semantic_index`

Index Kafka messages for semantic search. Consumes recent messages from a topic and adds them to the vector index for meaning-based retrieval.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `topic` | string | No |  |
| `limit` | number | No |  |
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_semantic_index",
  "arguments": {
    "topic": "example",
    "limit": 10
  }
}
```

### `kafka_semantic_search`

Search indexed Kafka messages by meaning using semantic similarity. Returns the most relevant messages matching the query intent.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `query` | string | No |  |
| `topic` | string | No |  |
| `limit` | number | No |  |

**Example request**

```json
{
  "name": "kafka_semantic_search",
  "arguments": {
    "query": "example",
    "topic": "example"
  }
}
```

### `kafka_rag_context`

Build RAG (Retrieval-Augmented Generation) context from indexed Kafka messages. Returns relevant message fragments optimized for LLM context windows.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `query` | string | No |  |
| `topic` | string | No |  |
| `max_tokens` | number | No |  |

**Example request**

```json
{
  "name": "kafka_rag_context",
  "arguments": {
    "query": "example",
    "topic": "example"
  }
}
```

### `kafka_semantic_stats`

Get statistics about the semantic search index including document counts, topic distribution, and vocabulary size.

**Example request**

```json
{
  "name": "kafka_semantic_stats",
  "arguments": {}
}
```

## StreamSQL

### `kafka_sql_execute`

Execute a streaming SQL query against Kafka topics. Supports SELECT, WHERE, GROUP BY, aggregations (COUNT/SUM/AVG/MIN/MAX), and WINDOW (TUMBLING/SLIDING/SESSION) clauses.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `sql` | string | No |  |
| `limit` | number | No |  |
| `cluster` | string | No |  |

**Example request**

```json
{
  "name": "kafka_sql_execute",
  "arguments": {
    "sql": "example",
    "limit": 10
  }
}
```

### `kafka_sql_explain`

Explain a streaming SQL query plan without executing it. Shows execution steps and estimated cost.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `sql` | string | No |  |

**Example request**

```json
{
  "name": "kafka_sql_explain",
  "arguments": {
    "sql": "example"
  }
}
```

### `kafka_sql_manage`

Manage SQL queries. Actions: list, get, delete.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `query_id` | string | No |  |

**Example request**

```json
{
  "name": "kafka_sql_manage",
  "arguments": {
    "action": "example",
    "query_id": "example"
  }
}
```

## Task Queue

### `kafka_task_publish`

Publish a task to a priority queue for other agents to claim and execute.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `queue` | string | No |  |
| `type` | string | No |  |
| `priority` | number | No |  |
| `payload` | object | No |  |
| `max_retries` | number | No |  |
| `deadline_sec` | number | No |  |

**Example request**

```json
{
  "name": "kafka_task_publish",
  "arguments": {
    "queue": "example",
    "type": "example"
  }
}
```

### `kafka_task_claim`

Claim the highest-priority pending task from a queue.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `queue` | string | No |  |

**Example request**

```json
{
  "name": "kafka_task_claim",
  "arguments": {
    "queue": "example"
  }
}
```

### `kafka_task_complete`

Mark a claimed task as completed or failed.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `task_id` | string | No |  |
| `status` | string | No |  |
| `result` | object | No |  |
| `error` | string | No |  |

**Example request**

```json
{
  "name": "kafka_task_complete",
  "arguments": {
    "task_id": "example",
    "status": "example"
  }
}
```

### `kafka_task_status`

Get task status, queue statistics, or list tasks. Actions: get, stats, list, heartbeat.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `task_id` | string | No |  |
| `queue` | string | No |  |
| `filter_status` | string | No |  |

**Example request**

```json
{
  "name": "kafka_task_status",
  "arguments": {
    "action": "example",
    "task_id": "example"
  }
}
```

## TimeSeries

### `kafka_timeseries_analyze`

Advanced time-series analysis: Mann-Kendall trend detection, FFT seasonality analysis, outlier identification, and cross-metric correlation. Returns chart-ready JSON.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `metric` | string | No |  |
| `source` | string | No |  |
| `metric2` | string | No |  |
| `source2` | string | No |  |
| `since_hours` | number | No |  |
| `value` | number | No |  |

**Example request**

```json
{
  "name": "kafka_timeseries_analyze",
  "arguments": {
    "action": "example",
    "metric": "example"
  }
}
```

## Tracing Flow

### `kafka_trace_flow`

Distributed tracing for Kafka messages: inject W3C Trace Context headers, correlate across topic chains, and visualize message flow graphs. Actions: trace (get trace by ID), record (record a span), create_context (generate trace context), list (recent traces).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `trace_id` | string | No |  |
| `operation` | string | No |  |
| `topic` | string | No |  |
| `producer` | string | No |  |
| `consumer` | string | No |  |
| `parent_span_id` | string | No |  |
| `partition` | number | No |  |
| `offset` | number | No |  |
| `duration_ms` | number | No |  |
| `limit` | number | No |  |

**Example request**

```json
{
  "name": "kafka_trace_flow",
  "arguments": {
    "action": "example",
    "trace_id": "example"
  }
}
```

## Usage

### `kafka_usage_report`

Generate usage analytics and chargeback reports. Actions: rollup (usage summary), chargeback (cost attribution), export_csv (CSV export), export_json (JSON export).

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `tenant_id` | string | No |  |
| `period` | string | No |  |
| `days_back` | number | No |  |

**Example request**

```json
{
  "name": "kafka_usage_report",
  "arguments": {
    "action": "example",
    "tenant_id": "example"
  }
}
```

## View Store

### `kafka_view_create`

Create a materialized view over a Kafka topic with projections, aggregations (count/sum/avg/min/max), windowed computations, and GROUP BY support for instant analytical queries.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `name` | string | No |  |
| `source_topic` | string | No |  |
| `key_field` | string | No |  |
| `aggregation_type` | string | No |  |
| `window_type` | string | No |  |
| `window_size_ms` | number | No |  |
| `group_by_fields` | object | No |  |
| `value_fields` | object | No |  |
| `filter_expr` | string | No |  |
| `description` | string | No |  |
| `retention_ms` | number | No |  |

**Example request**

```json
{
  "name": "kafka_view_create",
  "arguments": {
    "name": "example",
    "source_topic": "example"
  }
}
```

### `kafka_view_query`

Query a materialized view with point lookups, range scans, or full scans. Returns instant results without re-scanning the source topic.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `view_id` | string | No |  |
| `key_exact` | string | No |  |
| `key_prefix` | string | No |  |
| `limit` | number | No |  |
| `offset` | number | No |  |
| `descending` | boolean | No |  |

**Example request**

```json
{
  "name": "kafka_view_query",
  "arguments": {
    "view_id": "example",
    "key_exact": "example"
  }
}
```

### `kafka_view_status`

Manage materialized views. Actions: list, get, delete, stats.

| Parameter | Type | Required | Description |
|---|---|---:|---|
| `action` | string | No |  |
| `view_id` | string | No |  |

**Example request**

```json
{
  "name": "kafka_view_status",
  "arguments": {
    "action": "example",
    "view_id": "example"
  }
}
```

## Supported timestamp formats

Tools that accept ISO 8601 timestamps (`kafka_consume`, `kafka_search`, `kafka_reset_offsets`, `kafka_aggregate`) recognize the following formats:

| Format | Example |
|---|---|
| RFC 3339 | `2025-01-15T08:30:00Z` |
| RFC 3339 Nano | `2025-01-15T08:30:00.123456789Z` |
| ISO 8601 with `Z` | `2025-01-15T08:30:00Z` |
| ISO 8601 without zone | `2025-01-15T08:30:00` |
| Date only | `2025-01-15` |

When using date-only format, the time defaults to `00:00:00 UTC`.

## Practical notes

- omit `cluster` to use the default cluster
- schema tools require `schema_registry.url`
- connector tools require `connect_url`
- topic list and resource reads may be filtered by your policy rules when `default_deny` is enabled
- every tool call is subject to audit logging and rate limiting

## Validation limits

KafkaMCP enforces the following runtime limits on tool parameters. Requests exceeding these limits are rejected with a validation error.

| Constant | Value | Applies to |
|---|---|---|
| `MaxConsumeLimit` | 1,000 messages | `kafka_consume`, `kafka_subscribe` — max messages per call |
| `MaxProduceMessages` | 100 messages | `kafka_produce` — max messages per call |
| `MaxConsumeTimeout` | 60,000 ms | `kafka_consume` — max timeout |
| `MaxSearchScan` | 100,000 messages | `kafka_search` — max messages to scan |
| `DefaultSearchScan` | 10,000 messages | `kafka_search` — default scan count when not specified |
| `MaxPartitions` | 10,000 | `kafka_create_topic` — max partition count |
| `MaxReplicationFactor` | 10 | `kafka_create_topic` — max replication factor |
| `MaxSchemaSize` | 1 MB (1,048,576 bytes) | `kafka_check_schema_compatibility` — max schema body size |
| `MaxDLQMessages` | 10,000 messages | `kafka_analyze_dlq` — max messages to analyze |
| `MaxJoinLimit` | 10,000 results | `kafka_join` — max result count |
| `maxInputLength` | 1,024 characters | All tools — max length for user-supplied string parameters |
