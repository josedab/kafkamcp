---
sidebar_position: 1
title: "API Reference: Tools"
toc_max_heading_level: 3
---

KafkaMCP exposes 29 MCP tools across the supported production surface.

# KafkaMCP Tool Reference

> Auto-generated on 2026-07-30 from `internal/server/register_*.go`.
> Do not edit manually. Run `make generate` to regenerate.

## Summary

**29 tools** are available across the supported production surface.

The default `core-read` pack contains at most 15 read-only tools. The guarded `ops-write` and `governance` packs require secure network identity, default-deny authorization, and signed approval records. `ops-write` adds real mutations and optional idempotency keys; `governance` adds policy and decision controls. No simulated or placeholder tools are shipped.

| Stability | Count | Tool packs |
|---|---:|---|
| `stable` | 15 | `core-read` (default) |
| `beta` | 14 | `ops-write`, `governance` |

| Tool | Stability | Packs | Category | Description |
|---|---|---|---|---|
| [`kafka_acl_manage`](#kafka_acl_manage) | beta | governance, ops-write | governance | Inspect and reconcile live Kafka ACLs. Read-only actions (list, audit, diff, ... |
| [`kafka_alter_topic`](#kafka_alter_topic) | beta | ops-write | streaming | Modify configuration of an existing Kafka topic. |
| [`kafka_analyze_dlq`](#kafka_analyze_dlq) | stable | core-read | streaming | Analyze a dead-letter queue topic: consume recent messages, parse error heade... |
| [`kafka_approval`](#kafka_approval) | beta | governance, ops-write | governance | Manage identity-bound, short-lived approval grants for ops-write tools. Grant... |
| [`kafka_check_schema_compatibility`](#kafka_check_schema_compatibility) | stable | core-read | streaming | Check if a proposed schema is compatible with the subject's compatibility pol... |
| [`kafka_consume`](#kafka_consume) | stable | core-read | streaming | Read messages from a Kafka topic with flexible positioning. Supports earliest... |
| [`kafka_create_connector`](#kafka_create_connector) | beta | ops-write | streaming | Create a new Kafka Connect connector with the specified configuration. |
| [`kafka_create_topic`](#kafka_create_topic) | beta | ops-write | streaming | Create a new Kafka topic with specified partitions, replication factor, and c... |
| [`kafka_delete_connector`](#kafka_delete_connector) | beta | ops-write | streaming | Delete a Kafka Connect connector. |
| [`kafka_delete_topic`](#kafka_delete_topic) | beta | ops-write | streaming | Delete a Kafka topic. This is a destructive operation. |
| [`kafka_describe_consumer_group`](#kafka_describe_consumer_group) | stable | core-read | streaming | Get the state, members, partition assignments, and per-partition lag of a con... |
| [`kafka_describe_topic`](#kafka_describe_topic) | stable | core-read | streaming | Get detailed metadata for a specific topic including partition details, repli... |
| [`kafka_diagnose_lag`](#kafka_diagnose_lag) | stable | core-read | governance | Deep diagnostic analysis of consumer group lag. Traces root cause: slow consu... |
| [`kafka_discover_clusters`](#kafka_discover_clusters) | stable | core-read | ops | Auto-discover Kafka clusters by scanning common ports, environment variables,... |
| [`kafka_discover_tools`](#kafka_discover_tools) | stable | core-read | discovery | Discover the active provider, tool packs, and registered tools. Optionally fi... |
| [`kafka_get_schema`](#kafka_get_schema) | stable | core-read | streaming | Get the full schema definition for a subject. Returns parsed schema, field su... |
| [`kafka_governance_decision`](#kafka_governance_decision) | beta | governance | governance | Simulate mutation policy, preview blast radius, generate least-privilege guid... |
| [`kafka_health`](#kafka_health) | stable | core-read | observability | Run a read-only Kafka health check using live cluster metadata, topic discove... |
| [`kafka_list_consumer_groups`](#kafka_list_consumer_groups) | stable | core-read | streaming | List all consumer groups with summary state, member count, and total lag. |
| [`kafka_list_schemas`](#kafka_list_schemas) | stable | core-read | streaming | List all subjects in the Schema Registry with latest version, type, and compa... |
| [`kafka_list_topics`](#kafka_list_topics) | stable | core-read | streaming | List all Kafka topics in the cluster with metadata including partition count,... |
| [`kafka_metrics`](#kafka_metrics) | stable | core-read | observability | Return aggregated local KafkaMCP Prometheus metrics without exposing raw agen... |
| [`kafka_policy_evaluate`](#kafka_policy_evaluate) | beta | governance | governance | Evaluate an OPA Rego policy for an authorization decision. Returns whether th... |
| [`kafka_policy_simulate`](#kafka_policy_simulate) | beta | governance | governance | Simulate a policy evaluation showing how each policy rule matches, without re... |
| [`kafka_produce`](#kafka_produce) | beta | ops-write | streaming | Publish one or more messages to a Kafka topic. Returns delivery confirmation ... |
| [`kafka_reset_offsets`](#kafka_reset_offsets) | beta | ops-write | streaming | Reset consumer group offsets. Supports earliest, latest, or timestamp strateg... |
| [`kafka_restart_connector`](#kafka_restart_connector) | beta | ops-write | streaming | Restart a Kafka Connect connector or specific tasks. Supports bulk restart of... |
| [`kafka_search`](#kafka_search) | stable | core-read | streaming | Search/filter messages in a topic by key, header, value content, or time rang... |
| [`kafka_update_connector`](#kafka_update_connector) | beta | ops-write | streaming | Update an existing Kafka Connect connector configuration. |

---

## Stable (15 tools)

The read-only default surface. Stable names and schemas follow the published compatibility policy.

- [`kafka_analyze_dlq`](#kafka_analyze_dlq) — Analyze a dead-letter queue topic: consume recent messages, parse error headers, categorize failure types, and return a statistical summary with samples.
- [`kafka_check_schema_compatibility`](#kafka_check_schema_compatibility) — Check if a proposed schema is compatible with the subject's compatibility policy.
- [`kafka_consume`](#kafka_consume) — Read messages from a Kafka topic with flexible positioning. Supports earliest, latest, numeric offset, or ISO 8601 timestamp.
- [`kafka_describe_consumer_group`](#kafka_describe_consumer_group) — Get the state, members, partition assignments, and per-partition lag of a consumer group.
- [`kafka_describe_topic`](#kafka_describe_topic) — Get detailed metadata for a specific topic including partition details, replicas, ISR, offsets, and configuration.
- [`kafka_diagnose_lag`](#kafka_diagnose_lag) — Deep diagnostic analysis of consumer group lag. Traces root cause: slow consumers, producer spikes, partition imbalance, or GC pressure.
- [`kafka_discover_clusters`](#kafka_discover_clusters) — Auto-discover Kafka clusters by scanning common ports, environment variables, and DNS records.
- [`kafka_discover_tools`](#kafka_discover_tools) — Discover the active provider, tool packs, and registered tools. Optionally filter legacy capability metadata by category.
- [`kafka_get_schema`](#kafka_get_schema) — Get the full schema definition for a subject. Returns parsed schema, field summary, compatibility, and references.
- [`kafka_health`](#kafka_health) — Run a read-only Kafka health check using live cluster metadata, topic discovery, and consumer-group discovery.
- [`kafka_list_consumer_groups`](#kafka_list_consumer_groups) — List all consumer groups with summary state, member count, and total lag.
- [`kafka_list_schemas`](#kafka_list_schemas) — List all subjects in the Schema Registry with latest version, type, and compatibility level.
- [`kafka_list_topics`](#kafka_list_topics) — List all Kafka topics in the cluster with metadata including partition count, replication factor, and estimated message count.
- [`kafka_metrics`](#kafka_metrics) — Return aggregated local KafkaMCP Prometheus metrics without exposing raw agent, topic, group, or cluster labels.
- [`kafka_search`](#kafka_search) — Search/filter messages in a topic by key, header, value content, or time range. Bounded scan.

## Beta (14 tools)

Real functionality that must be opted into. Mutations require policy authorization and an approval grant.

- [`kafka_acl_manage`](#kafka_acl_manage) — Inspect and reconcile live Kafka ACLs. Read-only actions (list, audit, diff, dry_run) read live broker ACLs against a caller-supplied desired state. Reconciling actions (apply, reconcile) create missing and delete obsolete exact ACLs on the broker and require an approval grant. Stateless: no desired state is stored server-side.
- [`kafka_alter_topic`](#kafka_alter_topic) — Modify configuration of an existing Kafka topic.
- [`kafka_approval`](#kafka_approval) — Manage identity-bound, short-lived approval grants for ops-write tools. Grants are two-person by default, single-use, argument-scoped, persisted when SQLite is enabled, and recorded in a tamper-evident decision chain.
- [`kafka_create_connector`](#kafka_create_connector) — Create a new Kafka Connect connector with the specified configuration.
- [`kafka_create_topic`](#kafka_create_topic) — Create a new Kafka topic with specified partitions, replication factor, and configuration.
- [`kafka_delete_connector`](#kafka_delete_connector) — Delete a Kafka Connect connector.
- [`kafka_delete_topic`](#kafka_delete_topic) — Delete a Kafka topic. This is a destructive operation.
- [`kafka_governance_decision`](#kafka_governance_decision) — Simulate mutation policy, preview blast radius, generate least-privilege guidance, and verify/export tamper-evident approval decisions. Actions: simulate, verify, export.
- [`kafka_policy_evaluate`](#kafka_policy_evaluate) — Evaluate an OPA Rego policy for an authorization decision. Returns whether the action is allowed with reasoning.
- [`kafka_policy_simulate`](#kafka_policy_simulate) — Simulate a policy evaluation showing how each policy rule matches, without recording the decision.
- [`kafka_produce`](#kafka_produce) — Publish one or more messages to a Kafka topic. Returns delivery confirmation with partition, offset, and timestamp.
- [`kafka_reset_offsets`](#kafka_reset_offsets) — Reset consumer group offsets. Supports earliest, latest, or timestamp strategies with dry-run mode.
- [`kafka_restart_connector`](#kafka_restart_connector) — Restart a Kafka Connect connector or specific tasks. Supports bulk restart of failed tasks.
- [`kafka_update_connector`](#kafka_update_connector) — Update an existing Kafka Connect connector configuration.

## Tools

### `kafka_acl_manage`

Inspect and reconcile live Kafka ACLs. Read-only actions (list, audit, diff, dry_run) read live broker ACLs against a caller-supplied desired state. Reconciling actions (apply, reconcile) create missing and delete obsolete exact ACLs on the broker and require an approval grant. Stateless: no desired state is stored server-side.

**Source:** `register_aclmgmt.go`

**Stability:** `beta`

**Tool packs:** `governance`, `ops-write`

**Category:** `governance`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `action` | string | ✓ | Action: list, audit, diff, dry_run, apply, reconcile |
| `principal` | string |  | Principal filter or default principal for supplied rules (e.g. User:app1) |
| `resource_type` | string |  | Resource type filter for list/audit: topic, group, cluster, transactional_id |
| `cluster` | string |  | Cluster name for multi-cluster setups. |
| `rules` | object |  | Desired ACL rules array (for diff, dry_run, apply, reconcile). Each rule: &#123;principal, host, resource_type, resource_name, pattern_type, operation, permission&#125; |
| `approval_token` | string |  | Short-lived token issued by kafka_approval for this exact request. |
| `idempotency_key` | string |  | Optional retry key for safely suppressing a duplicate successful mutation. |

### `kafka_alter_topic`

Modify configuration of an existing Kafka topic.

**Source:** `register_p1.go`

**Stability:** `beta`

**Tool packs:** `ops-write`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✓ | Topic name |
| `cluster` | string |  | Cluster name. |
| `approval_token` | string |  | Short-lived token issued by kafka_approval for this exact request. |

### `kafka_analyze_dlq`

Analyze a dead-letter queue topic: consume recent messages, parse error headers, categorize failure types, and return a statistical summary with samples.

**Source:** `register_p1.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string | ✓ | DLQ topic name |
| `limit` | number |  | Max messages to analyze. Default: 100 |
| `cluster` | string |  | Cluster name. |

### `kafka_approval`

Manage identity-bound, short-lived approval grants for ops-write tools. Grants are two-person by default, single-use, argument-scoped, persisted when SQLite is enabled, and recorded in a tamper-evident decision chain.

**Source:** `register_approval.go`

**Stability:** `beta`

**Tool packs:** `governance`, `ops-write`

**Category:** `governance`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `action` | string | ✓ | request | approve | list | get | revoke | status |
| `tool` | string |  | Target write tool (for request), e.g. kafka_delete_topic |
| `request_reason` | string |  | Why the mutation is needed (for request) |
| `grant_id` | string |  | Grant identifier (for approve/get/revoke) |
| `ttl_seconds` | number |  | Requested grant lifetime in seconds (for approve); capped by config |
| `reason` | string |  | Reason (for revoke) |
| `agent_id` | string |  | Optional agent filter for list (admin only) |

### `kafka_check_schema_compatibility`

Check if a proposed schema is compatible with the subject's compatibility policy.

**Source:** `register_p0.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `subject` | string | ✓ | Schema subject name |
| `schema` | string | ✓ | The proposed schema definition as a JSON string |
| `schema_type` | string |  | Schema type: AVRO, PROTOBUF, JSON. Default: AVRO |
| `version` | string |  | Version to check against. Default: 'latest' |

### `kafka_consume`

Read messages from a Kafka topic with flexible positioning. Supports earliest, latest, numeric offset, or ISO 8601 timestamp.

**Source:** `register_p0.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string | ✓ | Topic name |
| `partition` | number |  | Specific partition (-1 for all). Default: all |
| `offset` | string |  | Start position: 'earliest', 'latest', numeric offset, or ISO 8601 timestamp. Default: 'latest' |
| `limit` | number |  | Maximum messages to return (1-1000). Default: 10 |
| `deserializer` | string |  | Deserialization format: auto, json, string, bytes. Auto tries JSON and falls back to string. |
| `timeout_ms` | number |  | Maximum wait time in ms. Default: 5000 |
| `cluster` | string |  | Cluster name. |

### `kafka_create_connector`

Create a new Kafka Connect connector with the specified configuration.

**Source:** `register_advanced.go`

**Stability:** `beta`

**Tool packs:** `ops-write`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✓ | Connector name |
| `config` | object | ✓ | Connector configuration map |
| `cluster` | string |  | Cluster name. |
| `approval_token` | string |  | Short-lived token issued by kafka_approval for this exact request. |
| `idempotency_key` | string |  | Optional retry key for safely suppressing a duplicate successful mutation. |

### `kafka_create_topic`

Create a new Kafka topic with specified partitions, replication factor, and configuration.

**Source:** `register_p1.go`

**Stability:** `beta`

**Tool packs:** `ops-write`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✓ | Topic name |
| `partitions` | number |  | Number of partitions. Default: 1 |
| `replication_factor` | number |  | Replication factor. Default: 1 |
| `cluster` | string |  | Cluster name. |
| `approval_token` | string |  | Short-lived token issued by kafka_approval for this exact request. |
| `idempotency_key` | string |  | Optional retry key for safely suppressing a duplicate successful mutation. |

### `kafka_delete_connector`

Delete a Kafka Connect connector.

**Source:** `register_advanced.go`

**Stability:** `beta`

**Tool packs:** `ops-write`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✓ | Connector name |
| `cluster` | string |  | Cluster name. |
| `approval_token` | string |  | Short-lived token issued by kafka_approval for this exact request. |
| `idempotency_key` | string |  | Optional retry key for safely suppressing a duplicate successful mutation. |

### `kafka_delete_topic`

Delete a Kafka topic. This is a destructive operation.

**Source:** `register_p1.go`

**Stability:** `beta`

**Tool packs:** `ops-write`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✓ | Topic name to delete |
| `cluster` | string |  | Cluster name. |
| `approval_token` | string |  | Short-lived token issued by kafka_approval for this exact request. |
| `idempotency_key` | string |  | Optional retry key for safely suppressing a duplicate successful mutation. |

### `kafka_describe_consumer_group`

Get the state, members, partition assignments, and per-partition lag of a consumer group.

**Source:** `register_p0.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `group_id` | string | ✓ | Consumer group ID |
| `cluster` | string |  | Cluster name. |

### `kafka_describe_topic`

Get detailed metadata for a specific topic including partition details, replicas, ISR, offsets, and configuration.

**Source:** `register_p0.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string | ✓ | Topic name |
| `cluster` | string |  | Cluster name for multi-cluster setups. |

### `kafka_diagnose_lag`

Deep diagnostic analysis of consumer group lag. Traces root cause: slow consumers, producer spikes, partition imbalance, or GC pressure.

**Source:** `register_features.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `governance`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `group_id` | string | ✓ | Consumer group ID |
| `cluster` | string |  | Cluster name. |

### `kafka_discover_clusters`

Auto-discover Kafka clusters by scanning common ports, environment variables, and DNS records.

**Source:** `register_onboarding.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `ops`

### `kafka_discover_tools`

Discover the active provider, tool packs, and registered tools. Optionally filter legacy capability metadata by category.

**Source:** `register_capability.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `discovery`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `category` | string |  | Optional tool category to discover |

### `kafka_get_schema`

Get the full schema definition for a subject. Returns parsed schema, field summary, compatibility, and references.

**Source:** `register_p0.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `subject` | string | ✓ | Schema subject name (e.g., 'orders.created-value') |
| `version` | string |  | Version: 'latest' or number. Default: 'latest' |

### `kafka_governance_decision`

Simulate mutation policy, preview blast radius, generate least-privilege guidance, and verify/export tamper-evident approval decisions. Actions: simulate, verify, export.

**Source:** `register_approval.go`

**Stability:** `beta`

**Tool packs:** `governance`

**Category:** `governance`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `action` | string | ✓ | simulate | verify | export |
| `tool` | string |  | Target write tool (for simulate) |
| `args` | object |  | Exact target-tool arguments (for simulate) |
| `grant_id` | string |  | Grant identifier (for verify) |
| `agent_id` | string |  | Optional agent filter (for export; admin only when different from caller) |

### `kafka_health`

Run a read-only Kafka health check using live cluster metadata, topic discovery, and consumer-group discovery.

**Source:** `register_core_observability.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `observability`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `cluster` | string |  | Cluster name. Defaults to the configured default cluster. |

### `kafka_list_consumer_groups`

List all consumer groups with summary state, member count, and total lag.

**Source:** `register_p0.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `state_filter` | string |  | Filter by state (Stable, Empty, Dead) |
| `cluster` | string |  | Cluster name. |

### `kafka_list_schemas`

List all subjects in the Schema Registry with latest version, type, and compatibility level.

**Source:** `register_p0.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `filter` | string |  | Glob pattern to filter subject names |

### `kafka_list_topics`

List all Kafka topics in the cluster with metadata including partition count, replication factor, and estimated message count.

**Source:** `register_p0.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `filter` | string |  | Glob pattern to filter topic names (e.g., 'orders.*', '*-dlq') |
| `include_internal` | boolean |  | Include internal topics (e.g., __consumer_offsets). Default: false |
| `cluster` | string |  | Cluster name for multi-cluster setups. |

### `kafka_metrics`

Return aggregated local KafkaMCP Prometheus metrics without exposing raw agent, topic, group, or cluster labels.

**Source:** `register_core_observability.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `observability`

### `kafka_policy_evaluate`

Evaluate an OPA Rego policy for an authorization decision. Returns whether the action is allowed with reasoning.

**Source:** `register_opa.go`

**Stability:** `beta`

**Tool packs:** `governance`

**Category:** `governance`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent` | string | ✓ | Agent ID to evaluate |
| `action` | string | ✓ | Action: read, write, create, delete, admin |
| `resource` | string | ✓ | Resource type: topic, consumer_group, schema, cluster |
| `resource_name` | string |  | Specific resource name. Default: * |

### `kafka_policy_simulate`

Simulate a policy evaluation showing how each policy rule matches, without recording the decision.

**Source:** `register_opa.go`

**Stability:** `beta`

**Tool packs:** `governance`

**Category:** `governance`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent` | string | ✓ | Agent ID to simulate |
| `action` | string | ✓ | Action to simulate |
| `resource` | string | ✓ | Resource type |
| `resource_name` | string |  | Specific resource name |

### `kafka_produce`

Publish one or more messages to a Kafka topic. Returns delivery confirmation with partition, offset, and timestamp.

**Source:** `register_p0.go`

**Stability:** `beta`

**Tool packs:** `ops-write`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string | ✓ | Topic name |
| `messages` | object | ✓ | Array of messages: [&#123;key, value, headers, partition&#125;]. Max 100. |
| `acks` | string |  | Acknowledgment level: 0, 1, all. Default: all |
| `cluster` | string |  | Cluster name. |
| `approval_token` | string |  | Short-lived token issued by kafka_approval for this exact request. |

### `kafka_reset_offsets`

Reset consumer group offsets. Supports earliest, latest, or timestamp strategies with dry-run mode.

**Source:** `register_p1.go`

**Stability:** `beta`

**Tool packs:** `ops-write`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `group_id` | string | ✓ | Consumer group ID |
| `topic` | string | ✓ | Topic name |
| `strategy` | string | ✓ | Reset strategy: earliest, latest, timestamp |
| `value` | string |  | Timestamp value for 'timestamp' strategy (ISO 8601) |
| `dry_run` | boolean |  | Preview without applying. Default: false |
| `cluster` | string |  | Cluster name. |
| `approval_token` | string |  | Short-lived token issued by kafka_approval for this exact request. |

### `kafka_restart_connector`

Restart a Kafka Connect connector or specific tasks. Supports bulk restart of failed tasks.

**Source:** `register_advanced.go`

**Stability:** `beta`

**Tool packs:** `ops-write`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✓ | Connector name |
| `task_id` | number |  | Specific task ID to restart (-1 for all). Default: -1 |
| `failed_only` | boolean |  | Only restart failed tasks. Default: false |
| `cluster` | string |  | Cluster name. |
| `approval_token` | string |  | Short-lived token issued by kafka_approval for this exact request. |

### `kafka_search`

Search/filter messages in a topic by key, header, value content, or time range. Bounded scan.

**Source:** `register_p1.go`

**Stability:** `stable`

**Tool packs:** `core-read`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `topic` | string | ✓ | Topic name |
| `key_filter` | string |  | Filter messages where key contains this string |
| `header_key` | string |  | Filter messages that have this header key |
| `header_value` | string |  | Filter messages where the header matches this value |
| `value_contains` | string |  | Filter messages where value contains this string |
| `start_time` | string |  | Start time (ISO 8601) for time range filter |
| `end_time` | string |  | End time (ISO 8601) for time range filter |
| `max_scan` | number |  | Max messages to scan. Default: 10000 |
| `limit` | number |  | Max matching messages to return. Default: 10 |
| `cluster` | string |  | Cluster name. |

### `kafka_update_connector`

Update an existing Kafka Connect connector configuration.

**Source:** `register_advanced.go`

**Stability:** `beta`

**Tool packs:** `ops-write`

**Category:** `streaming`

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `name` | string | ✓ | Connector name |
| `config` | object | ✓ | New connector configuration map |
| `cluster` | string |  | Cluster name. |
| `approval_token` | string |  | Short-lived token issued by kafka_approval for this exact request. |
| `idempotency_key` | string |  | Optional retry key for safely suppressing a duplicate successful mutation. |
