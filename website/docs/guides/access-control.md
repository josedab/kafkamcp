---
sidebar_position: 4
title: Access Control
---

# Access Control

KafkaMCP ships with a YAML policy engine for per-agent authorization.

It is simple on purpose:

- policies live in `kafkamcp.yaml`
- agent identity comes from MCP request headers
- topic rules use glob matching
- unknown agents are blocked when `default_deny` is enabled

## How the policy engine works

KafkaMCP resolves the agent ID in this order:

1. `X-Agent-ID`
2. `X-Client-ID`
3. `anonymous`

It then evaluates the current request against `policies.agents`.

### Default-deny

Set `default_deny: true` to block everything unless a matching rule explicitly allows it.

```yaml
policies:
  default_deny: true
  agents:
    - id: incident-agent
      role: reader
      topics:
        - pattern: "*.dlq"
          permissions: [read]
      consumer_groups:
        permissions: [describe]
      schemas:
        permissions: [read]
      rate_limit:
        requests_per_minute: 120
```

With `default_deny: true`:

- unknown agents are denied
- tools return `Unauthorized` for blocked actions
- topic listings are filtered to only authorized topics
- resource reads for topics, schemas, and groups are blocked unless allowed

:::warning
Turn on `default_deny` before exposing KafkaMCP to shared or untrusted agent environments.
:::

## Glob matching

Topic rules use glob-style patterns.

Common examples:

- `*` — every topic
- `orders.*` — topics that start with `orders.`
- `*.dlq` — dead-letter topics
- `payments-*` — payment topics with dash-separated names

KafkaMCP uses glob matching instead of regex so policies stay readable and predictable.

## Permission model

### Topics

Topic permissions are defined per pattern.

Supported topic permissions:

- `read`
- `write`
- `create`
- `delete`
- `admin`

`admin` on a topic pattern acts as a superset for topic actions.

### Consumer groups

Consumer group permissions are global to the agent.

Supported permissions:

- `describe`
- `reset`

### Schemas

Schema permissions are also global to the agent.

Supported permission used by current tools:

- `read`

## Example: reader-only agent

This agent can inspect topics and groups but cannot write or administer Kafka.

```yaml
policies:
  default_deny: true
  agents:
    - id: "reader-agent"
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
        requests_per_minute: 60
```

This agent can use:

- `kafka_list_topics`
- `kafka_describe_topic`
- `kafka_consume`
- `kafka_search`
- `kafka_describe_consumer_group`
- `kafka_list_schemas`
- `kafka_get_schema`

It cannot use:

- `kafka_produce`
- `kafka_create_topic`
- `kafka_delete_topic`
- `kafka_reset_offsets`

## Example: admin agent

This agent can operate broadly across the cluster.

```yaml
policies:
  default_deny: true
  agents:
    - id: "admin-agent"
      role: admin
      topics:
        - pattern: "*"
          permissions: [admin]
      consumer_groups:
        permissions: [describe, reset]
      schemas:
        permissions: [read]
      rate_limit:
        requests_per_minute: 600
```

Use this kind of policy only for trusted automation.

:::warning
Do not give broad topic admin permissions to experimental agents. Start read-only and expand deliberately.
:::

## Example: restrict by topic pattern

This policy limits an agent to payment topics and DLQs only.

```yaml
policies:
  default_deny: true
  agents:
    - id: "payments-agent"
      role: operator
      topics:
        - pattern: "payments.*"
          permissions: [read, write]
        - pattern: "payments-*.dlq"
          permissions: [read]
      consumer_groups:
        permissions: [describe]
      schemas:
        permissions: [read]
      rate_limit:
        requests_per_minute: 180
```

That agent cannot touch:

- `orders.*`
- `users.*`
- unrelated administrative topics

## Practical policy design

### Use narrow topic patterns first

Prefer this:

```yaml
- pattern: "orders.*"
  permissions: [read]
```

Over this:

```yaml
- pattern: "*"
  permissions: [read]
```

### Split readers and writers

Use one agent for inspection and another for write paths.

### Keep resets rare

Offset reset is powerful and operationally risky. Grant `reset` only to explicit recovery workflows.

### Pair access with rate limits

A permissive policy without a rate limit can still overload a cluster.

## Example: full policy block

```yaml
policies:
  default_deny: true
  agents:
    - id: "incident-agent"
      role: reader
      topics:
        - pattern: "*.dlq"
          permissions: [read]
        - pattern: "orders.*"
          permissions: [read]
      consumer_groups:
        permissions: [describe]
      schemas:
        permissions: [read]
      rate_limit:
        requests_per_minute: 120

    - id: "pipeline-agent"
      role: admin
      topics:
        - pattern: "*"
          permissions: [admin]
      consumer_groups:
        permissions: [describe, reset]
      schemas:
        permissions: [read]
      rate_limit:
        requests_per_minute: 600
```

## What happens when access is denied

KafkaMCP returns an explicit authorization error similar to:

```text
Unauthorized: agent "reader-agent" lacks write permission on orders.created
```

That makes policy failures easy to debug and safe to audit.
