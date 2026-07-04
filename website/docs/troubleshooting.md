---
sidebar_position: 11
title: Troubleshooting
---

# Troubleshooting

This page covers the most common KafkaMCP issues and the fastest way to fix them.

## Connection failures

### Symptom

KafkaMCP fails to start or tool calls fail with broker connection errors.

### Common causes

- wrong `bootstrap_servers`
- network path to brokers is blocked
- wrong `security_protocol`
- TLS CA file is missing or incorrect

### Fix

1. Validate the config:

```bash
kafkamcp validate --config /path/to/kafkamcp.yaml
```

2. Confirm the broker addresses are reachable from the KafkaMCP host or container.
3. Re-check `security_protocol`, SASL settings, and `ssl_ca_location`.
4. For Docker, make sure the broker address is correct for container networking.

## SASL authentication errors

### Symptom

KafkaMCP can reach the broker but authentication fails.

### Common causes

- `sasl_mechanism` does not match the cluster
- username or password is wrong
- environment variables did not expand as expected

### Fix

Use explicit environment-backed config values:

```yaml
sasl_mechanism: SCRAM-SHA-512
sasl_username: "${KAFKA_USERNAME}"
sasl_password: "${KAFKA_PASSWORD}"
```

Then verify the variables are present in the runtime environment.

:::warning
If `sasl_mechanism` is set, KafkaMCP requires both `sasl_username` and `sasl_password`.
:::

## Schema Registry unreachable

### Symptom

Schema tools such as `kafka_list_schemas` or `kafka_get_schema` fail.

### Common causes

- `schema_registry.url` is missing
- the URL is wrong or not reachable
- Schema Registry basic auth is wrong

### Fix

1. Confirm the cluster config includes:

```yaml
schema_registry:
  url: "http://schema-registry:8081"
```

2. If the registry requires auth, set:

```yaml
schema_registry:
  url: "https://registry.example.com"
  auth:
    username: "${SR_USERNAME}"
    password: "${SR_PASSWORD}"
```

3. Verify connectivity from the KafkaMCP runtime.

## Rate limiting errors

### Symptom

KafkaMCP returns:

```text
Rate limit exceeded. Please retry later.
```

### Cause

The current agent exceeded `requests_per_minute`.

### Fix

Raise the limit for the agent or reduce request volume.

```yaml
policies:
  agents:
    - id: incident-agent
      rate_limit:
        requests_per_minute: 240
```

Also check whether the agent is stuck in a loop or repeatedly polling large topics.

## Agent authentication required

### Symptom

KafkaMCP returns:

```text
Authentication required: valid X-Agent-ID header must be provided
```

### Cause

`policies.default_deny` is `true` and the request is missing a valid `X-Agent-ID` header, or the agent ID is not listed in `policies.agents`.

### Fix

For **SSE / streamable-http** transports, set the `X-Agent-ID` header on every HTTP request. For example:

```bash
curl -H "X-Agent-ID: my-agent" http://localhost:3100/...
```

Make sure the agent ID matches an entry in your `policies.agents` configuration. If you are testing locally and don't need auth, set `policies.default_deny: false`.

## Capacity / concurrency limit

### Symptom

KafkaMCP returns:

```text
timed out waiting for available capacity; too many concurrent expensive operations
```

### Cause

Too many expensive operations (consume, produce, describe) are running at the same time. The server limits concurrent expensive operations to protect the Kafka cluster.

### Fix

1. Reduce the number of parallel requests from your agents.
2. Increase the request context timeout so operations have more time to acquire a slot.
3. If the problem persists, check whether agents are stuck in retry loops that saturate concurrency.

## Schema Registry not configured

### Symptom

Schema tools return:

```text
Schema Registry not configured
```

### Cause

The cluster configuration does not include a `schema_registry` section. Schema Registry is optional — if you don't need schemas, this message is expected.

### Fix

Add the schema registry URL to the cluster config:

```yaml
clusters:
  - name: default
    bootstrap_servers: "localhost:9092"
    schema_registry:
      url: "http://localhost:8081"
```

If your registry requires authentication, add credentials:

```yaml
schema_registry:
  url: "https://registry.example.com"
  auth:
    username: "${SR_USERNAME}"
    password: "${SR_PASSWORD}"
```

## `unauthorized` errors

### Symptom

KafkaMCP returns an authorization error such as:

```text
Unauthorized: agent "reader-agent" lacks write permission on orders.created
```

### Common causes

- `default_deny: true` is enabled but the agent is missing from `policies.agents`
- the topic pattern does not match the requested topic
- the agent has `read` permission but needs `write`, `create`, `delete`, `describe`, or `reset`
- the caller is being identified as `anonymous`

### Fix

1. Check which agent ID the client actually sends.
2. Add or correct the policy rule.
3. Re-test with the exact topic name and permission.

Example:

```yaml
policies:
  default_deny: true
  agents:
    - id: reader-agent
      topics:
        - pattern: "orders.*"
          permissions: [read]
```

If the topic is `payments.created`, that rule will not match.

## Topic not found

### Symptom

A tool such as `kafka_describe_topic` or `kafka_consume` fails because the topic does not exist.

### Common causes

- misspelled topic name
- wrong cluster selected
- topic exists but the agent cannot see it under default-deny filtering

### Fix

1. Call `kafka_list_topics` first.
2. If you run multi-cluster, set the `cluster` parameter explicitly.
3. Check whether the topic is filtered by policy.

## Claude Desktop does not show KafkaMCP

### Symptom

Claude Desktop starts, but KafkaMCP tools are missing.

### Fix

- make sure `claude_desktop_config.json` is valid JSON
- use `stdio` transport for local Claude Desktop integrations
- use an absolute `command` path if `kafkamcp` is not on `PATH`
- fully restart Claude Desktop after editing the config

## Metrics endpoint does not respond

### Symptom

`/metrics` or `/healthz` returns nothing.

### Cause

KafkaMCP only starts the metrics server for `sse` and `streamable-http` transports.

### Fix

Use an HTTP transport and check `server.metrics_host` /
`server.metrics_port`.

If your main port is `3100`, metrics are on `3101`.

For Docker/Kubernetes, set `metrics_host: 0.0.0.0` and expose/protect port
`3101`. Probe `/livez` for process liveness and `/ready` for dependency
readiness.

## Docker-specific issues

### Symptom

KafkaMCP starts in Docker, but cannot reach Kafka.

### Fix

Use broker addresses that make sense from inside the container.

Examples:

- local Docker Desktop: `host.docker.internal:9092`
- Compose network: service name such as `kafka:9092`

## Still stuck?

When you need a clean baseline, use this sequence:

```bash
kafkamcp validate --config /path/to/kafkamcp.yaml
kafkamcp --config /path/to/kafkamcp.yaml
```

Then test one low-risk action first:

- list topics
- describe a topic
- read one schema

That narrows the problem quickly and keeps debugging deterministic.
