---
sidebar_position: 8
title: Configuration
---

# Configuration

KafkaMCP is configured with a single YAML file, usually named `kafkamcp.yaml`.

## Top-level structure

```yaml
server:
clusters:
policies:
schema_cache:
audit:
```

KafkaMCP validates the file at startup and applies defaults for omitted values where appropriate.

## `server`

Controls how KafkaMCP listens for MCP traffic.

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `transport` | string | No | `stdio` | MCP transport. Valid values: `stdio`, `sse`, `streamable-http`. |
| `host` | string | No | `0.0.0.0` | Bind host for HTTP transports. Ignored for `stdio`. |
| `port` | integer | No | `3100` | Bind port for HTTP transports. Metrics listen on `port + 1`. |
| `log_level` | string | No | `info` | Log level: `debug`, `info`, `warn`, `error`. |
| `max_message_size` | integer | No | `1048576` | Max MCP message size in bytes (1 MB). Used to validate produce message payloads. |
| `tls_cert` | string | No | — | Path to TLS certificate for HTTPS. |
| `tls_key` | string | No | — | Path to TLS private key for HTTPS. |

### Notes

- `tls_cert` and `tls_key` must both be set to enable TLS.
- `stdio` is the right choice for local MCP subprocess clients such as Claude Desktop.
- `sse` and `streamable-http` are the right choices for network-accessible deployments.

## `clusters`

Defines one or more Kafka clusters.

At least one cluster is required.

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `name` | string | Yes | — | Unique cluster name used by the `cluster` parameter and `kafka://clusters`. |
| `bootstrap_servers` | string | Yes | — | Comma-separated Kafka broker list. |
| `security_protocol` | string | No | empty | Kafka security mode such as `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`. |
| `sasl_mechanism` | string | No | empty | SASL mechanism such as `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. |
| `sasl_username` | string | Conditionally | empty | SASL username. Required when `sasl_mechanism` is set. |
| `sasl_password` | string | Conditionally | empty | SASL password. Required when `sasl_mechanism` is set. |
| `ssl_ca_location` | string | No | empty | Path to a CA bundle used for TLS broker validation. |
| `schema_registry` | object | No | — | Optional Schema Registry configuration for this cluster. |
| `connect_url` | string | No | empty | Optional Kafka Connect base URL for connector tools. |
| `connect_tls` | object | No | — | Optional TLS settings for the Kafka Connect REST API. |
| `default` | boolean | No | first cluster if none set | Marks the default cluster. |

### `clusters[].schema_registry`

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `url` | string | Yes | — | Base URL of the Schema Registry. |
| `auth` | object | No | — | Optional basic-auth credentials. |

### `clusters[].schema_registry.auth`

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `username` | string | No | empty | Schema Registry username. |
| `password` | string | No | empty | Schema Registry password. |

### `clusters[].connect_tls`

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `ca_file` | string | No | empty | Path to a CA certificate file for verifying the Connect REST API server. |
| `insecure_skip_verify` | boolean | No | `false` | Skip TLS certificate verification (not recommended for production). |

:::tip
If no cluster is marked `default: true`, KafkaMCP automatically uses the first cluster in the list as the default.
:::

## `policies`

Defines the per-agent authorization and rate limiting model.

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `default_deny` | boolean | No | `false` | Deny requests unless an explicit rule allows them. |
| `agents` | array | No | empty | List of per-agent policy blocks. |

### `policies.agents[]`

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `id` | string | Yes | — | Agent identifier matched against `X-Agent-ID`, then `X-Client-ID`, then `anonymous`. |
| `role` | string | No | empty | Optional human-readable role label. |
| `topics` | array | No | empty | Topic permission rules keyed by glob pattern. |
| `consumer_groups` | object | No | empty | Consumer group permissions for the agent. |
| `schemas` | object | No | empty | Schema permissions for the agent. |
| `rate_limit` | object | No | empty | Per-agent request budget. |

### `policies.agents[].topics[]`

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `pattern` | string | Yes | — | Glob pattern such as `orders.*`, `*.dlq`, or `*`. |
| `permissions` | array of strings | Yes | — | Topic permissions: `read`, `write`, `create`, `delete`, `admin`. |

### `policies.agents[].consumer_groups`

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `permissions` | array of strings | No | empty | Consumer group permissions: `describe`, `reset`. |

### `policies.agents[].schemas`

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `permissions` | array of strings | No | empty | Schema permissions. Current schema tools use `read`. |

### `policies.agents[].rate_limit`

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `requests_per_minute` | integer | No | `0` | Per-agent request cap. `0` means no explicit limit is installed. |

## `schema_cache`

Controls the in-memory Schema Registry cache.

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `max_entries` | integer | No | `10000` | Maximum cached schema entries. |
| `ttl_seconds` | integer | No | `300` | Cache TTL in seconds. |

## `audit`

Controls the audit trail.

| Field | Type | Required | Default | Description |
|---|---|---:|---|---|
| `enabled` | boolean | No | `false` | Enables the audit logger. |
| `max_entries` | integer | No | `10000` | Size of the in-memory ring buffer. |
| `log_file` | string | No | empty | Optional JSONL file path for durable audit logs. |

## Environment variable expansion

KafkaMCP expands environment variables before parsing the YAML file.

Supported forms:

- `${VAR}`
- `${VAR:-default}`

### Example

```yaml
clusters:
  - name: production
    bootstrap_servers: "${KAFKA_BOOTSTRAP_SERVERS}"
    sasl_username: "${KAFKA_USERNAME}"
    sasl_password: "${KAFKA_PASSWORD}"
    connect_url: "${CONNECT_URL:-http://connect:8083}"
```

Behavior:

- `${VAR}` fails if `VAR` is not set
- `${VAR:-default}` uses `default` when `VAR` is not set

:::warning
If you use `${VAR}` and the environment variable is missing, KafkaMCP refuses to start.
:::

## Complete annotated example

```yaml
server:
  transport: sse                 # stdio | sse | streamable-http
  host: 0.0.0.0                  # Bind host for HTTP transports
  port: 3100                     # Main MCP port; metrics will be 3101
  log_level: info                # debug | info | warn | error
  tls_cert: "${TLS_CERT:-}"     # Optional HTTPS certificate path
  tls_key: "${TLS_KEY:-}"       # Optional HTTPS private key path

clusters:
  - name: production
    bootstrap_servers: "${PROD_BOOTSTRAP_SERVERS}"
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: "${PROD_KAFKA_USERNAME}"
    sasl_password: "${PROD_KAFKA_PASSWORD}"
    ssl_ca_location: "${PROD_CA_FILE:-/etc/ssl/certs/ca-certificates.crt}"
    schema_registry:
      url: "${PROD_SCHEMA_REGISTRY_URL}"
      auth:
        username: "${PROD_SR_USERNAME}"
        password: "${PROD_SR_PASSWORD}"
    connect_url: "${PROD_CONNECT_URL:-http://connect:8083}"
    default: true

  - name: staging
    bootstrap_servers: "${STAGING_BOOTSTRAP_SERVERS:-staging-kafka:9092}"
    security_protocol: PLAINTEXT
    schema_registry:
      url: "${STAGING_SCHEMA_REGISTRY_URL:-http://staging-schema-registry:8081}"
    connect_url: "${STAGING_CONNECT_URL:-http://staging-connect:8083}"

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

    - id: pipeline-agent
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

schema_cache:
  max_entries: 10000
  ttl_seconds: 300

audit:
  enabled: true
  max_entries: 10000
  log_file: /var/log/kafkamcp/audit.jsonl
```

## Validate before startup

Always validate the file before deploying:

```bash
kafkamcp validate --config /path/to/kafkamcp.yaml
```

That catches:

- missing clusters
- duplicate cluster names
- invalid transports
- incomplete SASL settings

A clean config keeps KafkaMCP startup predictable and safe.
