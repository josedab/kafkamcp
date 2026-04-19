# ADR-003: YAML-Based Policy Engine

## Status

Accepted

## Context

AI agents interacting with Kafka need access control to prevent unauthorized operations — an agent designed for monitoring should not be able to delete topics or produce messages. The authorization system must be:

1. **Agent-scoped** — Different agents have different permissions.
2. **Resource-granular** — Permissions apply per topic pattern, consumer group, and schema.
3. **Simple to configure** — Operators should be able to set up policies without a separate auth service.
4. **Default-deny capable** — Production deployments should deny all access unless explicitly granted.

Options considered:

1. **External IAM integration** (e.g., OPA, Keycloak) — Full-featured but adds deployment complexity.
2. **YAML-based policy engine** — Policies defined in the KafkaMCP config file, evaluated in-process.
3. **Kafka ACLs only** — Reuse Kafka's built-in ACL system for agent authorization.

## Decision

We implement a **YAML-based policy engine** (`internal/auth/`) embedded in the KafkaMCP configuration. Policies are defined under the `policies` key in `kafkamcp.yaml`.

```yaml
policies:
  default_deny: true
  agents:
    - id: "monitoring-agent"
      role: reader
      topics:
        - pattern: "*"
          permissions: [read]
      consumer_groups:
        permissions: [describe]
    - id: "pipeline-agent"
      role: admin
      topics:
        - pattern: "*"
          permissions: [read, write, create, delete]
```

Key design choices:

- **Agent ID from headers** — `X-Agent-ID` or `X-Client-ID` HTTP headers identify the agent.
- **Glob patterns for topics** — `orders.*`, `*-dlq` enable flexible topic matching.
- **Action-based permissions** — `read`, `write`, `create`, `delete`, `describe`, `reset`.
- **Rate limiting per agent** — `rate_limit.requests_per_minute` prevents noisy-neighbor issues.
- **Auth-filtered results** — List operations (`kafka_list_topics`, `kafka_list_consumer_groups`) automatically filter results to show only authorized resources.

## Consequences

### Positive

- Zero external dependencies — no separate auth server to deploy or manage.
- Policies are version-controlled alongside the KafkaMCP config.
- The `withToolAuth` middleware centralizes auth checks, preventing missed authorization in new tools.
- Auth-filtered list results prevent information leakage.
- Rate limiting is co-located with authorization for unified agent governance.

### Negative

- YAML policies require a server restart to update (no hot-reload).
- No support for dynamic policy updates or external policy stores.
- Agent identity relies on header-based trust — in SSE/HTTP mode, agents can impersonate other agents without TLS mutual auth.

### Neutral

- Kafka's own ACLs remain the source of truth for broker-level security. KafkaMCP policies add an application-layer authorization gate that is independent of Kafka ACLs.
- Future ADR may address integration with external policy engines (OPA) for enterprise deployments.
