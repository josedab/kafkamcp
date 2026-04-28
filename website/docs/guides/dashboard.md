---
sidebar_position: 7
title: Dashboard Web UI
---

# Dashboard Web UI

KafkaMCP includes an embedded web dashboard that provides real-time visibility into agent activity, topic topology, audit trails, and more — accessible from any browser with no additional setup.

## Accessing the Dashboard

The dashboard is served automatically when KafkaMCP runs with `sse` or `streamable-http` transport. Open your browser at the same host and port configured for the MCP server:

```
http://localhost:3100/
```

The dashboard serves a single-page application from embedded static assets — no separate process or build step is required.

## Authentication

By default, the dashboard is accessible without authentication. To secure it, configure an API key in your `kafkamcp.yaml`:

```yaml
server:
  transport: sse
  host: 0.0.0.0
  port: 3100
  dashboard:
    api_key: "${DASHBOARD_API_KEY}"
```

When an API key is set, all dashboard API requests must include one of:

- **Bearer token**: `Authorization: Bearer <api_key>`
- **API key header**: `X-API-Key: <api_key>`

The browser UI will prompt for the key on first load.

## REST API Endpoints

The dashboard exposes a versioned REST API under `/api/`:

### V1 Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/v1/activity` | Current agent activity and connected sessions |
| `GET /api/v1/audit` | Structured audit trail of all agent interactions |
| `GET /api/v1/topics` | Topic list with partition and offset details |
| `GET /api/v1/topology` | Topic topology and data flow graph |
| `GET /api/v1/ratelimits` | Per-agent rate limit status and quotas |
| `GET /api/v1/schemas` | Schema Registry subjects and versions |
| `GET /api/v1/health` | Server health check |
| `GET /api/v1/events` | SSE stream for real-time updates |

### V2 Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/v2/messages/tail-stream` | SSE stream for live message tailing |

## Features

### Agent Activity Monitor

View all connected AI agents in real time — including which tools they are calling, request rates, and error counts. The activity view updates live via server-sent events.

### Audit Trail

Browse the full structured audit log of every tool call made by every agent. Filter by agent ID, tool name, time range, or result status.

### Topic Topology

Visualize your Kafka topics, partitions, and consumer group assignments as an interactive graph. Understand data flow at a glance.

### Rate Limit Status

Monitor per-agent rate limit consumption, remaining quota, and throttle events. Useful for tuning `requests_per_minute` in your access control policies.

### Schema Browser

Browse Schema Registry subjects and their versions. View Avro, Protobuf, and JSON Schema definitions with human-readable field summaries.

### Live Message Tailing (V2)

The V2 API adds real-time message tailing via SSE. Subscribe to a topic and watch messages arrive in your browser without polling.

## Example: Accessing the API

```bash
# Health check
curl http://localhost:3100/api/v1/health

# List topics (with auth)
curl -H "Authorization: Bearer $DASHBOARD_API_KEY" \
  http://localhost:3100/api/v1/topics

# Stream real-time events
curl -N http://localhost:3100/api/v1/events
```

## Docker Compose

When using the provided Docker Compose setup, the dashboard is accessible at `http://localhost:3100`:

```bash
docker compose -f examples/docker-compose.yaml up -d
open http://localhost:3100
```

## See Also

- [Monitoring](monitoring.md) — Prometheus metrics and Grafana dashboards
- [Configuration](../configuration.md) — Full server configuration reference
- [Docker Compose Guide](docker-compose.md) — Local development setup
