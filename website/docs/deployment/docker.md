---
sidebar_position: 2
title: Docker Deployment
---

# Docker Deployment

KafkaMCP publishes container images to GHCR:

```text
ghcr.io/kafkamcp/kafkamcp:latest
```

## What the image expects

The container entrypoint is:

```text
/usr/local/bin/kafkamcp
```

The default command is:

```text
--config /etc/kafkamcp/kafkamcp.yaml
```

That means you normally mount a config file at `/etc/kafkamcp/kafkamcp.yaml`.

## Basic `docker run`

```bash
docker run --rm \
  -p 3100:3100 \
  -p 3101:3101 \
  -v $(pwd)/kafkamcp.yaml:/etc/kafkamcp/kafkamcp.yaml:ro \
  ghcr.io/kafkamcp/kafkamcp:latest
```

This assumes your config uses an HTTP transport such as `sse` or `streamable-http`.

## Example config for Docker

```yaml
server:
  transport: sse
  host: 0.0.0.0
  port: 3100
  log_level: info

clusters:
  - name: local
    bootstrap_servers: "host.docker.internal:9092"
    security_protocol: PLAINTEXT
    default: true

audit:
  enabled: true
  max_entries: 10000
```

:::warning
`stdio` is usually the wrong transport for detached container deployment. Use `sse` or `streamable-http` unless your container is launched directly by an MCP-aware runtime.
:::

## Pass secrets with environment variables

KafkaMCP supports `${VAR}` and `${VAR:-default}` expansion inside `kafkamcp.yaml`.

Example config:

```yaml
clusters:
  - name: production
    bootstrap_servers: "${KAFKA_BOOTSTRAP_SERVERS}"
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: "${KAFKA_USERNAME}"
    sasl_password: "${KAFKA_PASSWORD}"
```

Run it like this:

```bash
docker run --rm \
  -p 3100:3100 \
  -p 3101:3101 \
  -e KAFKA_BOOTSTRAP_SERVERS='broker-1:9092,broker-2:9092' \
  -e KAFKA_USERNAME='agent-user' \
  -e KAFKA_PASSWORD='secret-password' \
  -v $(pwd)/kafkamcp.yaml:/etc/kafkamcp/kafkamcp.yaml:ro \
  ghcr.io/kafkamcp/kafkamcp:latest
```

## Common volume mounts

### Config file

```text
-v $(pwd)/kafkamcp.yaml:/etc/kafkamcp/kafkamcp.yaml:ro
```

### TLS certificates

```text
-v $(pwd)/certs:/etc/kafkamcp/certs:ro
```

### Audit logs

```text
-v $(pwd)/logs:/var/log/kafkamcp
```

Use the mounted path in your config:

```yaml
audit:
  enabled: true
  max_entries: 10000
  log_file: /var/log/kafkamcp/audit.jsonl
```

## Health and metrics

For HTTP transports:

- MCP endpoint: `http://localhost:3100`
- Metrics: `http://localhost:3101/metrics`
- Health: `http://localhost:3101/healthz`

## Runtime notes

- the image runs as a non-root `kafkamcp` user
- expose port `3100` for the MCP API and `3101` for metrics
- mount config read-only whenever possible
- prefer environment variables over baking secrets into images

Docker deployment is a good fit when you want a portable runtime with simple bind mounts and clean promotion into Compose or Kubernetes.
