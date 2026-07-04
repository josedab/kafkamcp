---
sidebar_position: 2
title: Docker Deployment
---

# Docker Deployment

:::warning Pre-release
The GHCR image is not published yet. This page documents the image contract
that the first tagged release will publish. Build locally with
`docker build -t kafkamcp:dev .` in the meantime.
:::

Tagged releases publish container images to GHCR:

```text
ghcr.io/josedab/kafkamcp:VERSION
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
  ghcr.io/josedab/kafkamcp:VERSION
```

This assumes your config uses an HTTP transport such as `sse` or `streamable-http`.

## Example config for Docker

```yaml
server:
  transport: sse
  host: 0.0.0.0
  port: 3100
  metrics_host: 0.0.0.0
  metrics_port: 3101
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
  ghcr.io/josedab/kafkamcp:VERSION
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
- Liveness: `http://localhost:3101/livez`
- Kafka health: `http://localhost:3101/healthz`
- Readiness: `http://localhost:3101/ready`

## Runtime notes

- the image runs as a non-root `kafkamcp` user
- expose port `3100` for the MCP API and `3101` for metrics
- set `server.metrics_host: 0.0.0.0` only when port `3101` is protected by
  container-network/firewall rules
- mount config read-only whenever possible
- prefer environment variables over baking secrets into images

Docker deployment is a good fit when you want a portable runtime with simple bind mounts and clean promotion into Compose or Kubernetes.
