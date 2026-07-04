---
sidebar_position: 2
title: Docker Compose
---

# Docker Compose

Use the example Compose stack in `examples/` when you want a full local environment with:

- Kafka
- Confluent Schema Registry
- KafkaMCP

## Files used by the example

- `examples/docker-compose.yaml`
- `examples/docker-compose-kafkamcp.yaml`

## Full `docker-compose.yaml`

This is the exact Compose file from the repository:

```yaml
services:
  kafka:
    image: bitnami/kafka:3.7
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT
    volumes:
      - kafka-data:/bitnami/kafka

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    depends_on:
      - kafka

  kafkamcp:
    build:
      context: ..
      dockerfile: Dockerfile
    ports:
      - "3100:3100"
    volumes:
      - ./docker-compose-kafkamcp.yaml:/etc/kafkamcp/kafkamcp.yaml
    depends_on:
      - kafka
      - schema-registry

volumes:
  kafka-data:
```

## Companion KafkaMCP config

The Compose stack mounts this file into the container:

```yaml
server:
  transport: sse
  host: 0.0.0.0
  port: 3100
  log_level: info

clusters:
  - name: local
    bootstrap_servers: "kafka:9092"
    security_protocol: PLAINTEXT
    schema_registry:
      url: "http://schema-registry:8081"
    default: true

schema_cache:
  max_entries: 10000
  ttl_seconds: 300

audit:
  enabled: true
  max_entries: 10000
```

## Start the stack

From the repository root:

```bash
cd examples
docker compose up --build
```

## What you get

- Kafka broker on `localhost:9092`
- Schema Registry on `http://localhost:8081`
- KafkaMCP SSE endpoint on `http://localhost:3100`
- Prometheus metrics on `http://localhost:3101/metrics`
- Process liveness on `http://localhost:3101/livez`
- Full readiness on `http://localhost:3101/ready`

:::info
This example explicitly binds `metrics_host: 0.0.0.0` so Docker can publish
port `3101`. Direct binary deployments default the metrics listener to
`127.0.0.1`.
:::

## Test the stack

In another terminal, verify KafkaMCP is up:

```bash
curl -s http://localhost:3101/ready
```

You should get:

```text
ready
```

## Connect an MCP client

Use the SSE endpoint:

```text
http://localhost:3100
```

That endpoint is useful for remote clients, browser-based tooling, and local integration tests.

## When to use this setup

Use the Compose example when you want:

- a reproducible local demo environment
- a quick way to test Schema Registry features
- an isolated stack for documentation, development, or demos

If you only need KafkaMCP for Claude Desktop, the simpler `stdio` setup in [Getting Started](../getting-started) is usually faster.
