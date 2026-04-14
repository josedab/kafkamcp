---
sidebar_position: 1
slug: /
title: What is KafkaMCP?
---

# What is KafkaMCP?

KafkaMCP is an open-source **MCP (Model Context Protocol) server** that gives AI agents native, real-time access to Apache Kafka. It exposes Kafka topics, consumer groups, schemas, and cluster metadata as MCP tools and resources — so any MCP-compatible agent can discover, read, write, and reason about streaming data without custom integration code.

## The Problem

Today, AI agents are blind to Kafka. To access streaming data, teams must:

1. Write a Python/Go consumer that dumps data to a file or database
2. Build a REST API wrapper around the consumer
3. Write an MCP tool that calls the REST API
4. Agent finally gets stale, batch-mode access

This means 3+ layers of bespoke code, no schema awareness, no real-time streaming, and every team reinvents the stack independently.

## The Solution

KafkaMCP eliminates this gap with a single binary:

```
AI Agent ←→ MCP Protocol ←→ KafkaMCP ←→ Kafka + Schema Registry
```

Agents can now:

- **Discover** — List topics, browse schemas, inspect consumer groups
- **Read** — Consume messages with flexible positioning (offset, timestamp, latest)
- **Write** — Produce messages with delivery confirmation
- **Search** — Filter messages by key, header, value, or time range
- **Manage** — Create/alter/delete topics, reset offsets, analyze DLQs
- **Reason** — Aggregate data, join topics, check schema compatibility

## Key Features

| Feature | Description |
|---------|-------------|
| **22 MCP Tools** | From basic consume/produce to windowed joins and natural language queries |
| **10 MCP Resources** | Browsable cluster metadata, topic catalogs, schema registry, audit logs |
| **Schema-Aware** | Auto-discovers Avro/Protobuf/JSON Schema from Schema Registry |
| **Secure** | Per-agent access control, rate limiting, TLS, audit logging |
| **Multi-Cluster** | Connect to multiple Kafka clusters with per-cluster configuration |
| **Observable** | Prometheus metrics for every tool call |
| **Single Binary** | Pure Go — no JVM, no native dependencies, runs everywhere |

## Quick Install

```bash
go install github.com/kafkamcp/kafkamcp/cmd/kafkamcp@latest
```

Or with Docker:

```bash
docker run ghcr.io/kafkamcp/kafkamcp:latest
```

## Next Steps

- [**Getting Started**](./getting-started) — Connect to Kafka in 5 minutes
- [**Core Concepts**](./core-concepts) — Understand the mental model
- [**API Reference**](./api-reference/tools) — All 22 tools documented
