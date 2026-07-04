---
sidebar_position: 1
slug: /
title: What is KafkaMCP?
---

# What is KafkaMCP?

KafkaMCP is an open-source **MCP (Model Context Protocol) server** that gives
AI agents direct, bounded access to Apache Kafka operations. It exposes topics,
consumer groups, schemas, and cluster metadata as MCP tools and resources so
MCP-compatible agents can inspect and operate Kafka without bespoke wrappers.

It is designed as a **secure, vendor-neutral Kafka operations gateway**: read-only by default, with every mutating or destructive operation behind explicit policy and short-lived approval grants.

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://go.dev)
[![CI](https://github.com/josedab/kafkamcp/actions/workflows/ci.yml/badge.svg)](https://github.com/josedab/kafkamcp/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/josedab/kafkamcp/branch/main/graph/badge.svg)](https://codecov.io/gh/josedab/kafkamcp)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/josedab/kafkamcp/blob/main/LICENSE)

## Who is this for?

- **Platform engineers** who run Kafka and want to give AI agents safe, governed access
- **SREs** who want agents to diagnose consumer lag, DLQ issues, and cluster health autonomously
- **AI/ML engineers** building LLM agents that need to read from or write to streaming data
- **Data engineers** who want schema-aware, auditable Kafka access from agent workflows

## The Problem

Today, AI agents are blind to Kafka. To access streaming data, teams must:

1. Write a Python/Go consumer that dumps data to a file or database
2. Build a REST API wrapper around the consumer
3. Write an MCP tool that calls the REST API
4. Agent finally gets stale, batch-mode access

This means layers of bespoke code, no shared schema discovery or governance,
and every team reinvents the integration independently.

## The Solution

KafkaMCP eliminates this gap with a single binary:

```
AI Agent ←→ MCP Protocol ←→ KafkaMCP ←→ Kafka + Schema Registry
```

Agents can now:

- **Discover** — List topics, browse schemas, inspect consumer groups
- **Read** — Consume messages with flexible positioning (offset, timestamp, latest)
- **Write** — Produce messages with delivery confirmation (requires approval grant)
- **Search** — Filter messages by key, header, value, or time range
- **Diagnose** — Analyze DLQs, consumer lag, health, and message patterns
- **Manage** — Create/alter/delete topics and reset offsets with explicit approval
- **Govern** — Check schema compatibility, evaluate policy, and reconcile Kafka ACLs

## Key Features

| Feature | Description |
|---------|-------------|
| **Read-only default surface** | `core-read` pack (≤15 tools) is the default — safe for any agent on first connect |
| **Approval-gated writes** | Every mutating tool requires a short-lived, single-use approval grant |
| **9 stable MCP resources** | Browsable cluster metadata, topic catalogs, schemas, groups, and audit logs |
| **Schema-Aware** | Auto-discovers Avro/Protobuf/JSON Schema from Schema Registry |
| **Secure** | Per-agent access control, rate limiting, TLS 1.2+, audit logging |
| **Multi-Cluster** | Connect to multiple Kafka clusters with per-cluster configuration |
| **Observable** | Prometheus metrics and optional OpenTelemetry tracing |
| **Single Binary** | Pure Go — no JVM, no native dependencies, runs everywhere |
| **Vendor Neutral** | Apache Kafka, Redpanda; documented profiles for MSK and Confluent Cloud |

> **Tool packs:** The default `core-read` pack exposes ≤15 strictly read-only tools.
> The `ops-write` and `governance` packs add real, guarded mutation and policy
> capabilities and are opt-in via `server.tool_packs` in `kafkamcp.yaml`.
> KafkaMCP ships no simulated or placeholder tools.

## Quick Install

```bash
go install github.com/josedab/kafkamcp/cmd/kafkamcp@latest
```

Signed binaries and GHCR images will be published with the first tagged
release. Until then, install from source.

## Next Steps

- [**Getting Started**](./getting-started) — Connect to Kafka in 5 minutes
- [**Core Concepts**](./core-concepts) — Understand the mental model
- [**API Reference**](./api-reference/tools) — Full tool catalog
- [**Architecture**](./architecture) — How KafkaMCP works under the hood
