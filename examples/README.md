# KafkaMCP Examples

This directory contains example configurations, Docker Compose setups, and SDK integration examples for KafkaMCP.

## Examples Index

| Example | Type | Description | Prerequisites | Quick Start |
|---------|------|-------------|---------------|-------------|
| [`minimal.yaml`](minimal.yaml) | Config | Minimal single-broker configuration with no auth | Kafka cluster | `kafkamcp --config examples/minimal.yaml` |
| [`confluent-cloud.yaml`](confluent-cloud.yaml) | Config | Confluent Cloud connection with SASL/SSL auth | Confluent Cloud account | `kafkamcp --config examples/confluent-cloud.yaml` |
| [`multi-cluster.yaml`](multi-cluster.yaml) | Config | Multi-cluster setup with per-agent access control policies | Multiple Kafka clusters | `kafkamcp --config examples/multi-cluster.yaml` |
| [`masking.yaml`](masking.yaml) | Config | PII masking and field-level data redaction rules | Kafka cluster | `kafkamcp --config examples/masking.yaml` |
| [`docker-compose.yaml`](docker-compose.yaml) | Docker | Local Kafka + Schema Registry + KafkaMCP dev stack | Docker | `docker compose -f examples/docker-compose.yaml up -d` |
| [`docker-compose-kafkamcp.yaml`](docker-compose-kafkamcp.yaml) | Docker | Full-stack Kafka + KafkaMCP (containerised) | Docker | `docker compose -f examples/docker-compose-kafkamcp.yaml up -d` |
| [`go-sdk/`](go-sdk/) | SDK | Go SDK integration example | Go 1.25+ | See [`go-sdk/README.md`](go-sdk/README.md) |
| [`langchain-agent/`](langchain-agent/) | SDK | LangChain Python agent example | Python 3.8+ | See [`langchain-agent/README.md`](langchain-agent/README.md) |
| [`crewai-agent/`](crewai-agent/) | SDK | CrewAI multi-agent example | Python 3.8+ | See [`crewai-agent/README.md`](crewai-agent/README.md) |
| [`http-client/`](http-client/) | SDK | HTTP/SSE client example | Python 3.8+ | See [`http-client/README.md`](http-client/README.md) |

## Getting Started

1. **Quickest start** — Run `make quickstart` from the repo root to install deps, start Kafka, build, and run.
2. **Local dev** — Use `docker-compose.yaml` to spin up Kafka, then run KafkaMCP with `minimal.yaml`.
3. **Cloud** — Copy `confluent-cloud.yaml` and fill in your Confluent Cloud credentials.

See the [documentation site](https://kafkamcp.dev) for full guides.
