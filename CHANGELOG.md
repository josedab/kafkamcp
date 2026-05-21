# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Adaptive tool surface: register tools by **tier** (`essential`, `advanced`, `experimental`) and **pack** (`essential`, `ops`, `analytics`, `governance`, `streaming`, `experimental`, `all`). Configure via `server.tool_packs` in `kafkamcp.yaml`; defaults to `[essential]` so the advertised surface stays under the ~40–60 tool ceiling where LLM tool-routing degrades. Full catalog still ~317 tools — opt in to additional packs as needed.

### Changed
- `internal/server`: every `mcp.NewTool` registration now flows through the new `s.addTool` wrapper which consults the active `ToolPackFilter`. Experimental tools (`kafka_chaos_*`, `kafka_wasm_*`, `kafka_proxy`, `kafka_controlplane_*`, `kafka_migration_plan`, `kafka_autopilot_execute/rollback`, `kafka_plugin_install`) are now tagged with tier `experimental` and only register when the `experimental` (or `all`) pack is active.
- `docs/tool-reference.md` is grouped by tier; the generator (`cmd/gen-tool-docs`) now reads tier annotations and the `essentialPackTools` allow-list to render per-tier sections.

## [0.1.0] - 2026-05-03

### Added
- YAML configuration loader with environment variable expansion
- Multi-cluster Kafka client manager with TLS and SASL support
- Message decoding with format-aware deserialization (auto, JSON, string, bytes)
- Schema Registry client with LRU cache and field summary
- YAML-based policy engine with glob pattern matching for authorization
- Structured JSON audit logger with ring buffer
- MCP server with 251 tools and 18 resource handlers
- Token bucket rate limiter (per-agent and global)
- Prometheus metrics for all tool operations
- Topic management: create, alter, delete topics
- Consumer group offset reset (earliest, latest, timestamp)
- Message search with key, header, and value filters
- Dead Letter Queue (DLQ) analysis with error categorization
- P2 operations: windowed aggregation, topic graph, stream join
- Natural language query interface for topic discovery
- Schema evolution suggestions
- Kafka Connect client with connector list/describe
- Cobra CLI with stdio, SSE, and streamable-http transports
- `--log-level` flag to override config file log level
- Configurable max message size limit
- Dockerfile and GoReleaser configuration
- CI pipeline with test, lint, and cross-platform build
- Docusaurus documentation website
- Example configurations for common deployment scenarios
- Contributing guidelines, issue templates, PR template, and CODEOWNERS

### Security
- Default-deny for unconfigured agents in rate limiter
- Authorization checks on all tool and resource handlers
- Agent identity validation on tool calls
- Cluster name validation against configuration
- Input length validation for string parameters
- Dangerous topic config keys restriction (min.insync.replicas, etc.)
- Sensitive data redaction in audit logs and connector configs
- Bootstrap servers removed from cluster list resource
- Minimum TLS 1.3 for Kafka and Connect clients
- Cryptographic random subscriber group ID generation
- SASL PLAIN over non-TLS warning
- Schema Registry credential over non-HTTPS warning
- HTTP security headers middleware
- Consume timeout and max_scan capping
- Window bounds clamping in aggregate and join handlers
- Join limit parameter clamping
- Semaphore for concurrent expensive operations
- HTTP response body size limit (10MB) for Connect client
- Metrics endpoint bound to localhost

### Fixed
- HTTP Connect client hardened with URL encoding and timeouts
- Audit logger mutex held during file write to prevent interleaved entries
- Schema Registry safe fallback on JSON escape failure
- Offset list failure logging improved
- Schema error handling in field summary improved

[Unreleased]: https://github.com/kafkamcp/kafkamcp/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/kafkamcp/kafkamcp/releases/tag/v0.1.0
