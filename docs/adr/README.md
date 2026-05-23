# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for KafkaMCP.

ADRs document significant architectural decisions made during the project's development. Each record describes the context, decision, and consequences of a particular choice.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-001](001-mcp-protocol.md) | Use Model Context Protocol (MCP) | Accepted |
| [ADR-002](002-multi-cluster-support.md) | Multi-Cluster Architecture | Accepted |
| [ADR-003](003-policy-engine.md) | YAML-Based Policy Engine | Accepted |
| [ADR-004](004-subsystem-grouping.md) | Subsystem Grouping Architecture | Accepted |
| [ADR-005](005-engine-pattern.md) | Shared Engine Store Pattern | Accepted |
| [ADR-006](006-persistent-engine-store.md) | Persistent Engine Store with Pluggable Backend | Accepted |
| [ADR-007](007-agent-identity-oidc.md) | Agent Identity & OIDC Verification | Accepted |
| [ADR-008](008-avro-wire-format-codec.md) | First-class Avro Wire-format Codec | Accepted |
| [ADR-009](009-mtls-identity-provider.md) | mTLS Identity Provider | Accepted |
| [ADR-010](010-protobuf-wire-codec.md) | First-class Protobuf Wire-format Codec | Accepted |

## Format

Each ADR follows the standard format:
- **Title** — Short descriptive name
- **Status** — Proposed, Accepted, Deprecated, Superseded
- **Context** — What is the issue being addressed?
- **Decision** — What was decided?
- **Consequences** — What are the trade-offs?

## References

- [ADR GitHub Organization](https://adr.github.io/)
- [Michael Nygard's article on ADRs](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)
