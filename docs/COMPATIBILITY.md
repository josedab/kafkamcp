# Compatibility Matrix

> **Last reviewed:** 2026-07
>
> **Scope:** KafkaMCP's own version compatibility, Go toolchain, MCP protocol,
> config schema, Kafka broker, and transport support.
>
> For Kafka *vendor/flavor* profiles (MSK, Confluent Cloud, Aiven, Redpanda)
> see [`docs/VENDOR_COMPAT.md`](VENDOR_COMPAT.md).
>
> For the stable API/tool/config contract see [`docs/API_STABILITY.md`](API_STABILITY.md).

---

## Reading this document

Each section uses a standard legend:

| Symbol | Meaning |
| --- | --- |
| ✓ | Tested and passing in the referenced harness |
| ✓† | Tested with noted limitations |
| ~ | Configuration guidance only; not CI-verified |
| — | Not verified; no claim made |
| ✗ | Known incompatibility |

---

## 1. KafkaMCP release history

| Release | Date | Stable tools | Breaking changes |
| --- | --- | --- | --- |
| 0.1.0 | 2026-05-03 | 15 (see [API_STABILITY.md §1](API_STABILITY.md#1-stable-tools-core-read-v010-baseline)) | Initial release — no prior stable surface |
| Unreleased (main) | — | 15 | See [CHANGELOG.md [Unreleased]](../CHANGELOG.md) |

> Breaking changes between pre-release commits on `main` are noted in
> CHANGELOG under `[Unreleased] ### Changed`. No stable surface has been broken
> since v0.1.0; all changes to date are additive or affect beta
> surfaces.

---

## 2. Go toolchain

KafkaMCP is built with the toolchain declared in `go.mod`. Builds with older
toolchains are not tested and may fail.

| Go version | Status | Notes |
| --- | --- | --- |
| 1.26.5 (declared in `go.mod`) | ✓ | CI matrix; required minimum for `go.mod toolchain` directive |
| 1.26.x other patch | ✓ | Same minor; patch differences are not breaking |
| 1.25.x | ~ | Not in CI; may work but not supported |
| < 1.25 | ✗ | Uses language features not available before 1.22; not supported |

The `go.mod` `toolchain` directive pins the minimum patch. Runs with the exact
pinned toolchain are the reference for reproducible builds. CGO is disabled
(`CGO_ENABLED=0`); the binary is fully static and cross-compiles to all
supported target triples.

---

## 3. MCP protocol

KafkaMCP implements the Model Context Protocol via
`github.com/mark3labs/mcp-go v0.50.0` (see `go.mod`).

| MCP SDK version | KafkaMCP compat | Notes |
| --- | --- | --- |
| `mcp-go v0.50.0` | ✓ | Declared dependency; used in CI |
| `mcp-go v0.4x` | — | Not tested |
| Earlier `mcp-go` | ✗ | API incompatible |

**Transport support:**

| Transport | Status | Suitable for |
| --- | --- | --- |
| `stdio` | ✓ | Local agents (Claude Desktop, dev tooling); read-only (`core-read` only) |
| `sse` | ✓ | Remote/multi-agent; required for `ops-write` |
| `streamable-http` | ✓ | Remote/multi-agent; required for `ops-write` |

`ops-write` is rejected on `stdio` at startup (see [OPERATIONS.md](OPERATIONS.md#ops-write-prerequisites)).

---

## 4. Config schema versions

The config schema is versioned implicitly by KafkaMCP's release version. There
is currently no explicit schema version field in `kafkamcp.yaml`.

| KafkaMCP version | Schema changes | Forward/backward compat |
| --- | --- | --- |
| 0.1.0 | Initial schema | — |
| Unreleased | Added `policies.approval.*`, `policies.identity.*`, `server.tool_packs` (tightened) | **Backward:** a 0.1.0 config with no `tool_packs` key loads successfully (defaults to `core-read`). **Forward:** a config using new fields will fail on 0.1.0 due to unknown keys being rejected by `additionalProperties: false`. |

### Migration notes for 0.1.0 → current

| Old config | New behavior | Action |
| --- | --- | --- |
| No `server.tool_packs` key | Defaults to `[core-read]` | No action |
| `tool_packs: [essential]` | Accepted with deprecation warning; maps to `core-read` | Change to `core-read` |
| `tool_packs: [ops]` | Accepted with deprecation warning; maps to `core-read` + `ops-write` | Change to `[core-read, ops-write]` and supply required env vars |
| `tool_packs: [analytics]` | **Rejected** at startup | Remove; analytics tools are no longer available |
| `tool_packs: [streaming]` | **Rejected** at startup | Remove |
| `tool_packs: [all]` | **Rejected** at startup | Enumerate the packs you need explicitly |
| `tool_packs: [ops-write]` + `transport: stdio` | **Rejected** at startup | Switch transport to `sse` or `streamable-http` |

---

## 5. Kafka broker compatibility

The following table reflects the state of the local integration harness. A row
is added only after a configuration has been exercised end-to-end.

| Platform | Broker version | core-read | ops-write | Schema Registry | Notes |
| --- | --- | --- | --- | --- | --- |
| **Apache Kafka** | `confluentinc/cp-kafka:7.6.0` (KRaft) | ✓ | ✓* | not exercised | Local contract suite passed 2026-07-26 |
| **Redpanda** | `redpandadata/redpanda:v24.1.3` | ✓ | ✓* | not exercised | Local contract suite passed 2026-07-26 |
| **Amazon MSK** | _profile only_ | ~ | ~ | ~ | Config profile; not CI-verified |
| **Confluent Cloud** | _profile only_ | ~ | ~ | ~ | Config profile; not CI-verified |
| **Aiven for Kafka** | _profile only_ | ~ | — | ~ | Config profile; not CI-verified |

`✓*` covers topic lifecycle/config, produce/consume, headers, metadata, and
consumer-group basics. Kafka Connect and live ACL mutation are not part of the
provider contract.

See [VENDOR_COMPAT.md](VENDOR_COMPAT.md) for configuration profiles and the
procedure for adding a verified row.

### Message payload formats

Supported consume deserializers are `auto`, `json`, `string`, and `bytes`.
`auto` attempts JSON and otherwise returns a string. Produce sends strings
unchanged and JSON-encodes structured values. KafkaMCP exposes Schema Registry
metadata and compatibility checks, but does not claim server-side Avro or
Protobuf wire decoding/encoding in the supported tool surface.

### Kafka protocol minimum version

KafkaMCP uses `github.com/twmb/franz-go` which negotiates the highest supported
API version with the broker at connect time. The practical minimum broker version
is Apache Kafka **2.6** (for KRaft-readiness paths) but the library will
degrade gracefully to earlier versions for the subset of APIs they support.
No explicit minimum has been tested; use the latest available patch release of
your Kafka flavor.

---

## 6. TLS and SASL compatibility

| Feature | Status | Notes |
| --- | --- | --- |
| TLS 1.2 (Kafka, Connect) | ✓ | Compatibility default; certificate verification remains required |
| TLS 1.3 (Kafka, Connect) | ✓ | Set `tls_min_version: "1.3"` / `connect_tls.min_version: "1.3"` |
| TLS 1.3 (MCP HTTPS server) | ✓ | Enforced minimum for inbound MCP clients |
| SASL PLAIN | ✓ | Warning emitted without TLS |
| SASL SCRAM-SHA-256 | ✓ | Tested via docker-dev |
| SASL SCRAM-SHA-512 | ✓ | Tested via docker-dev |
| SASL GSSAPI / Kerberos | ✗ | Not implemented |
| SASL IAM (MSK) | ✗ | Not implemented; use SCRAM for MSK |
| mTLS (client cert, Kafka) | ~ | Supported at the Kafka client level via `ssl_ca_location`; not separately verified |

---

## 7. MCP client compatibility

KafkaMCP has been exercised with the following MCP clients. This list reflects
documented usage; it is not exhaustive and does not imply certification.

| MCP client | Transport | Status | Notes |
| --- | --- | --- | --- |
| Claude Desktop | stdio | ✓ | Primary target; read-only (`core-read`) |
| LangChain SDK | caller-supplied MCP client | ✓ | Async `ClientSession` path and wrapper contracts are tested |
| CrewAI SDK | stdio | ✓ | Includes a synchronous `StdioMCPClient` adapter |
| TypeScript SDK | caller-supplied transport | ~ | Contract-tested; no bundled network transport |
| Go SDK | caller-supplied transport | ~ | Contract-tested; no bundled network transport |
| Custom MCP client | stdio / SSE / streamable-http | ~ | Any client implementing `mcp-go v0.50.0` protocol |

---

## 8. Upgrade and rollback

### Minor upgrade (0.x.y → 0.x.z)

No config changes required. The binary is replaced in-place; re-run
`kafkamcp doctor` to verify. A rollback redeploys the previous binary.

### Minor series upgrade (0.x → 0.(x+1))

Check CHANGELOG for any beta-surface breaking changes. The stable surface
(15 core-read tools and the config keys listed in [API_STABILITY.md §4](API_STABILITY.md#4-config-key-stability))
is not broken between minor series pre-1.0 except where explicitly noted.

### Future major upgrade (0.x → 1.0 or 1.x → 2.x)

Will include a migration guide in CHANGELOG and be announced with at least one
prior MINOR release's deprecation window. Every stable-surface break will
appear in the `Breaking Changes` section of CHANGELOG.

---

## 9. Dependency freshness

Key runtime dependencies as of 2026-07. These are illustrative; consult `go.mod`
and `go.sum` for the definitive pinned versions.

| Dependency | Version (go.mod) | Purpose |
| --- | --- | --- |
| `github.com/mark3labs/mcp-go` | v0.50.0 | MCP server framework |
| `github.com/twmb/franz-go` | see go.mod | Kafka client |
| `github.com/coreos/go-oidc/v3` | see go.mod | OIDC identity verification |
| `modernc.org/sqlite` | see go.mod | Pure-Go SQLite persistence backend |

Run `make deps` to check for outdated or vulnerable dependencies.
