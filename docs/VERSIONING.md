# Versioning, API Contract, Deprecation & Support Policy

> **Last reviewed:** 2026-07
>
> This document defines KafkaMCP's versioning scheme, what is and is not covered
> by the stable API/tool/config contract, the deprecation window, migration
> policy, and the LTS/support lifecycle.

## Versioning scheme

KafkaMCP follows [Semantic Versioning 2.0.0](https://semver.org/):

```
MAJOR.MINOR.PATCH
```

| Component | Trigger |
| --- | --- |
| **MAJOR** | Breaking change to a stable tool, config key, or resource URI that requires an operator action to upgrade |
| **MINOR** | New stable tools, config keys, or resources; non-breaking changes; beta/experimental additions |
| **PATCH** | Bug fixes, security patches, and documentation updates; no interface changes |

Pre-1.0: while the version is `0.x`, minor versions may carry breaking changes;
they will be documented explicitly in CHANGELOG.md.

## Capability classification

Every capability has a declared classification that determines its contract:

| Classification | Contract | Pack |
| --- | --- | --- |
| **Stable** | Breaking changes require a MAJOR bump + deprecation window. API/tool/config names are stable. | `core-read` |
| **Beta** | May change in MINOR versions; breaking changes are documented in CHANGELOG. Migration notes are provided. | `ops-write`, `governance` |

KafkaMCP ships no experimental or simulated tool pack: the supported surface is
exactly `core-read` + `ops-write` + `governance` (29 tools). Config keys that
lack an explicit stability annotation are treated as unstable.

The current classification of every tool is visible in the generated
[`docs/tool-reference.md`](tool-reference.md), which groups tools by tier.

## Stable API contract

The following are covered by the stable contract (no breaking change without a
MAJOR version and deprecation window):

- **Tool names** listed in the `core-read` pack.
- **Required tool input parameters** — a new required parameter to a stable tool
  is a breaking change.
- **Config key names** in `kafkamcp.yaml` that are documented in
  [`docs/kafkamcp.schema.json`](kafkamcp.schema.json) with `"stability": "stable"`.
- **Resource URIs** (`kafka://cluster/metadata`, etc.) for resources listed as
  stable in the tool reference.
- **MCP tool output schemas** — removing or renaming a field in a stable tool's
  JSON output is a breaking change.

The following are **not** covered:

- Tool names in `ops-write` and `governance` packs.
- Internal Go package APIs (all packages under `internal/`).
- Config keys without an explicit `"stability": "stable"` annotation.
- Log output format, metric label names, or span attribute names.
- Database schema of the SQLite persistence backend.

## Deprecation window

For stable tools and config keys, the deprecation process is:

1. **Announce** — the item is marked `deprecated` in `docs/tool-reference.md`
   and CHANGELOG, with a clear migration path and timeline.
2. **Minimum window** — at least **one minor release** between the deprecation
   announcement and removal.
3. **Remove** — the item is removed in the next MAJOR release after the window.

Deprecated tools continue to work and return results normally during the
deprecation window; they are not silently degraded.

For beta capabilities, the minimum window is **one patch release** and is
documented in CHANGELOG.

## Migration policy

When a breaking change is required:

1. A CHANGELOG entry in the `Breaking Changes` section describes exactly what
   changed and the migration steps.
2. If both old and new interfaces can coexist, the old one is kept as a
   deprecated alias for the deprecation window.
3. Configuration-level breaks include a migration snippet showing the before/after
   YAML change.
4. The `kafkamcp doctor` command will surface deprecation warnings at startup
   when deprecated config keys or tool packs are detected.

## LTS and support policy

KafkaMCP does not currently have a formal LTS release. The supported version is
**latest stable**. Security patches are backported to the previous MINOR series
for 90 days after a new MINOR is published, then support ends for the older series.

Once a `v1.0` is declared:
- The `v1.x` series will receive security patches for at least 12 months after
  `v2.0` is published.
- Breaking changes will be deferred to MAJOR releases with a minimum 6-month
  deprecation window for stable APIs.

## Breaking-change process

A change is considered a "breaking change" if a consumer following the stable
contract must modify their configuration, tool call arguments, or output parsing
to continue operating correctly.

Process for landing a breaking change:

1. Open an issue labeled `breaking-change` describing the motivation and the
   proposed new interface.
2. Document the before/after in the issue and the deprecation plan.
3. Land the change in a MAJOR version bump (or, for beta, a MINOR bump).
4. Update `CHANGELOG.md`, `docs/tool-reference.md`, and the relevant config
   schema.
5. Tag the release and push release notes to GitHub Releases.

## Config schema stability annotations

The JSON Schema at `docs/kafkamcp.schema.json` uses a custom `"stability"`
annotation on each property:

```json
"server.transport": {
  "type": "string",
  "stability": "stable",
  "description": "..."
}
```

If a config key lacks the `"stability"` annotation it should be treated as
`"experimental"` until annotated.

## Compatibility snapshot

Before every release, the `make readiness` target generates a machine-checkable
snapshot of stable tool names and config keys. The snapshot lives at
`internal/server/testdata/` and is checked in CI to catch unintentional breaks.
