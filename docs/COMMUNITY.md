# Community & Contribution Guide (Narrowed Roadmap)

KafkaMCP has a deliberately **narrowed** roadmap: a secure, vendor-neutral Kafka
operations gateway for AI agents. New feature families are frozen. Contributions
that deepen the read-only core, the governance/approval wedge, vendor-neutral
compatibility, and truthfulness are the most valuable.

> We do **not** fabricate users, pilots, testimonials, stars, or case studies.
> Real adoption is tracked only via opt-in
> [design-partner feedback](../.github/ISSUE_TEMPLATE/design_partner_feedback.yml).

## Where contributions are welcome

| Area | Examples |
| --- | --- |
| Core-read hardening | Better bounded consume/search, lag/DLQ diagnosis, discovery |
| Governance wedge | Policy simulation, approval UX, blast-radius previews, signed decision records |
| Compatibility | Redpanda/MSK/Confluent profiles, KIP-1318 adapter |
| Truthfulness | Removing simulated behavior from production paths, docs accuracy |
| Docs & DX | Doctor checks, examples, playbooks, comparison accuracy |

## Where contributions are currently out of scope (demand-gated)

Managed cloud, billing, SSO/SAML, marketplace, and generic streaming-platform
expansion. See [`adr/0001-defer-managed-cloud-saml-billing.md`](adr/0001-defer-managed-cloud-saml-billing.md).
New experimental/simulated tools will generally not be accepted.

## Issue labels

| Label | Meaning |
| --- | --- |
| `good-first-issue` | Small, well-scoped, no deep context required |
| `help-wanted` | Maintainers would welcome a contributor |
| `area:core-read` | The read-only default surface |
| `area:governance` | Policy, approval, masking, audit |
| `area:compatibility` | Kafka/Redpanda/MSK/Confluent/KIP-1318 |
| `area:security` | Identity, transport, fail-closed behavior |
| `type:truthfulness` | Removing/labeling simulated behavior |
| `feedback` | Design-partner or user feedback |
| `needs-triage` | Not yet reviewed by a maintainer |

## Good-first-issue candidates

These are concrete, low-context starting points:

1. Add a `doctor` check for OIDC issuer reachability (extend `internal/doctor`).
2. Add more read-only tasks/adversarial cases to the tool-routing harness
   (`internal/tooleval/fixtures.go`).
3. Add recovery-guidance strings for any write tool missing one
   (`internal/server/register_approval.go`).
4. Improve an example config under `examples/` with inline comments.
5. Add a compatibility-matrix row after testing a new Kafka/Redpanda version.

## Support matrix

| Component | Supported |
| --- | --- |
| Go toolchain | 1.26.x (see `go.mod` toolchain) |
| MCP transport | stdio (recommended locally), sse, streamable-http |
| Identity providers | none (stdio only), static, OIDC, mTLS |
| Persistence | in-memory (default), SQLite (single-node only) |
| OS/arch (release binaries) | linux/darwin/windows × amd64/arm64 |
| Tool packs | `core-read` (default), `ops-write`, `governance`; `essential`/`ops` deprecated aliases |
| Removed packs | `analytics`, `streaming`, `all`, `experimental` — rejected at startup |

## Compatibility matrix

> Rows are added only after a version has actually been exercised against the
> local integration harness. Do not add unverified rows.

| Platform | Version tested | Read core | Write (ops-write) | Schema registry | Notes |
| --- | --- | --- | --- | --- | --- |
| Apache Kafka | (local docker-dev) | ✓ | ✓ | ✓ | Primary target; CI integration profile |
| Redpanda | _to verify_ | — | — | — | Add after running the Redpanda harness |
| Amazon MSK | _profile only_ | — | — | — | Documented config profile; not certified |
| Confluent Cloud | _profile only_ | — | — | — | Documented config profile; not certified |

## How to propose a change

1. Open an issue describing the problem and how it fits the narrowed roadmap.
2. For code, follow `CONTRIBUTING.md`, run `make validate`, and add tests.
3. Keep the default surface read-only and route any new mutation through the
   approval enforcement path.
