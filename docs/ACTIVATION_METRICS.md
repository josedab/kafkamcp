# Activation Metrics (opt-in, local, privacy-preserving)

KafkaMCP can expose a small set of **activation metrics** that show whether an
installation is successfully set up and exercising the core secure workflows.

## Principles

- **Opt-in.** Disabled by default. Enable with `telemetry.activation_metrics: true`.
- **No phone-home.** Metrics are exposed only on the local Prometheus endpoint
  (the metrics port for network transports). Nothing is transmitted anywhere.
- **Privacy-preserving.** Labels are low-cardinality and bounded. Raw agent and
  resource identifiers are never used as labels; the active-agent gauge counts
  opaque hashes in a 24-hour window and caps the in-memory set at 10,000.

## Metrics

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kafkamcp_activation_setup_total` | counter | `outcome` | Setup / cluster-validation successes and failures |
| `kafkamcp_activation_active_verified_agents` | gauge | — | Distinct cryptographically-verified agents in the current 24-hour process window (capped at 10,000) |
| `kafkamcp_activation_tool_pack_use_total` | counter | `pack` | Active tool-pack selections at startup |
| `kafkamcp_activation_denied_unsafe_calls_total` | counter | `tool` | Write/destructive calls denied for lack of a valid approval grant |
| `kafkamcp_activation_approval_outcomes_total` | counter | `outcome` | Approval lifecycle events (request/approve/use/deny/expire/revoke) |
| `kafkamcp_activation_core_workflow_total` | counter | `workflow` | Completions of core secure workflows |

All label values are bounded: `pack` ∈ the known pack names, `tool` ∈ the
authoritative write set, and `outcome`/`workflow` are fixed enums.

## Enabling

```yaml
telemetry:
  activation_metrics: true
```

Scrape the local metrics endpoint (`/metrics`) with Prometheus as usual. See
[`OPERATIONS.md`](OPERATIONS.md) for the metrics port and SLO guidance.
