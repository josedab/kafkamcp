# ADR-004: Subsystem Grouping Architecture

## Status

Accepted

## Context

As KafkaMCP grew beyond its initial set of tools, the `Server` struct accumulated dozens of subsystem fields — anomaly detectors, policy engines, workflow engines, streaming managers, and more. This created several problems:

1. **Cognitive overload** — Developers reading `server.go` had to scan 30+ fields to understand the system.
2. **Unclear ownership** — It was not obvious which subsystems were related or which team/domain owned them.
3. **Initialization sprawl** — The `New()` constructor became a wall of initialization code with no logical grouping.
4. **Shutdown ordering** — `Close()` had no structured way to shut down related subsystems together.

Options considered:

1. **Flat field list** — Keep all subsystem fields directly on `Server`. Simple but unmanageable at scale.
2. **Interface-based subsystem registry** — Register subsystems dynamically via an interface. Flexible but loses type safety.
3. **Composite struct grouping** — Group related fields into embedded structs by domain. Preserves type safety and direct field access.

## Decision

We organize subsystem fields into four **composite structs** embedded in `Server`:

- **Analytics** — AI-powered analysis and intelligence (anomaly detection, NL query, cost analysis, forecasting, knowledge graph, lag diagnosis, schema evolution, time-series analytics, advisor).
- **Governance** — Policy, audit, security, and compliance (policy engine, audit logger, rate limiter, masking, contracts, personas, multi-tenancy, compliance).
- **Streaming** — Event-driven and stream-processing (watch subscriptions, DLQ remediation, replication monitoring, self-healing, lineage tracking, message tracing, flow debugging).
- **Platform** — Infrastructure, orchestration, and extensibility (plugins, agent registry, telemetry, workflows, pipelines, chaos engineering, replay, federation, and all other operational subsystems).

```go
type Server struct {
    // Core fields
    config       *config.Config
    kafkaManager kafka.KafkaManager
    // ...

    // Composite subsystems (embedded for direct field access)
    Analytics
    Governance
    Streaming
    Platform
}
```

Key design choices:

- **Embedded structs** — `s.anomalyDetector` works directly without `s.Analytics.anomalyDetector`, keeping call sites unchanged.
- **Domain-driven boundaries** — Groups follow operational domains, not technical layers.
- **Unexported fields** — Subsystem instances remain unexported; the grouping is an internal organizational concern.
- **Four groups, not more** — Fewer groups reduce the overhead of deciding where new subsystems belong.

### Placement heuristic for new subsystems

| Question | If yes → |
|----------|----------|
| Does it analyze data, detect patterns, or produce insights? | **Analytics** |
| Does it enforce policy, audit actions, or manage access? | **Governance** |
| Does it process events in real-time or manage subscriptions? | **Streaming** |
| Everything else (infra, orchestration, extensibility) | **Platform** |

## Consequences

### Positive

- `server.go` is scannable — each group has a clear label and bounded scope.
- New subsystems have a clear placement heuristic rather than appending to an unbounded list.
- `Close()` can be structured to shut down groups in order (Streaming → Analytics → Governance → Platform).
- Code reviewers can quickly identify which domain a change affects.

### Negative

- Embedding four structs means Go's field promotion rules apply — name collisions between groups would cause compile errors.
- Moving a subsystem between groups requires updating both the struct definition and the constructor.
- The four-group taxonomy is a judgment call; some subsystems (e.g., lineage) could arguably fit in Analytics or Streaming.

### Neutral

- This is a purely internal organizational decision with no impact on the MCP protocol surface or external APIs.
- The grouping can evolve — splitting Platform into smaller groups is a natural future step if it grows too large.
