# Production Deployment Checklist

Use this checklist before exposing KafkaMCP to production agents. It describes
the supported production shape; passing repository tests alone is not a
substitute for validating your Kafka, identity, and network environment.

## Release and image

- Deploy an immutable version tag, never a mutable development tag.
- Verify checksums, Sigstore signatures, SBOMs, and GitHub provenance as
  described in [`RELEASE.md`](RELEASE.md).
- Run `make readiness`, `make security`, and `make helm-check` against the exact
  source revision being promoted.

## Identity and authorization

- Use `sse` or `streamable-http` with `policies.default_deny: true`.
- Configure `static`, `oidc`, or `mtls` identity and keep
  `allow_header_fallback: false`.
- Do not set `server.dangerously_allow_unauthenticated_network` or
  `KAFKAMCP_UNSAFE_ALLOW_UNAUTHENTICATED_NETWORK`.
- Give each agent only the topic, group, schema, and action permissions it
  requires. Test denied paths before rollout.
- Keep two-person approval enabled for guarded packs and store the approval
  signing key in a secrets manager.

## Replica and persistence model

- `core-read` with `inmem` may run multiple replicas.
- `ops-write`, `governance`, or SQLite must run with **one active replica**.
- Use a `ReadWriteOnce` local/block-backed PVC for SQLite. Do not use
  NFS/EFS/SMB or share one file between pods.
- Use active/passive failover for guarded deployments. Stop the active writer
  before promoting a passive instance.
- Back up SQLite with the online backup procedure in
  [`OPERATIONS.md`](OPERATIONS.md) and test restore plus integrity checks.

## TLS and secrets

- Use `SASL_SSL` or `SSL` for production Kafka traffic.
- TLS 1.2 is the compatibility minimum; set `tls_min_version: "1.3"` when the
  entire broker/proxy path supports it.
- Mount CA bundles read-only and keep `insecure_skip_verify: false`.
- Pass Kafka, Schema Registry, OIDC, masking, identity, and approval secrets
  through environment variables or a secrets manager, never a ConfigMap/image.
- Exercise credential and signing-key rotation before the first incident.

## Network and probes

- Expose only the MCP API port to agent networks.
- Bind the auxiliary listener to `0.0.0.0` only inside a protected
  container/pod network; otherwise keep the loopback default.
- Restrict the metrics port to kubelet/load-balancer health checks and your
  monitoring namespace.
- Use `/livez` for startup/liveness, `/ready` for readiness, `/healthz` only for
  Kafka diagnostics, and `/metrics` for Prometheus.
- Permit OTLP ports (`4317`/`4318` by default) when telemetry is enabled.
- Terminate external TLS at a trusted gateway or configure KafkaMCP server TLS
  and mTLS directly.

## Capacity and observability

- Set CPU/memory requests and limits from load tests using your real topic
  counts, message sizes, and concurrent agents.
- Alert on tool error rate/latency, Kafka connection errors, authorization
  denials, rate-limit denials, approval failures, and pod readiness.
- Ship `audit.log_file` to immutable external storage/SIEM; the in-process ring
  buffer is not a long-term audit archive.
- Keep activation metrics opt-in and do not add raw identifiers or message
  contents to metrics labels.

## Rollout and recovery

- Validate the config with `kafkamcp validate` and connectivity with
  `kafkamcp doctor --json`.
- Start with `core-read`; enable guarded packs only after identity, approval,
  persistence, audit export, and rollback drills pass.
- Run a canary against non-critical topics and execute the incident demo/runbook.
- Record the previous image/config version and test rollback before promotion.
- Confirm KafkaMCP shuts down within the orchestrator termination grace period
  and that in-flight operations surface explicit errors rather than hanging.
