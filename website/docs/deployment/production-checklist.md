---
sidebar_position: 4
title: Production Checklist
---

# Production Checklist

Before exposing KafkaMCP to production agents:

1. Deploy an immutable release and verify checksums, Sigstore signatures, SBOM,
   and provenance.
2. Run `make readiness`, `make security`, and `make helm-check`.
3. Use cryptographic identity, `default_deny: true`, and disabled header
   fallback.
4. Run exactly one active replica when SQLite, `ops-write`, or `governance` is
   enabled. Multi-replica is supported only for read-only in-memory deployments.
5. Use a `ReadWriteOnce` PVC for SQLite; never share the file over NFS/EFS.
6. Use `/livez` for liveness and `/ready` for readiness.
7. Restrict the metrics port and explicitly permit OTLP egress when telemetry is
   enabled.
8. Use TLS/SASL, external secret management, durable audit export, tested
   backups, and a documented rollback.
9. Start with `core-read`, canary on non-critical topics, and enable guarded
   writes only after approval and recovery drills pass.

The complete operator checklist, including TLS, observability, rotation,
capacity, backup, and failover requirements, is maintained in
[`docs/PRODUCTION_CHECKLIST.md`](https://github.com/josedab/kafkamcp/blob/main/docs/PRODUCTION_CHECKLIST.md).
