---
sidebar_position: 2
title: "API Reference: Resources"
---

# API Reference: Resources

KafkaMCP exposes **nine stable, read-only MCP resources**. Resource reads pass
through the same identity, policy, rate-limit, masking, and audit controls as
read-only tools.

| URI | Description |
|---|---|
| `kafka://cluster/metadata` | Brokers, controller, topic count, and partition count for the default cluster |
| `kafka://clusters` | Configured cluster names and safe connection metadata |
| `kafka://topics` | Authorization-filtered topic catalog |
| `kafka://topics/{name}` | Topic partitions, offsets, replicas, ISR, and configuration |
| `kafka://schemas` | Schema Registry subject catalog when configured |
| `kafka://schemas/{subject}` | Schema definition, version, type, and field summary |
| `kafka://groups` | Consumer-group catalog with state and lag |
| `kafka://groups/{id}` | Members, assignments, offsets, and partition lag |
| `kafka://audit/log` | Recent redacted audit entries |

:::info Cluster routing
Cluster-specific resources use the configured default cluster. Use
`kafka://clusters` to discover names, then use a core-read tool with its
`cluster` argument when explicit routing is required.
:::

:::note Schema Registry
Schema resources return a clear configuration error when no Schema Registry is
configured for the default cluster.
:::

The resource URI list is protected by the stable compatibility snapshot in
`internal/server/testdata/stable_core_read.json`. See
[API Stability](https://github.com/josedab/kafkamcp/blob/main/docs/API_STABILITY.md)
for versioning guarantees.
