---
sidebar_position: 10
title: IntelliJ Plugin
---

# IntelliJ Plugin

KafkaMCP provides an IntelliJ/JetBrains IDE integration at [`sdk/intellij/`](https://github.com/kafkamcp/kafkamcp/tree/main/sdk/intellij) for working with Kafka topics, schemas, and consumer groups directly from your IDE.

:::info Status
The IntelliJ plugin is currently a **planned feature**. VS Code and Cursor integrations are available now. The IntelliJ plugin will be developed based on community demand — see [Contributing](#contributing) below if you'd like to help.
:::

## Planned Features

- **Topic/Schema/Consumer Group tool windows** — Browse Kafka resources in the IDE sidebar
- **Inline consume/produce** — Read and write messages from the editor
- **Schema validation** — Avro/Protobuf/JSON Schema support with language integration
- **MCP protocol integration** — Connects to KafkaMCP server via HTTP/SSE

## Architecture

The plugin will use:

| Component | Technology |
|-----------|-----------|
| Platform | IntelliJ Platform SDK |
| Language | Kotlin |
| Communication | MCP protocol over HTTP/SSE |
| UI | IntelliJ tool windows with JBTable |

The plugin follows the same MCP client pattern as the VS Code extension, using the HTTP/SSE API at the configured KafkaMCP server URL.

## Prerequisites

Before using the IntelliJ plugin, ensure KafkaMCP is running with SSE or streamable-http transport:

```yaml
server:
  transport: sse
  host: 0.0.0.0
  port: 3100
```

```bash
kafkamcp --config kafkamcp.yaml
```

## Current Alternatives

While the IntelliJ plugin is under development, JetBrains users can:

1. **Use the MCP HTTP API directly** — KafkaMCP's SSE/HTTP transport works with any HTTP client
2. **Use the TypeScript SDK** — For Node.js-based tooling within your project
3. **Use the Go SDK** — For programmatic access from Go codebases
4. **Use Claude Desktop** — Connect via stdio transport for AI-assisted Kafka operations

## Contributing

If you're interested in developing the IntelliJ plugin:

1. Review the [VS Code extension](https://github.com/kafkamcp/kafkamcp/tree/main/sdk/vscode) for reference implementation patterns
2. See [CONTRIBUTING.md](https://github.com/kafkamcp/kafkamcp/blob/main/CONTRIBUTING.md) for guidelines
3. Open an issue or discussion on GitHub to coordinate with maintainers

## See Also

- [Getting Started](../getting-started.md) — Initial setup
- [Go SDK Usage](go-sdk.md) — Programmatic Go client
- [TypeScript SDK Usage](typescript-sdk.md) — TypeScript client
- [Dashboard Web UI](dashboard.md) — Browser-based monitoring
