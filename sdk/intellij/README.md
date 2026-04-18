# KafkaMCP IntelliJ Plugin

> **Status**: Placeholder — VS Code and Cursor are prioritized. IntelliJ plugin will be developed based on community demand.

## Planned Features

- Topic/Schema/Consumer Group tool windows
- Inline consume/produce from editor
- Schema validation with Avro/Protobuf language support
- Integration with KafkaMCP server via MCP protocol

## Architecture Notes

The IntelliJ plugin would use:
- **Platform**: IntelliJ Platform SDK
- **Language**: Kotlin
- **Communication**: MCP protocol over HTTP/SSE (same as VS Code extension)
- **UI**: IntelliJ tool windows with JBTable for data display

## Contributing

If you're interested in developing the IntelliJ plugin, see [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.

The plugin should follow the same MCP client pattern as the VS Code extension,
using the `mcp-go` compatible HTTP API at the configured server URL.
