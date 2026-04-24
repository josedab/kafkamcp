# KafkaMCP IntelliJ Plugin

IDE integration for KafkaMCP, providing Kafka topic browsing, schema-aware editing,
and inline message inspection directly in IntelliJ IDEA.

## Features

- **Topic Browser**: Browse and inspect Kafka topics in a tree view panel
- **Schema-Aware Editing**: Autocomplete fields from Avro/Protobuf/JSON schemas
- **Message Inspector**: Preview messages inline without leaving your IDE
- **Consumer Group Monitoring**: View consumer lag and group status
- **MCP Tool Integration**: Execute KafkaMCP tools directly from the IDE

## Requirements

- IntelliJ IDEA 2024.1+
- Running KafkaMCP server (default: `http://localhost:3100`)

## Setup

1. Install the plugin from the JetBrains Marketplace (or build from source)
2. Configure the KafkaMCP server URL in Settings → Tools → KafkaMCP
3. The plugin will auto-connect on startup

## Building from Source

```bash
cd ide/intellij
./gradlew buildPlugin
```

The built plugin will be at `build/distributions/kafkamcp-plugin-*.zip`.

## Server-Side API

The plugin connects to the KafkaMCP server's IDE integration API at `/api/ide/v1/`:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/ide/v1/connect` | POST | Establish IDE session |
| `/api/ide/v1/topics` | GET | Browse topics |
| `/api/ide/v1/topics/{name}` | GET | Topic detail with partitions |
| `/api/ide/v1/schemas` | GET | List schemas |
| `/api/ide/v1/messages` | GET | Preview messages |
| `/api/ide/v1/completions` | GET | Autocompletion suggestions |
| `/api/ide/v1/health` | GET | Server health check |
