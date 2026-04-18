# KafkaMCP VS Code Extension

Browse topics, consume/produce messages, view schemas, run workflows, and query Kafka with natural language — all from VS Code.

## Features

- **Topic Explorer**: Tree view of all Kafka topics with partition counts and message estimates
- **Schema Browser**: Browse Schema Registry subjects with inline schema viewing
- **Consumer Groups**: View consumer group status and lag
- **Workflow Templates**: Run pre-built workflows (DLQ investigation, capacity planning, etc.)
- **Natural Language Query**: Ask questions in plain English
- **Cost Analysis**: Estimate topic storage costs inline

## Setup

1. Install the extension
2. Configure `kafkamcp.serverUrl` in VS Code settings (default: `http://localhost:8080`)
3. Start your KafkaMCP server
4. Use the KafkaMCP activity bar icon to browse topics

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `kafkamcp.serverUrl` | `http://localhost:8080` | KafkaMCP server URL |
| `kafkamcp.agentId` | `vscode-extension` | Agent ID for authentication |
| `kafkamcp.autoRefresh` | `true` | Auto-refresh topic list |
| `kafkamcp.refreshIntervalMs` | `30000` | Refresh interval in ms |

## Commands

- `KafkaMCP: Connect to Server` - Configure server connection
- `KafkaMCP: Consume Messages` - Read messages from a topic
- `KafkaMCP: Produce Message` - Publish a message
- `KafkaMCP: Search Messages` - Search messages by content
- `KafkaMCP: Natural Language Query` - Query Kafka in plain English
- `KafkaMCP: Run Workflow` - Execute a workflow template
- `KafkaMCP: Analyze Cost` - Get cost analysis for a topic

## Development

```bash
cd sdk/vscode
npm install
npm run compile
```

Press F5 in VS Code to launch the extension in development mode.
