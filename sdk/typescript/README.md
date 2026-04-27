# @kafkamcp/sdk

TypeScript SDK for [KafkaMCP](https://github.com/kafkamcp/kafkamcp) — type-safe async/await access to Kafka through the Model Context Protocol.

## Prerequisites

- Node.js ≥ 18.0.0
- A running KafkaMCP server (see [main README](../../README.md))

## Installation

```bash
npm install @kafkamcp/sdk
```

## Quick Start

```typescript
import { KafkaMCPClient, Transport } from "@kafkamcp/sdk";

// Provide a Transport implementation that connects to your KafkaMCP server
const transport: Transport = /* your transport (stdio, SSE, etc.) */;

const client = new KafkaMCPClient(transport, {
  agentId: "my-agent",
  timeout: 30000,
});

// List topics
const topics = await client.listTopics();
console.log(topics);

// Consume messages
const messages = await client.consume("my-topic", 10, "latest");
console.log(messages);

// Clean up
await client.close();
```

## API Overview

### Topic Operations

| Method | Description |
|---|---|
| `listTopics(filter?)` | List topics, optionally filtered by name pattern |
| `describeTopic(topic)` | Get detailed topic metadata |
| `createTopic(topic, partitions, replicationFactor, configs?)` | Create a new topic |
| `deleteTopic(topic)` | Delete a topic |

### Message Operations

| Method | Description |
|---|---|
| `consume(topic, limit?, offset?)` | Consume messages from a topic |
| `produce(topic, messages)` | Produce messages to a topic |
| `search(topic, valueContains, limit?)` | Search messages by value content |

### Consumer Group Operations

| Method | Description |
|---|---|
| `listConsumerGroups(cluster?)` | List consumer groups |
| `describeConsumerGroup(groupId)` | Describe a consumer group |
| `resetOffsets(groupId, topic, strategy)` | Reset consumer group offsets |

### Schema Registry

| Method | Description |
|---|---|
| `listSchemas()` | List all schema subjects |
| `describeSchema(subject)` | Get schema details for a subject |

### AI & Analytics

| Method | Description |
|---|---|
| `naturalLanguageQuery(query)` | Query Kafka using natural language |
| `getAnomalies(severity?, limit?)` | Detect anomalies in the cluster |
| `advisorScan(cluster?)` | Run a cluster health scan |
| `advisorReport(cluster?)` | Generate a cluster health report |
| `diagnoseLag(groupId)` | Diagnose consumer lag root causes |

## Transport Interface

The SDK requires a `Transport` implementation to communicate with KafkaMCP:

```typescript
interface Transport {
  callTool(name: string, args: Record<string, unknown>): Promise<unknown>;
  readResource(uri: string): Promise<unknown>;
  close(): Promise<void>;
}
```

## Development

```bash
npm install
npm run build
npm run test
npm run lint
```

## License

Apache-2.0
