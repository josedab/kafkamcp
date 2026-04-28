---
sidebar_position: 7
title: TypeScript SDK Usage
---

# TypeScript SDK Usage

KafkaMCP provides a TypeScript SDK at [`sdk/typescript/`](https://github.com/kafkamcp/kafkamcp/tree/main/sdk/typescript) for type-safe, async/await access to all KafkaMCP tools from Node.js or TypeScript agents.

## Installation

```bash
npm install @kafkamcp/sdk
```

## Creating a Client

```typescript
import { KafkaMCPClient } from "@kafkamcp/sdk";

// Provide an MCP transport (stdio, SSE, or HTTP)
const client = new KafkaMCPClient(transport, {
  agentId: "my-ts-agent",
  timeout: 30000,
});
```

## Topic Operations

```typescript
// List all topics matching a pattern
const topics = await client.listTopics("orders.*");

// Describe a specific topic
const details = await client.describeTopic("orders.created");

// Create a topic
await client.createTopic("events.new", 6, 3, {
  "retention.ms": "604800000",
});

// Delete a topic
await client.deleteTopic("events.old");
```

## Message Operations

```typescript
// Consume the latest 10 messages
const messages = await client.consume("orders.created", 10, "latest");

// Produce messages
const results = await client.produce("orders.created", [
  { key: "order-1", value: { id: 1, total: 99.99 } },
  { key: "order-2", value: { id: 2, total: 149.99 } },
]);

// Search messages by content
const matches = await client.search("orders.created", "refund", 50);
```

## Consumer Groups

```typescript
// List all consumer groups
const groups = await client.listConsumerGroups();

// Describe a specific group
const group = await client.describeConsumerGroup("order-processor");

// Reset offsets (group must be inactive)
await client.resetOffsets("order-processor", "orders.created", "earliest");
```

## Schema Registry

```typescript
// List all subjects
const subjects = await client.listSchemas();

// Describe a schema
const schema = await client.describeSchema("orders-value");
```

## AI & Analytics

```typescript
// Natural language query
const answer = await client.naturalLanguageQuery(
  "What are the busiest topics in the last hour?"
);

// Get anomalies
const anomalies = await client.getAnomalies("critical", 10);

// Advisor scan and report
const scan = await client.advisorScan();
const report = await client.advisorReport();

// Diagnose consumer lag
const diagnosis = await client.diagnoseLag("order-processor");
```

## Cleanup

```typescript
await client.close();
```

## Interfaces

The SDK exports the following TypeScript interfaces:

| Interface | Description |
|-----------|-------------|
| `Transport` | MCP transport abstraction (implement for custom connectivity) |
| `ClientOptions` | Client configuration (`agentId`, `timeout`) |
| `TopicSummary` | Topic metadata (name, partitions, replication factor) |
| `Message` | Consumed/searched message (partition, offset, key, value, headers) |
| `DeliveryResult` | Produce result (partition, offset, timestamp, status) |
| `ConsumerGroupSummary` | Group overview (group_id, state, member_count, total_lag) |
| `SchemaSubject` | Schema Registry subject (subject, version, schema_type) |

## See Also

- [Go SDK Usage](go-sdk.md) — Native Go client
- [API Reference: Tools](../api-reference/tools.md) — Full list of MCP tools
- [API Reference: Resources](../api-reference/resources.md) — MCP resources
