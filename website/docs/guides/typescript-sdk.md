---
sidebar_position: 7
title: TypeScript SDK Usage
---

# TypeScript SDK Usage

KafkaMCP provides a TypeScript SDK at
[`sdk/typescript/`](https://github.com/josedab/kafkamcp/tree/main/sdk/typescript)
for a tested subset of topic, message, schema, consumer-group, and lag tools.

## Installation

The TypeScript SDK is currently a private workspace package and is not
published to npm.

```bash
git clone https://github.com/josedab/kafkamcp.git
cd kafkamcp/sdk/typescript
npm install
```

## Creating a Client

```typescript
import { KafkaMCPClient } from "@josedab/kafkamcp-sdk";

// Provide an MCP transport (stdio, SSE, or HTTP)
const client = new KafkaMCPClient(transport);
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
}, { approvalToken, idempotencyKey: "create-events-new" });

// Delete a topic
await client.deleteTopic("events.old", {
  approvalToken,
  idempotencyKey: "delete-events-old",
});
```

## Message Operations

```typescript
// Consume the latest 10 messages
const messages = await client.consume("orders.created", 10, "latest");

// Produce messages
const results = await client.produce("orders.created", [
  { key: "order-1", value: { id: 1, total: 99.99 } },
  { key: "order-2", value: { id: 2, total: 149.99 } },
], { approvalToken });

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
await client.resetOffsets(
  "order-processor",
  "orders.created",
  "earliest",
  { approvalToken },
);
```

## Schema Registry

```typescript
// List all subjects
const subjects = await client.listSchemas();

// Describe a schema
const schema = await client.getSchema("orders-value");
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
| `MutationOptions` | Approval token, optional idempotency key, and cluster |
| `TopicSummary` | Topic metadata (name, partitions, replication factor) |
| `Message` | Consumed/searched message (partition, offset, key, value, headers) |
| `DeliveryResult` | Produce result (partition, offset, timestamp, status) |
| `ConsumerGroupSummary` | Group overview (group_id, state, member_count, total_lag) |
| `SchemaSubject` | Schema Registry subject (subject, version, schema_type) |

Identity and timeouts are configured by the MCP `Transport`. KafkaMCP write
tools reject calls without a matching short-lived approval token.

## See Also

- [Go SDK Usage](go-sdk.md) — Native Go client
- [API Reference: Tools](../api-reference/tools.md) — Full list of MCP tools
- [API Reference: Resources](../api-reference/resources.md) — MCP resources
