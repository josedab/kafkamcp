# KafkaMCP Go SDK

Go client SDK for [KafkaMCP](https://github.com/josedab/kafkamcp) — interact with Kafka through MCP from Go applications and AI agents.

## Installation

```bash
go get github.com/josedab/kafkamcp/sdk/go/kafkamcpsdk
```

## Quick Start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/josedab/kafkamcp/sdk/go/kafkamcpsdk"
)

func main() {
	// Create a transport (implement kafkamcpsdk.Transport interface)
	transport := NewMyTransport("localhost:3100")
	defer transport.Close()

	// Create the SDK client
	client := kafkamcpsdk.New(transport, kafkamcpsdk.WithTimeout(10*time.Second))
	defer client.Close()

	ctx := context.Background()

	// List topics
	topics, err := client.ListTopics(ctx, "orders.*")
	if err != nil {
		log.Fatal(err)
	}
	for _, t := range topics {
		fmt.Printf("Topic: %s (%d partitions)\n", t.Name, t.Partitions)
	}

	// Consume messages
	messages, err := client.Consume(ctx, "orders.created", 5, "latest")
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range messages {
		fmt.Printf("Offset %d: %v\n", m.Offset, m.Value)
	}

	// Produce messages
	results, err := client.Produce(ctx, "test-events", []map[string]any{
		{"key": "order-1", "value": `{"status": "shipped"}`},
	}, kafkamcpsdk.MutationOptions{ApprovalToken: approvalToken})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Produced to partition %d at offset %d\n", results[0].Partition, results[0].Offset)
}
```

## API Reference

### Client

```go
// New creates a new KafkaMCP SDK client.
func New(transport Transport, opts ...Option) *Client
```

**Options:**
- `WithTimeout(d time.Duration)` — Set the default request timeout (default: `30s`)

Authentication belongs to the transport (static token, OIDC, mTLS, or stdio).
Guarded write methods require `MutationOptions` with an approval token.

### Transport Interface

```go
type Transport interface {
	CallTool(ctx context.Context, name string, args map[string]any) (json.RawMessage, error)
	ReadResource(ctx context.Context, uri string) (json.RawMessage, error)
	Close() error
}
```

Implement this interface to connect to a KafkaMCP server via stdio, SSE, or HTTP.

### Methods

#### Core

| Method | Description |
|--------|-------------|
| `ListTopics(ctx, filter)` | List topics matching an optional glob filter |
| `Consume(ctx, topic, limit, offset)` | Read messages from a topic |
| `Produce(ctx, topic, messages, mutationOptions)` | Publish messages to a topic |
| `Search(ctx, topic, valueContains, limit)` | Search messages by content |
| `Close()` | Close the client connection |

#### Consumer Groups

| Method | Description |
|--------|-------------|
| `ListConsumerGroups(ctx, cluster)` | List all consumer groups, optionally filtered by cluster |
| `DescribeConsumerGroup(ctx, groupID)` | Get detailed info about a consumer group |

#### Topic Management

| Method | Description |
|--------|-------------|
| `DescribeTopic(ctx, topic)` | Get detailed information about a topic |
| `CreateTopic(ctx, topic, partitions, replicationFactor, configs, mutationOptions)` | Create a new Kafka topic |
| `DeleteTopic(ctx, topic, mutationOptions)` | Delete a Kafka topic |

#### Schema Registry

| Method | Description |
|--------|-------------|
| `ListSchemas(ctx)` | List all schema subjects |
| `GetSchema(ctx, subject)` | Get a schema by subject name |

#### Admin Operations

| Method | Description |
|--------|-------------|
| `ResetOffsets(ctx, groupID, topic, strategy, mutationOptions)` | Reset consumer group offsets (earliest, latest, timestamp) |
| `DiagnoseLag(ctx, groupID)` | Diagnose consumer lag for a group |

### Types

- `TopicSummary` — Topic name, partitions, replication factor, message count estimate
- `Message` — Partition, offset, timestamp, key, value, headers
- `DeliveryResult` — Partition, offset, timestamp, status
- `ConsumerGroupSummary` — Group ID, state, member count, total lag
- `SchemaInfo` — Subject, version, schema type, schema content

## Links

- [Main Project](https://github.com/josedab/kafkamcp)
- [Documentation](https://josedab.github.io/kafkamcp/)
- [API Docs (GoDoc)](https://pkg.go.dev/github.com/josedab/kafkamcp/sdk/go/kafkamcpsdk)

## License

Apache-2.0
