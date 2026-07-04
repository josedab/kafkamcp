---
sidebar_position: 6
title: Go SDK Usage
---

# Go SDK Usage

KafkaMCP provides a native Go SDK at
[`sdk/go/kafkamcpsdk`](https://github.com/josedab/kafkamcp/tree/main/sdk/go/kafkamcpsdk)
for a tested subset of topic, message, schema, consumer-group, and lag tools.

## Installation

```bash
go get github.com/josedab/kafkamcp/sdk/go/kafkamcpsdk
```

## Creating a client

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
    client := kafkamcpsdk.New(transport,
        kafkamcpsdk.WithTimeout(30*time.Second),
    )
    defer client.Close()
}
```

## Listing topics

```go
topics, err := client.ListTopics(ctx, "orders.*")
if err != nil {
    log.Fatal(err)
}
for _, t := range topics {
    fmt.Printf("Topic: %s  Partitions: %d\n", t.Name, t.Partitions)
}
```

## Consuming messages

```go
messages, err := client.Consume(ctx, "orders.created", 10, "latest")
if err != nil {
    log.Fatal(err)
}
for _, msg := range messages {
    fmt.Printf("[%d:%d] key=%s value=%v\n",
        msg.Partition, msg.Offset, msg.Key, msg.Value)
}
```

## Producing messages

```go
deliveries, err := client.Produce(ctx, "ops.events", []map[string]any{
    {
        "key":   "deploy-123",
        "value": map[string]any{"status": "started", "service": "billing"},
    },
}, kafkamcpsdk.MutationOptions{ApprovalToken: approvalToken})
if err != nil {
    log.Fatal(err)
}
for _, d := range deliveries {
    fmt.Printf("Delivered to partition %d offset %d\n", d.Partition, d.Offset)
}
```

The transport owns authentication. Every guarded mutation requires a
short-lived token returned by `kafka_approval`; retry-safe topic mutations may
also set `IdempotencyKey`.

## Resetting consumer offsets

```go
err := client.ResetOffsets(
    ctx,
    "order-processor",
    "orders.created",
    "timestamp",
    kafkamcpsdk.ResetOffsetsOptions{
        MutationOptions: kafkamcpsdk.MutationOptions{
            ApprovalToken: approvalToken,
        },
        Value:  "2026-07-27T00:00:00Z",
        DryRun: true,
    },
)
if err != nil {
    log.Fatal(err)
}
```

Set `Value` for timestamp resets. Use `DryRun` to preview the partition offset
changes before applying the approved request.

## Searching messages

```go
results, err := client.Search(ctx, "payments.events", "failed", 5)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Found %d failed payment messages\n", len(results))
```

## Error handling

All SDK methods return standard Go errors. Check for errors after each call:

```go
topics, err := client.ListTopics(ctx, "*")
if err != nil {
    // Handle authorization errors, network issues, etc.
    log.Fatalf("Failed to list topics: %v", err)
}
```

## Configuration with masking

When consuming sensitive data, configure masking rules in your `kafkamcp.yaml`:

```yaml
masking:
  enabled: true
  rules:
    - name: redact-emails
      type: email
      field_path: "**email**"
      agents: ["my-agent"]
```

Messages consumed via the SDK will have matching fields automatically redacted.

## Next steps

- See the [API Reference: Tools](/docs/api-reference/tools) for all available operations
- See the [Access Control](/docs/guides/access-control) guide for policy configuration
- See the [Monitoring](/docs/guides/monitoring) guide for observability setup
