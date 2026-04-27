# Go SDK Example

Demonstrates using the KafkaMCP Go SDK to list topics via a stdio transport.

## Prerequisites

- Go 1.21+
- `kafkamcp` binary installed and on `PATH`
- A running Kafka broker (e.g., `make docker-dev` from the repo root)
- A valid `kafkamcp.yaml` configuration file

## Setup

```bash
# From the repository root
make docker-dev   # start local Kafka
make build        # build kafkamcp binary
```

## Usage

```bash
# Run with default config (kafkamcp.yaml in current directory)
go run ./examples/go-sdk/

# Run with a custom config path
go run ./examples/go-sdk/ /path/to/kafkamcp.yaml
```

## Expected Output

```
KafkaMCP Go SDK Example
Config: kafkamcp.yaml

Listing topics...
Found 3 topic(s):
  orders                          partitions=6   replication=1
  users                           partitions=3   replication=1
  events                          partitions=12  replication=1
```

## What It Does

1. Spawns a `kafkamcp` child process with stdio transport
2. Creates a Go SDK client with a custom agent ID
3. Calls `kafka_list_topics` and prints the results
