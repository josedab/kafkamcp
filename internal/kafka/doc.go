// Package kafka contains the core business logic for Kafka operations in KafkaMCP.
//
// It provides a Manager that wraps the confluent-kafka-go admin, producer, and
// consumer clients to expose higher-level operations used by the MCP server:
//
//   - Cluster metadata retrieval and multi-cluster routing
//   - Topic CRUD (list, describe, create, alter, delete)
//   - Message production with optional schema-aware serialization
//   - Message consumption with configurable offsets and deserialization
//   - Consumer group inspection, offset reset, and subscription
//   - Cross-topic search, aggregation, and key-based joins
//   - Dead-letter queue (DLQ) analysis
//   - Natural-language query translation
//   - Schema evolution suggestions
//   - Kafka Connect REST client for connector management
package kafka
