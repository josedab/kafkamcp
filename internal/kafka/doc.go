// Package kafka contains the core business logic for Kafka operations in KafkaMCP.
//
// It provides a Manager built on franz-go clients and higher-level operations
// used by the MCP server:
//
//   - Cluster metadata retrieval and multi-cluster routing
//   - Topic CRUD (list, describe, create, alter, delete)
//   - JSON/string message production with delivery metadata
//   - Bounded message consumption and search with configurable positioning
//   - Consumer group inspection and offset reset
//   - Dead-letter queue (DLQ) analysis
//   - Kafka ACL listing, creation, and deletion
//   - Kafka Connect REST client for connector management
package kafka
