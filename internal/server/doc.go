// Package server implements the MCP (Model Context Protocol) server for KafkaMCP.
//
// It registers and dispatches MCP tool calls and resource reads, wiring each
// request through rate limiting, authorization, audit logging, and metrics
// collection before delegating to the kafka, schema, or connect layers.
//
// Tool handlers are organized by priority tier:
//   - P0 (tools_p0.go): core read operations — list topics, describe topic,
//     consume, produce, schemas, consumer groups.
//   - P1 (tools_p1.go): administrative and search operations — create/alter/delete
//     topic, search messages, reset offsets, schema compatibility, DLQ analysis,
//     subscribe.
//   - P2 (tools_p2.go): advanced analytics and connectors — aggregate, join,
//     natural-language query, schema evolution, Kafka Connect integration.
//
// Resource handlers (resources.go) expose read-only MCP resources such as
// cluster metadata, topic catalogs, schema catalogs, consumer group lists,
// audit logs, and a topic dependency graph.
package server
