// Package server implements the MCP (Model Context Protocol) server for KafkaMCP.
//
// It registers and dispatches MCP tool calls and resource reads, wiring each
// request through rate limiting, authorization, audit logging, and metrics
// collection before delegating to the kafka, schema, or connect layers.
//
// # Server Configuration
//
// The server is constructed with [New] and configured via functional [Option]
// values. Options inject supported dependencies such as audit logging, rate
// limiting, metrics, masking, approvals, persistence, and telemetry.
//
// # Supported Surface
//
// Tool handlers implement the 29-tool supported surface across the core-read,
// ops-write, and governance packs. They cover bounded Kafka reads, guarded
// topic/group/connector/ACL mutations, schema operations, lag and DLQ
// diagnosis, health and discovery, approvals, and policy evaluation.
//
// Resource handlers expose nine stable, read-only resources for cluster
// metadata, configured clusters, topics, schemas, consumer groups, and the
// redacted audit tail.
package server
