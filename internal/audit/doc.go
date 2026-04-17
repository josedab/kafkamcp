// Package audit provides a structured JSON audit logger for KafkaMCP.
//
// It records every MCP tool call and resource read with agent identity,
// action, outcome, timing, and item counts. Entries are held in an in-memory
// ring buffer and optionally written to a JSONL file for durable storage.
package audit
