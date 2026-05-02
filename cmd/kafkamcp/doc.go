// Package main is the entry point for the kafkamcp binary.
//
// It configures and starts the KafkaMCP server — an MCP (Model Context Protocol)
// server that exposes Apache Kafka topics, consumer groups, schemas, and cluster
// metadata as MCP tools and resources for AI agents.
//
// Usage:
//
//	kafkamcp --config kafkamcp.yaml
//	kafkamcp --config kafkamcp.yaml --log-level debug
//	kafkamcp version
package main
