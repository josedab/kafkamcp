// Package auth implements the per-agent authorization policy engine for KafkaMCP.
//
// It evaluates whether an MCP agent is allowed to perform a given action on a
// resource (topic, consumer group, schema, or cluster) based on glob-pattern
// rules defined in the KafkaMCP configuration file. When default_deny is
// enabled, all requests are rejected unless an explicit policy grants access.
package auth
