// Package kafkamcpsdk provides a Go client SDK for interacting with KafkaMCP
// servers programmatically from Go applications and AI agents.
//
// The SDK exposes a high-level [Client] that communicates with a KafkaMCP server
// over any MCP transport (stdio, SSE, or streamable-http). It supports listing
// topics, consuming and producing messages, searching, watching for changes, and
// querying cluster metadata.
//
// Basic usage:
//
//	client := kafkamcpsdk.New(transport, kafkamcpsdk.WithTimeout(30*time.Second))
//	topics, err := client.ListTopics(ctx, "orders.*")
package kafkamcpsdk
