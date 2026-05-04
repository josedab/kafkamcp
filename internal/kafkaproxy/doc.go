// Package kafkaproxy provides a thin proxy layer that translates standard Kafka
// protocol wire format to/from MCP tool calls, allowing existing Kafka clients
// (Java, Python, librdkafka) to transparently use KafkaMCP features (auth,
// audit, masking) without code changes.
package kafkaproxy
