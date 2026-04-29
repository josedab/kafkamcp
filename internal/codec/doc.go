// Package codec provides Avro and Protobuf wire-format serialization
// compatible with Confluent Schema Registry (magic byte 0x00 + 4-byte schema ID + payload).
// It supports auto-detection of wire format with JSON fallback.
package codec
