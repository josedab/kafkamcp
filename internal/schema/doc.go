// Package schema provides a client for the Confluent Schema Registry with
// built-in TTL-based caching.
//
// It supports listing subjects, fetching schemas by subject and version,
// checking schema compatibility, and deserializing Confluent wire-format
// messages. The cache reduces Registry traffic for frequently accessed schemas.
package schema
