// Package streamsql provides a streaming SQL query engine for Kafka topics.
// It parses SQL-like queries with WHERE clauses, windowed aggregations, and
// GROUP BY support, executes them against live topic data, and generates
// query execution plans for optimization.
package streamsql
