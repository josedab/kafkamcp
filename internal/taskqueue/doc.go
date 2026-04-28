// Package taskqueue provides a distributed task queue built on Kafka topics.
// It supports publishing tasks, claiming them with lease-based ownership,
// heartbeat-driven liveness checks, automatic reassignment of stale tasks,
// and per-queue statistics.
package taskqueue
