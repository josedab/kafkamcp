// Package perfguard provides automated performance regression detection for
// Kafka operations. It stores baselines, detects degradation via statistical
// change-point detection (CUSUM/PELT), and auto-throttles misbehaving agents.
package perfguard
