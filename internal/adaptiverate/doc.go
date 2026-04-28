// Package adaptiverate provides an adaptive rate limiting engine that
// adjusts per-agent quotas based on observed request patterns. It tracks
// agent profiles, records request samples, and dynamically tunes
// requests-per-minute limits to balance fairness and throughput.
package adaptiverate
