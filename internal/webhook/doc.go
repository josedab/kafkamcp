// Package webhook provides a webhook event gateway for dispatching Kafka
// events to registered HTTP endpoints, with HMAC-SHA256 signing, automatic
// retries with exponential backoff, and per-subscription delivery history.
package webhook
