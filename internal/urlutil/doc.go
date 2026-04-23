// Package urlutil provides URL validation utilities for KafkaMCP.
//
// It validates webhook destination URLs, rejecting private/internal IP ranges
// and non-HTTPS schemes to prevent SSRF and insecure callback targets.
package urlutil
