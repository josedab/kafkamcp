package kafka

import (
	"errors"
	"testing"
	"time"
)

func TestIsTransientConnectError(t *testing.T) {
	t.Parallel()
	transient := []string{
		"dial tcp 127.0.0.1:9092: connect: connection refused",
		"i/o timeout",
		"no such host",
		"context deadline exceeded while fetching metadata",
		"dial tcp: lookup foo: no such host",
		"broker not available",
	}
	for _, msg := range transient {
		if !isTransientConnectError(errors.New(msg)) {
			t.Errorf("expected transient: %q", msg)
		}
	}
	permanent := []string{
		"unauthorized",
		"unknown topic",
		"invalid SASL credentials",
	}
	for _, msg := range permanent {
		if isTransientConnectError(errors.New(msg)) {
			t.Errorf("expected NOT transient: %q", msg)
		}
	}
	if isTransientConnectError(nil) {
		t.Error("nil should not be transient")
	}
}

// TestConnectBackoffKnobs ensures the package-private knobs exist with
// reasonable defaults (regression for F16 — values must not be zero).
func TestConnectBackoffKnobs(t *testing.T) {
	t.Parallel()
	if connectInitialBackoff <= 0 {
		t.Errorf("connectInitialBackoff must be positive, got %v", connectInitialBackoff)
	}
	if connectMaxBackoff < connectInitialBackoff {
		t.Errorf("connectMaxBackoff (%v) must be >= initial (%v)", connectMaxBackoff, connectInitialBackoff)
	}
	if connectMaxAttempts < 2 {
		t.Errorf("connectMaxAttempts must be >= 2 for retry semantics, got %d", connectMaxAttempts)
	}
	// Sanity check on units: 1-second base backoff is reasonable.
	if connectInitialBackoff > time.Minute {
		t.Errorf("connectInitialBackoff %v looks like wrong units (expected sub-minute)", connectInitialBackoff)
	}
}
