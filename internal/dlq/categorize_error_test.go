package dlq

import (
	"errors"
	"testing"
)

func TestCategorizeDLQError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"connection refused", errors.New("connection refused"), "connection_error"},
		{"connect error", errors.New("failed to connect to broker"), "connection_error"},
		{"context deadline exceeded", errors.New("context deadline exceeded"), "timeout_error"},
		{"timeout", errors.New("request timeout"), "timeout_error"},
		{"authentication failed", errors.New("authentication failed: bad credentials"), "authentication_error"},
		{"permission denied", errors.New("permission denied for topic"), "authentication_error"},
		{"json unmarshal", errors.New("json: cannot unmarshal string into Go value"), "serialization_error"},
		{"serialization error", errors.New("serialization error: invalid schema"), "serialization_error"},
		{"marshal error", errors.New("failed to marshal message"), "serialization_error"},
		{"failed to produce", errors.New("failed to produce message"), "produce_failed"},
		{"publish error", errors.New("publish error: broker unavailable"), "produce_failed"},
		{"unknown error", errors.New("some completely unknown error"), "produce_failed"},
		{"case insensitivity CONNECTION", errors.New("CONNECTION REFUSED"), "connection_error"},
		{"case insensitivity TIMEOUT", errors.New("DEADLINE exceeded"), "timeout_error"},
		{"case insensitivity AUTH", errors.New("AUTH failure"), "authentication_error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := categorizeDLQError(tt.err)
			if got != tt.want {
				t.Errorf("categorizeDLQError(%q) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}
