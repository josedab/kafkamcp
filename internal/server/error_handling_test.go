package server

import (
	"errors"
	"fmt"
	"testing"
)

func TestToolError(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	tests := []struct {
		name string
		msg  string
		err  error
	}{
		{"with error", "something failed", errors.New("connection refused")},
		{"nil error", "something failed", nil},
		{"wrapped error", "something failed", fmt.Errorf("outer: %w", errors.New("inner"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := s.toolError(tt.msg, tt.err)
			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if !result.IsError {
				t.Error("expected IsError=true")
			}
			if len(result.Content) == 0 {
				t.Fatal("expected at least one content item")
			}
		})
	}
}

func TestToolErrorWithContext(t *testing.T) {
	t.Parallel()
	s := newTestServer(t)

	tests := []struct {
		name      string
		msg       string
		err       error
		toolName  string
		requestID string
	}{
		{"with context", "something failed", errors.New("timeout"), "list_topics", "req-123"},
		{"nil error", "something failed", nil, "list_topics", "req-456"},
		{"empty tool and request", "something failed", errors.New("err"), "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := s.toolErrorWithContext(tt.msg, tt.err, tt.toolName, tt.requestID)
			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if !result.IsError {
				t.Error("expected IsError=true")
			}
			if len(result.Content) == 0 {
				t.Fatal("expected at least one content item")
			}
		})
	}
}

func TestValidateKafkaIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		field   string
		value   string
		wantErr bool
	}{
		{"empty value", "topic", "", false},
		{"simple valid", "topic", "test", false},
		{"valid with special chars", "topic", "my-topic.v1_test", false},
		{"hyphen and dots", "topic", "my.topic-name", false},
		{"invalid special chars", "topic", "topic@#!", true},
		{"reserved prefix", "topic", "__consumer_offsets", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := validateKafkaIdentifier(tt.field, tt.value)
			if tt.wantErr && result == nil {
				t.Error("expected error result, got nil")
			}
			if !tt.wantErr && result != nil {
				t.Errorf("expected nil, got error result")
			}
		})
	}
}

func TestValidateEnum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		field   string
		value   string
		allowed map[string]bool
		wantErr bool
	}{
		{"valid value", "format", "json", map[string]bool{"json": true, "avro": true}, false},
		{"invalid value", "format", "xml", map[string]bool{"json": true, "avro": true}, true},
		{"case sensitive", "format", "JSON", map[string]bool{"json": true}, true},
		{"empty allowed map", "format", "any", map[string]bool{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := validateEnum(tt.field, tt.value, tt.allowed)
			if tt.wantErr && result == nil {
				t.Error("expected error result, got nil")
			}
			if !tt.wantErr && result != nil {
				t.Errorf("expected nil, got error result")
			}
		})
	}
}

func TestRedactURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"empty string", "", ""},
		{"with path", "http://host:9092/path/to/resource", "http://host:9092/***"},
		{"with password", "http://user:pass@host:9092/path", "http://user:pass@host:9092/***"},
		{"with query params", "http://host:9092/path?token=secret", "http://host:9092/***"},
		{"without path", "http://host:9092", "http://host:9092/***"},
		{"no scheme", "just-a-string", "***"},
		{"https", "https://host/api/v1", "https://host/***"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := redactURL(tt.raw)
			if got != tt.want {
				t.Errorf("redactURL(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}
