package main

import (
	"strings"
	"testing"
)

// TestValidatePrefix covers the safe-cleanup guard logic.
func TestValidatePrefix(t *testing.T) {
	cases := []struct {
		prefix  string
		wantErr bool
	}{
		{"incident-demo.", false},
		{"mytest.", false},
		{"demo-2024.", false},
		{"", true},                   // empty
		{".", true},                  // just a dot
		{"_", true},                  // too short / ambiguous
		{"ab", true},                 // too short (no dot, len<3)
		{"__consumer_offsets", true}, // reserved prefix
		{"connect-offsets.", true},   // reserved prefix
		{"schema-registry.", true},   // reserved prefix
		{"incident-demo", false},     // no trailing dot is fine (we append it in New)
	}
	for _, c := range cases {
		err := ValidatePrefix(c.prefix)
		if c.wantErr && err == nil {
			t.Errorf("ValidatePrefix(%q): expected error, got nil", c.prefix)
		}
		if !c.wantErr && err != nil {
			t.Errorf("ValidatePrefix(%q): unexpected error: %v", c.prefix, err)
		}
	}
}

// TestDemoTopicsPrefix verifies all topic names carry the prefix.
func TestDemoTopicsPrefix(t *testing.T) {
	prefix := "mytestprefix."
	topics := demoTopics(prefix)
	if len(topics) == 0 {
		t.Fatal("demoTopics returned no topics")
	}
	for _, topic := range topics {
		if !strings.HasPrefix(topic.Name, prefix) {
			t.Errorf("topic %q does not start with prefix %q", topic.Name, prefix)
		}
	}
}

// TestDemoTopicsDistinct verifies no duplicate topic names.
func TestDemoTopicsDistinct(t *testing.T) {
	topics := demoTopics("test.")
	seen := make(map[string]bool)
	for _, topic := range topics {
		if seen[topic.Name] {
			t.Errorf("duplicate topic name: %s", topic.Name)
		}
		seen[topic.Name] = true
	}
}

// TestDemoTopicsIncludeDLQ verifies DLQ topics are present.
func TestDemoTopicsIncludeDLQ(t *testing.T) {
	topics := demoTopics("demo.")
	dlqCount := 0
	for _, topic := range topics {
		if strings.HasSuffix(topic.Name, ".dlq") {
			dlqCount++
		}
	}
	if dlqCount == 0 {
		t.Error("no DLQ topics found in demoTopics")
	}
}

// TestDemoTopicsPartitions verifies partitions are positive.
func TestDemoTopicsPartitions(t *testing.T) {
	topics := demoTopics("demo.")
	for _, topic := range topics {
		if topic.Partitions <= 0 {
			t.Errorf("topic %q has non-positive partitions: %d", topic.Name, topic.Partitions)
		}
	}
}

// TestDemoTopicNamesPrefix verifies all names from demoTopicNames carry the prefix.
func TestDemoTopicNamesPrefix(t *testing.T) {
	prefix := "test-prefix."
	names := demoTopicNames(prefix)
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			t.Errorf("demoTopicName %q does not carry prefix %q", name, prefix)
		}
	}
}

// TestIsAlreadyExistsErr covers the idempotency helper.
func TestIsAlreadyExistsErr(t *testing.T) {
	cases := []struct {
		input    error
		expected bool
	}{
		{nil, false},
		{errorString("TOPIC_ALREADY_EXISTS"), true},
		{errorString("topic already exists"), true},
		{errorString("some other error"), false},
	}
	for _, c := range cases {
		got := isAlreadyExistsErr(c.input)
		if got != c.expected {
			t.Errorf("isAlreadyExistsErr(%v) = %v, want %v", c.input, got, c.expected)
		}
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }

// TestMakeRecord verifies the helper emits correctly structured records.
func TestMakeRecord(t *testing.T) {
	type payload struct {
		ID string `json:"id"`
	}
	rec := makeRecord("my-topic", "my-key", payload{ID: "abc"})
	if rec.Topic != "my-topic" {
		t.Errorf("topic: want 'my-topic', got %q", rec.Topic)
	}
	if string(rec.Key) != "my-key" {
		t.Errorf("key: want 'my-key', got %q", rec.Key)
	}
	if !strings.Contains(string(rec.Value), `"id":"abc"`) {
		t.Errorf("value: expected JSON with id field, got %s", rec.Value)
	}
	// Check headers exist
	if len(rec.Headers) == 0 {
		t.Error("expected default headers to be set")
	}
	foundContentType := false
	for _, h := range rec.Headers {
		if h.Key == "x-content-type" {
			foundContentType = true
		}
	}
	if !foundContentType {
		t.Error("expected x-content-type header in record")
	}
}

// TestCleanupPrefixGuard validates the cleanup safety check without a real Kafka.
// We test the ValidatePrefix guard specifically because the full Cleanup() method
// requires a broker connection.
func TestCleanupPrefixGuard(t *testing.T) {
	dangerousPrefixes := []string{"", ".", "_"}
	for _, p := range dangerousPrefixes {
		if err := ValidatePrefix(p); err == nil {
			t.Errorf("ValidatePrefix(%q): expected error for dangerous prefix, got nil", p)
		}
	}
}
