package nlquery

import (
	"log/slog"
	"testing"
)

func TestParseOperational_CreateTopic(t *testing.T) {
	e := NewEngine(slog.Default())

	cmd, err := e.ParseOperational("create topic orders with 6 partitions")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Action != "create_topic" {
		t.Errorf("expected create_topic, got %s", cmd.Action)
	}
	if cmd.Name != "orders" {
		t.Errorf("expected orders, got %s", cmd.Name)
	}
	if cmd.Parameters["partitions"] != 6 {
		t.Errorf("expected 6 partitions, got %v", cmd.Parameters["partitions"])
	}
}

func TestParseOperational_DeleteTopic(t *testing.T) {
	e := NewEngine(slog.Default())

	cmd, err := e.ParseOperational("delete topic test-data")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Action != "delete_topic" || cmd.Name != "test-data" {
		t.Errorf("unexpected: action=%s name=%s", cmd.Action, cmd.Name)
	}
}

func TestParseOperational_ListTopics(t *testing.T) {
	e := NewEngine(slog.Default())

	cmd, err := e.ParseOperational("list all topics")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Action != "list_topics" {
		t.Errorf("expected list_topics, got %s", cmd.Action)
	}
}

func TestParseOperational_ResetOffsets(t *testing.T) {
	e := NewEngine(slog.Default())

	cmd, err := e.ParseOperational("reset offsets for my-consumers to earliest")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Action != "reset_offsets" {
		t.Errorf("expected reset_offsets, got %s", cmd.Action)
	}
	if cmd.Name != "my-consumers" {
		t.Errorf("expected my-consumers, got %s", cmd.Name)
	}
}

func TestParseOperational_DescribeGroup(t *testing.T) {
	e := NewEngine(slog.Default())

	cmd, err := e.ParseOperational("describe consumer group my-group")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Action != "describe_group" {
		t.Errorf("expected describe_group, got %s", cmd.Action)
	}
	if cmd.Name != "my-group" {
		t.Errorf("expected my-group, got %s", cmd.Name)
	}
}

func TestParseOperational_ClusterExtraction(t *testing.T) {
	e := NewEngine(slog.Default())

	cmd, err := e.ParseOperational("list all topics on cluster production")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Cluster != "production" {
		t.Errorf("expected cluster=production, got %s", cmd.Cluster)
	}
}

func TestParseOperational_EmptyInput(t *testing.T) {
	e := NewEngine(slog.Default())

	_, err := e.ParseOperational("")
	if err == nil {
		t.Error("expected error for empty input")
	}
}

func TestParseOperational_UnparsableInput(t *testing.T) {
	e := NewEngine(slog.Default())

	_, err := e.ParseOperational("what is the weather today")
	if err == nil {
		t.Error("expected error for unparsable input")
	}
}

func TestParseOperational_KeywordFallback(t *testing.T) {
	e := NewEngine(slog.Default())

	// This should fall through regex patterns and hit keyword detection
	cmd, err := e.ParseOperational("please create a topic called my-events")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Action != "create_topic" {
		t.Errorf("expected create_topic from keyword fallback, got %s", cmd.Action)
	}
}
