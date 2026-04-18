package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
)

// --------------- generateSampleData ---------------

func TestGenerateSampleData_Count(t *testing.T) {
	for _, count := range []int{0, 1, 5, 20} {
		msgs := generateSampleData("ecommerce", count)
		if len(msgs) != count {
			t.Errorf("generateSampleData(ecommerce, %d) returned %d messages", count, len(msgs))
		}
	}
}

func TestGenerateSampleData_Ecommerce(t *testing.T) {
	msgs := generateSampleData("ecommerce", 10)
	for i, msg := range msgs {
		assertKeyNotEmpty(t, msg, i)
		m := unmarshalValue(t, msg, i)
		for _, key := range []string{"order_id", "product", "price"} {
			if _, ok := m[key]; !ok {
				t.Errorf("ecommerce msg[%d] missing key %q", i, key)
			}
		}
	}
}

func TestGenerateSampleData_IOT(t *testing.T) {
	msgs := generateSampleData("iot", 10)
	for i, msg := range msgs {
		assertKeyNotEmpty(t, msg, i)
		m := unmarshalValue(t, msg, i)
		for _, key := range []string{"device_id", "temperature", "humidity"} {
			if _, ok := m[key]; !ok {
				t.Errorf("iot msg[%d] missing key %q", i, key)
			}
		}
	}
}

func TestGenerateSampleData_UserActivity(t *testing.T) {
	msgs := generateSampleData("user_activity", 10)
	for i, msg := range msgs {
		assertKeyNotEmpty(t, msg, i)
		m := unmarshalValue(t, msg, i)
		for _, key := range []string{"user_id", "action"} {
			if _, ok := m[key]; !ok {
				t.Errorf("user_activity msg[%d] missing key %q", i, key)
			}
		}
	}
}

func TestGenerateSampleData_Logs(t *testing.T) {
	msgs := generateSampleData("logs", 10)
	for i, msg := range msgs {
		assertKeyNotEmpty(t, msg, i)
		m := unmarshalValue(t, msg, i)
		for _, key := range []string{"level", "service", "message"} {
			if _, ok := m[key]; !ok {
				t.Errorf("logs msg[%d] missing key %q", i, key)
			}
		}
	}
}

func TestGenerateSampleData_DefaultIsEcommerce(t *testing.T) {
	msgs := generateSampleData("unknown_pattern", 5)
	for i, msg := range msgs {
		m := unmarshalValue(t, msg, i)
		if _, ok := m["order_id"]; !ok {
			t.Errorf("default pattern msg[%d] missing order_id; expected ecommerce fallback", i)
		}
	}
}

// --------------- Playground topic naming ---------------

func TestPlaygroundTopicNaming(t *testing.T) {
	expected := fmt.Sprintf("%s%s_%s", playgroundPrefix, "agent1", "test-topic")
	want := "_playground_agent1_test-topic"
	if expected != want {
		t.Errorf("topic naming = %q, want %q", expected, want)
	}
}

// --------------- PlaygroundCreate error path ---------------

func TestPlaygroundCreate_ClusterNotFound(t *testing.T) {
	m := testManager()
	_, err := m.PlaygroundCreate(context.Background(), "nonexistent", "agent1", "test", 3)
	if err == nil {
		t.Fatal("PlaygroundCreate with nonexistent cluster should return error")
	}
}

func TestPlaygroundCreate_DefaultPartitions(t *testing.T) {
	m := testManager()
	// Even though it will fail (no real broker), partitions <= 0 path is exercised.
	_, err := m.PlaygroundCreate(context.Background(), "prod", "agent1", "test", 0)
	if err == nil {
		t.Fatal("PlaygroundCreate with no broker should return error")
	}
}

// --------------- PlaygroundList error path ---------------

func TestPlaygroundList_ClusterNotFound(t *testing.T) {
	m := testManager()
	_, err := m.PlaygroundList(context.Background(), "nonexistent", "agent1")
	if err == nil {
		t.Fatal("PlaygroundList with nonexistent cluster should return error")
	}
}

func TestPlaygroundList_NoBroker(t *testing.T) {
	m := testManager()
	_, err := m.PlaygroundList(context.Background(), "prod", "agent1")
	if err == nil {
		t.Fatal("PlaygroundList with no broker should return error")
	}
}

// --------------- PlaygroundCleanup error path ---------------

func TestPlaygroundCleanup_ClusterNotFound(t *testing.T) {
	m := testManager()
	_, err := m.PlaygroundCleanup(context.Background(), "nonexistent", "agent1")
	if err == nil {
		t.Fatal("PlaygroundCleanup with nonexistent cluster should return error")
	}
}

func TestPlaygroundCleanup_NoBroker(t *testing.T) {
	m := testManager()
	_, err := m.PlaygroundCleanup(context.Background(), "prod", "agent1")
	if err == nil {
		t.Fatal("PlaygroundCleanup with no broker should return error")
	}
}

// --------------- helpers ---------------

func assertKeyNotEmpty(t *testing.T, msg ProduceMessage, idx int) {
	t.Helper()
	if msg.Key == "" {
		t.Errorf("msg[%d].Key is empty", idx)
	}
}

func unmarshalValue(t *testing.T, msg ProduceMessage, idx int) map[string]any {
	t.Helper()
	raw, ok := msg.Value.(json.RawMessage)
	if !ok {
		t.Fatalf("msg[%d].Value is not json.RawMessage, got %T", idx, msg.Value)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("msg[%d] unmarshal error: %v", idx, err)
	}
	return m
}
