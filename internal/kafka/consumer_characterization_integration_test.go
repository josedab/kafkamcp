//go:build integration

package kafka

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumeCharacterization_OffsetsHeadersAndDeserialization(t *testing.T) {
	m := contractManager(t)
	topic := createCharacterizationTopic(t, m, "consume-offsets", 1)

	baseTime := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	produceCharacterizationRecords(t, m,
		&kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte("json-key"),
			Value:     []byte(`{"sequence":0}`),
			Timestamp: baseTime,
			Headers: []kgo.RecordHeader{
				{Key: "trace-id", Value: []byte("trace-0")},
			},
		},
		&kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte("plain-key"),
			Value:     []byte("plain-text"),
			Timestamp: baseTime.Add(time.Second),
		},
		&kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte("latest-key"),
			Value:     []byte(`{"sequence":2}`),
			Timestamp: baseTime.Add(2 * time.Second),
		},
	)

	t.Run("earliest auto decoding preserves headers", func(t *testing.T) {
		messages, err := m.Consume(context.Background(), "contract", topic, 0, "earliest", 3, 1000, "auto")
		if err != nil {
			t.Fatalf("Consume earliest: %v", err)
		}
		if len(messages) != 3 {
			t.Fatalf("Consume earliest returned %d messages, want 3", len(messages))
		}
		if messages[0].Offset != 0 || messages[1].Offset != 1 || messages[2].Offset != 2 {
			t.Fatalf("Consume earliest offsets = [%d %d %d], want [0 1 2]",
				messages[0].Offset, messages[1].Offset, messages[2].Offset)
		}
		value, ok := messages[0].Value.(map[string]any)
		if !ok || value["sequence"] != float64(0) {
			t.Fatalf("Consume auto decoded first value = %#v, want JSON object", messages[0].Value)
		}
		if messages[1].Value != "plain-text" {
			t.Fatalf("Consume auto decoded second value = %#v, want plain-text", messages[1].Value)
		}
		if messages[0].Headers["trace-id"] != "trace-0" {
			t.Fatalf("Consume header trace-id = %q, want trace-0", messages[0].Headers["trace-id"])
		}
	})

	t.Run("latest returns the final offset", func(t *testing.T) {
		messages, err := m.Consume(context.Background(), "contract", topic, 0, "latest", 1, 1000, "auto")
		if err != nil {
			t.Fatalf("Consume latest: %v", err)
		}
		if len(messages) != 1 || messages[0].Offset != 2 {
			t.Fatalf("Consume latest = %#v, want only offset 2", messages)
		}
	})

	t.Run("numeric offset starts exactly at the requested offset", func(t *testing.T) {
		messages, err := m.Consume(context.Background(), "contract", topic, 0, "1", 2, 1000, "string")
		if err != nil {
			t.Fatalf("Consume numeric offset: %v", err)
		}
		if len(messages) != 2 || messages[0].Offset != 1 || messages[1].Offset != 2 {
			t.Fatalf("Consume numeric offsets = %#v, want offsets 1 and 2", messages)
		}
	})

	t.Run("timestamp starts at the first record at or after the timestamp", func(t *testing.T) {
		offset := baseTime.Add(1500 * time.Millisecond).Format(time.RFC3339Nano)
		messages, err := m.Consume(context.Background(), "contract", topic, 0, offset, 2, 1000, "auto")
		if err != nil {
			t.Fatalf("Consume timestamp offset: %v", err)
		}
		if len(messages) != 1 || messages[0].Offset != 2 {
			t.Fatalf("Consume timestamp = %#v, want only offset 2", messages)
		}
	})

	t.Run("bytes deserializer base64 encodes keys and values", func(t *testing.T) {
		messages, err := m.Consume(context.Background(), "contract", topic, 0, "earliest", 1, 1000, "bytes")
		if err != nil {
			t.Fatalf("Consume bytes: %v", err)
		}
		if len(messages) != 1 {
			t.Fatalf("Consume bytes returned %d messages, want 1", len(messages))
		}
		if messages[0].Key != base64.StdEncoding.EncodeToString([]byte("json-key")) {
			t.Fatalf("Consume bytes key = %#v", messages[0].Key)
		}
		if messages[0].Value != base64.StdEncoding.EncodeToString([]byte(`{"sequence":0}`)) {
			t.Fatalf("Consume bytes value = %#v", messages[0].Value)
		}
	})
}

func TestConsumeCharacterization_MultiPartitionLatestLimit(t *testing.T) {
	m := contractManager(t)
	topic := createCharacterizationTopic(t, m, "consume-partitions", 3)

	var records []*kgo.Record
	for partition := int32(0); partition < 3; partition++ {
		for offset := 0; offset < 3; offset++ {
			records = append(records, &kgo.Record{
				Topic:     topic,
				Partition: partition,
				Key:       []byte("key"),
				Value:     []byte("value"),
			})
		}
	}
	produceCharacterizationRecords(t, m, records...)

	messages, err := m.Consume(context.Background(), "contract", topic, -1, "latest", 3, 1000, "string")
	if err != nil {
		t.Fatalf("Consume latest across partitions: %v", err)
	}
	if len(messages) != 3 {
		t.Fatalf("Consume latest across partitions returned %d messages, want 3", len(messages))
	}
	for _, message := range messages {
		if message.Offset < 1 {
			t.Fatalf("Consume latest returned offset %d; current per-partition limit should start at offset 1", message.Offset)
		}
	}
}

func TestConsumeCharacterization_EmptyTopicStopsAtTimeout(t *testing.T) {
	m := contractManager(t)
	topic := createCharacterizationTopic(t, m, "consume-timeout", 1)

	start := time.Now()
	messages, err := m.Consume(context.Background(), "contract", topic, 0, "earliest", 1, 75, "auto")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Consume empty topic: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("Consume empty topic returned %d messages, want 0", len(messages))
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Consume empty topic took %s, want bounded timeout behavior", elapsed)
	}
}

func createCharacterizationTopic(t *testing.T, m *Manager, suffix string, partitions int32) string {
	t.Helper()
	topic := contractTopic(t, suffix)
	t.Cleanup(func() { cleanupTopic(t, m, "contract", topic) })
	if err := m.CreateTopic(context.Background(), "contract", CreateTopicRequest{
		Name:              topic,
		Partitions:        partitions,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	waitForTopic(t, m, "contract", topic, int(partitions))
	return topic
}

func produceCharacterizationRecords(t *testing.T, m *Manager, records ...*kgo.Record) {
	t.Helper()
	client, err := m.GetClient("contract")
	if err != nil {
		t.Fatalf("GetClient: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}
}
