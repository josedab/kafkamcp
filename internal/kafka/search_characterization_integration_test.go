//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestSearchMessagesCharacterization(t *testing.T) {
	m := contractManager(t)
	topic := createCharacterizationTopic(t, m, "search", 1)
	baseTime := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)

	produceCharacterizationRecords(t, m,
		searchRecord(topic, "order-1", `{"status":"failed","reason":"timeout"}`, baseTime,
			map[string]string{"env": "prod"}),
		searchRecord(topic, "order-2", `{"status":"success"}`, baseTime.Add(time.Second),
			map[string]string{"env": "staging"}),
		searchRecord(topic, "payment-1", `{"status":"failed","reason":"auth"}`, baseTime.Add(2*time.Second),
			map[string]string{"env": "prod"}),
		searchRecord(topic, "order-3", `{"status":"failed","reason":"validation"}`, baseTime.Add(3*time.Second), nil),
		searchRecord(topic, "order-4", `{"status":"failed","reason":"quota"}`, baseTime.Add(4*time.Second),
			map[string]string{"env": "prod"}),
	)

	t.Run("combined filters", func(t *testing.T) {
		messages, scanned, err := m.SearchMessages(context.Background(), "contract", SearchRequest{
			Topic:         topic,
			KeyFilter:     "order",
			HeaderKey:     "env",
			HeaderValue:   "prod",
			ValueContains: "failed",
			MaxScan:       5,
			Limit:         10,
		})
		if err != nil {
			t.Fatalf("SearchMessages combined filters: %v", err)
		}
		if scanned != 5 {
			t.Fatalf("SearchMessages scanned = %d, want 5", scanned)
		}
		if len(messages) != 2 || messages[0].Offset != 0 || messages[1].Offset != 4 {
			t.Fatalf("SearchMessages combined filters = %#v, want offsets 0 and 4", messages)
		}
	})

	t.Run("limit counts the first unprocessed record as scanned", func(t *testing.T) {
		messages, scanned, err := m.SearchMessages(context.Background(), "contract", SearchRequest{
			Topic:         topic,
			ValueContains: "failed",
			MaxScan:       5,
			Limit:         2,
		})
		if err != nil {
			t.Fatalf("SearchMessages limit: %v", err)
		}
		if len(messages) != 2 || messages[0].Offset != 0 || messages[1].Offset != 2 {
			t.Fatalf("SearchMessages limited results = %#v, want offsets 0 and 2", messages)
		}
		if scanned != 4 {
			t.Fatalf("SearchMessages limited scanned = %d, want current behavior of 4", scanned)
		}
	})

	t.Run("max scan bounds returned input", func(t *testing.T) {
		messages, scanned, err := m.SearchMessages(context.Background(), "contract", SearchRequest{
			Topic:   topic,
			MaxScan: 3,
			Limit:   10,
		})
		if err != nil {
			t.Fatalf("SearchMessages max scan: %v", err)
		}
		if len(messages) != 3 || scanned != 3 {
			t.Fatalf("SearchMessages max scan returned %d messages and scanned %d, want 3 and 3",
				len(messages), scanned)
		}
	})

	t.Run("end time filters after the cutoff", func(t *testing.T) {
		messages, scanned, err := m.SearchMessages(context.Background(), "contract", SearchRequest{
			Topic:   topic,
			EndTime: baseTime.Add(1500 * time.Millisecond).Format(time.RFC3339Nano),
			MaxScan: 5,
			Limit:   10,
		})
		if err != nil {
			t.Fatalf("SearchMessages end time: %v", err)
		}
		if scanned != 5 || len(messages) != 2 || messages[0].Offset != 0 || messages[1].Offset != 1 {
			t.Fatalf("SearchMessages end time returned %#v and scanned %d, want offsets 0 and 1 with 5 scanned",
				messages, scanned)
		}
	})

	t.Run("invalid end time is ignored", func(t *testing.T) {
		messages, scanned, err := m.SearchMessages(context.Background(), "contract", SearchRequest{
			Topic:   topic,
			EndTime: "not-a-time",
			MaxScan: 5,
			Limit:   10,
		})
		if err != nil {
			t.Fatalf("SearchMessages invalid end time: %v", err)
		}
		if len(messages) != 5 || scanned != 5 {
			t.Fatalf("SearchMessages invalid end time returned %d messages and scanned %d, want 5 and 5",
				len(messages), scanned)
		}
	})

	t.Run("invalid start time falls back to the end", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		messages, scanned, err := m.SearchMessages(ctx, "contract", SearchRequest{
			Topic:     topic,
			StartTime: "not-a-time",
			MaxScan:   1,
			Limit:     10,
		})
		if err != nil {
			t.Fatalf("SearchMessages invalid start time: %v", err)
		}
		if len(messages) != 0 || scanned != 0 {
			t.Fatalf("SearchMessages invalid start time returned %d messages and scanned %d, want 0 and 0",
				len(messages), scanned)
		}
	})
}

func searchRecord(topic, key, value string, timestamp time.Time, headers map[string]string) *kgo.Record {
	record := &kgo.Record{
		Topic:     topic,
		Partition: 0,
		Key:       []byte(key),
		Value:     []byte(value),
		Timestamp: timestamp,
	}
	for name, value := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: name, Value: []byte(value)})
	}
	return record
}
