//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestAnalyzeDLQCharacterization(t *testing.T) {
	m := contractManager(t)
	topic := createCharacterizationTopic(t, m, "dlq", 1)
	baseTime := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)

	produceCharacterizationRecords(t, m,
		dlqRecord(topic, baseTime, map[string]string{
			"error.type":   "primary",
			"x-error-type": "secondary",
			"error_type":   "tertiary",
		}),
		dlqRecord(topic, baseTime.Add(time.Second), map[string]string{
			"x-error-type": "secondary",
			"error_type":   "tertiary",
		}),
		dlqRecord(topic, baseTime.Add(2*time.Second), map[string]string{
			"error_type": "tertiary",
		}),
		dlqRecord(topic, baseTime.Add(3*time.Second), map[string]string{
			"error.message": "failed",
		}),
		dlqRecord(topic, baseTime.Add(4*time.Second), map[string]string{
			"x-error-message": "failed",
		}),
		dlqRecord(topic, baseTime.Add(5*time.Second), nil),
	)

	result, err := m.AnalyzeDLQ(context.Background(), "contract", topic, 6)
	if err != nil {
		t.Fatalf("AnalyzeDLQ: %v", err)
	}
	if result.TotalMessages != 6 {
		t.Fatalf("AnalyzeDLQ total = %d, want 6", result.TotalMessages)
	}
	wantErrorTypes := map[string]int{
		"primary":          1,
		"secondary":        1,
		"tertiary":         1,
		"processing_error": 2,
		"unknown":          1,
	}
	if len(result.ErrorTypes) != len(wantErrorTypes) {
		t.Fatalf("AnalyzeDLQ error types = %#v, want %#v", result.ErrorTypes, wantErrorTypes)
	}
	for errorType, want := range wantErrorTypes {
		if got := result.ErrorTypes[errorType]; got != want {
			t.Fatalf("AnalyzeDLQ error type %q = %d, want %d", errorType, got, want)
		}
	}
	if result.TimeRange.Earliest != baseTime.Format("2006-01-02T15:04:05.000Z") {
		t.Fatalf("AnalyzeDLQ earliest = %q, want %q",
			result.TimeRange.Earliest, baseTime.Format("2006-01-02T15:04:05.000Z"))
	}
	if result.TimeRange.Latest != baseTime.Add(5*time.Second).Format("2006-01-02T15:04:05.000Z") {
		t.Fatalf("AnalyzeDLQ latest = %q, want %q",
			result.TimeRange.Latest, baseTime.Add(5*time.Second).Format("2006-01-02T15:04:05.000Z"))
	}
	if len(result.Samples) != 5 {
		t.Fatalf("AnalyzeDLQ samples = %d, want current cap of 5", len(result.Samples))
	}
	for i, sample := range result.Samples {
		if sample.Offset != int64(i) {
			t.Fatalf("AnalyzeDLQ sample %d offset = %d, want %d", i, sample.Offset, i)
		}
	}
}

func dlqRecord(topic string, timestamp time.Time, headers map[string]string) *kgo.Record {
	record := &kgo.Record{
		Topic:     topic,
		Partition: 0,
		Value:     []byte(`{"payload":"failed"}`),
		Timestamp: timestamp,
	}
	for name, value := range headers {
		record.Headers = append(record.Headers, kgo.RecordHeader{Key: name, Value: []byte(value)})
	}
	return record
}
