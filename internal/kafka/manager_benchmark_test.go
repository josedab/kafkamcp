package kafka

import (
	"encoding/json"
	"testing"
)

// BenchmarkDecodeValueAuto benchmarks auto-decoding of JSON message values.
func BenchmarkDecodeValueAuto(b *testing.B) {
	data := []byte(`{"orderId":"abc-123","amount":99.95,"items":["widget","gadget"]}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = decodeValue(data, "auto")
	}
}

// BenchmarkDecodeValueString benchmarks string decoding of message values.
func BenchmarkDecodeValueString(b *testing.B) {
	data := []byte("plain text message value")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = decodeValue(data, "string")
	}
}

// BenchmarkDecodeValueBytes benchmarks base64 decoding of binary message values.
func BenchmarkDecodeValueBytes(b *testing.B) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = decodeValue(data, "bytes")
	}
}

// BenchmarkSerializeValueJSON benchmarks JSON serialization of produce message values.
func BenchmarkSerializeValueJSON(b *testing.B) {
	value := map[string]any{
		"orderId": "abc-123",
		"amount":  99.95,
		"items":   []string{"widget", "gadget"},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = serializeValue(value)
	}
}

// BenchmarkSerializeValueString benchmarks string serialization of produce message values.
func BenchmarkSerializeValueString(b *testing.B) {
	value := "plain text message"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = serializeValue(value)
	}
}

// BenchmarkProduceMessageMarshal benchmarks marshaling ProduceMessage structs to JSON.
func BenchmarkProduceMessageMarshal(b *testing.B) {
	partition := int32(0)
	msg := ProduceMessage{
		Key:   "order-123",
		Value: map[string]any{"amount": 99.95, "currency": "USD"},
		Headers: map[string]string{
			"correlation-id": "trace-abc",
			"content-type":   "application/json",
		},
		Partition: &partition,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(msg)
	}
}

// BenchmarkConsumeMessageBuild benchmarks constructing Message structs from raw data,
// simulating the hot path inside Consume.
func BenchmarkConsumeMessageBuild(b *testing.B) {
	key := []byte("order-123")
	value := []byte(`{"amount":99.95}`)
	rawHeaders := []struct {
		Key   string
		Value []byte
	}{
		{"correlation-id", []byte("trace-abc")},
		{"content-type", []byte("application/json")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		headers := make(map[string]string, len(rawHeaders))
		for _, h := range rawHeaders {
			headers[h.Key] = string(h.Value)
		}
		_ = Message{
			Partition: 0,
			Offset:    int64(i),
			Timestamp: "2025-01-01T00:00:00.000Z",
			Key:       decodeValue(key, "auto"),
			Value:     decodeValue(value, "auto"),
			Headers:   headers,
		}
	}
}

// BenchmarkSearchMessageMatch benchmarks the message matching logic used in SearchMessages.
func BenchmarkSearchMessageMatch(b *testing.B) {
	msg := SearchMessage{
		Key:   "order-123",
		Value: map[string]any{"status": "failed", "error": "timeout"},
		Headers: map[string]string{
			"correlation-id": "trace-abc",
		},
	}
	keyFilter := "order"
	valueContains := "failed"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate key filter
		keyStr, _ := msg.Key.(string)
		_ = len(keyFilter) == 0 || benchContains(keyStr, keyFilter)

		// Simulate value contains
		valBytes, _ := json.Marshal(msg.Value)
		_ = len(valueContains) == 0 || benchContains(string(valBytes), valueContains)
	}
}

func benchContains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
