package kafka

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// tryParseJSON decodes message bytes according to the specified format.
// Supported formats: "auto" (JSON then string), "json", "string", "bytes" (base64).
func decodeValue(data []byte, format string) any {
	if len(data) == 0 {
		return nil
	}

	switch format {
	case "bytes":
		return base64.StdEncoding.EncodeToString(data)
	case "string":
		return string(data)
	case "json":
		var result any
		if err := json.Unmarshal(data, &result); err == nil {
			return result
		}
		return string(data)
	default: // "auto", "avro", "protobuf" — try JSON, fallback to string
		var result any
		if err := json.Unmarshal(data, &result); err == nil {
			return result
		}
		return string(data)
	}
}

// serializeValue converts a value to a string suitable for Kafka.
func serializeValue(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	default:
		data, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(data)
	}
}

// java_timeout converts milliseconds to a Go duration.
func java_timeout(ms int) time.Duration {
	if ms <= 0 {
		ms = 5000
	}
	return time.Duration(ms) * time.Millisecond
}

// parseTimestamp tries to parse a string as ISO 8601 timestamp.
func parseTimestamp(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse %q as timestamp", s)
}
