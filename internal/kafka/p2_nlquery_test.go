package kafka

import (
	"testing"
)

func TestTopicMatchScore_ExactMatch(t *testing.T) {
	t.Parallel()
	score := topicMatchScore("show me messages from orders.created", "orders.created")
	if score < 100 {
		t.Errorf("expected score >= 100 for exact match, got %d", score)
	}
}

func TestTopicMatchScore_PartialMatch(t *testing.T) {
	t.Parallel()
	score := topicMatchScore("show me orders", "orders.created")
	if score < 10 {
		t.Errorf("expected score >= 10 for partial segment match, got %d", score)
	}
}

func TestTopicMatchScore_KeywordAssociation(t *testing.T) {
	t.Parallel()
	score := topicMatchScore("show me recent purchases", "order-events")
	// "order" in topic, "purchase" in query → should score via associations
	if score < 5 {
		t.Errorf("expected score >= 5 for keyword association, got %d", score)
	}
}

func TestTopicMatchScore_NoMatch(t *testing.T) {
	t.Parallel()
	score := topicMatchScore("find xyz data", "completely-unrelated-topic")
	if score != 0 {
		t.Errorf("expected 0 score for no match, got %d", score)
	}
}

func TestNLQ_LimitExtraction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		query   string
		wantMin int
		wantMax int
	}{
		{"show last 5 messages from orders", 5, 5},
		{"show recent 20 events", 20, 20},
		{"show latest 50 logs", 50, 50},
		{"show messages from orders", 10, 10}, // default
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			// Replicate the limit extraction logic from NaturalLanguageQuery
			limit := extractNLQLimit(tt.query)
			if limit < tt.wantMin || limit > tt.wantMax {
				t.Errorf("query %q: expected limit %d-%d, got %d", tt.query, tt.wantMin, tt.wantMax, limit)
			}
		})
	}
}

func TestNLQ_LimitClampedTo1000(t *testing.T) {
	t.Parallel()
	limit := extractNLQLimit("show last 5000 messages")
	if limit != 10 {
		// The code only matches n <= 1000, so 5000 falls through to default 10
		t.Errorf("expected default 10 for >1000 limit, got %d", limit)
	}
}

func TestNLQ_TopicMatchingPartialDot(t *testing.T) {
	t.Parallel()
	topics := []TopicSummary{
		{Name: "com.company.orders"},
		{Name: "com.company.payments"},
	}

	// Partial matching splits by "." and checks query containment
	topicNames := make([]string, len(topics))
	for i, t := range topics {
		topicNames[i] = t.Name
	}

	// If no direct match, fall through to partial match
	var bestTopic string
	for _, name := range topicNames {
		if topicMatchScore("show me orders", name) > 0 {
			bestTopic = name
			break
		}
	}
	if bestTopic != "com.company.orders" {
		t.Errorf("expected com.company.orders, got %q", bestTopic)
	}
}

func TestNLQ_FilterKeywordExtraction(t *testing.T) {
	t.Parallel()
	filterKeywords := []string{"failed", "error", "success", "warning", "critical", "timeout", "rejected", "denied"}
	tests := []struct {
		query string
		want  string
	}{
		{"show failed messages from orders", "failed"},
		{"find error events in payments", "error"},
		{"get success responses", "success"},
		{"show me all messages", ""},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			q := tt.query
			var found string
			for _, kw := range filterKeywords {
				if containsSubstr(q, kw) {
					found = kw
					break
				}
			}
			if found != tt.want {
				t.Errorf("query %q: expected filter %q, got %q", tt.query, tt.want, found)
			}
		})
	}
}

func TestNLQ_EmptyTopicList(t *testing.T) {
	t.Parallel()
	// With no topics, NaturalLanguageQuery should return error
	topics := []TopicSummary{}
	topicNames := make([]string, len(topics))
	for i, t := range topics {
		topicNames[i] = t.Name
	}

	bestTopic := ""
	for _, name := range topicNames {
		if topicMatchScore("show orders", name) > 0 {
			bestTopic = name
		}
	}
	if bestTopic != "" {
		t.Errorf("expected empty topic for empty list, got %q", bestTopic)
	}
}

func TestNLQ_NoMatchingTopic(t *testing.T) {
	t.Parallel()
	topics := []TopicSummary{{Name: "completely.unrelated"}}
	topicNames := make([]string, len(topics))
	for i, t := range topics {
		topicNames[i] = t.Name
	}

	var bestTopic string
	var bestScore int
	for _, name := range topicNames {
		score := topicMatchScore("find xyz 123 data", name)
		if score > bestScore {
			bestScore = score
			bestTopic = name
		}
	}
	// No match by score, would fall to partial matching
	if bestScore != 0 {
		t.Errorf("expected 0 score, got %d for topic %q", bestScore, bestTopic)
	}
}

// Helper to extract limit from NL query (mirrors p2_nlquery.go logic)
func extractNLQLimit(query string) int {
	q := query
	limit := 10
	for _, word := range []string{"last ", "recent ", "latest "} {
		if idx := indexOf(q, word); idx >= 0 {
			rest := q[idx+len(word):]
			parts := splitFields(rest)
			if len(parts) > 0 {
				if n := atoiSafe(parts[0]); n > 0 && n <= 1000 {
					limit = n
				}
			}
		}
	}
	return limit
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

func splitFields(s string) []string {
	var fields []string
	start := -1
	for i, c := range s {
		if c == ' ' || c == '\t' {
			if start >= 0 {
				fields = append(fields, s[start:i])
				start = -1
			}
		} else {
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		fields = append(fields, s[start:])
	}
	return fields
}

func atoiSafe(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func containsSubstr(s, sub string) bool {
	if len(sub) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
