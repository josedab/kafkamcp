package kafka

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// NaturalLanguageQueryResult is the result of a natural language query.
type NaturalLanguageQueryResult struct {
	InterpretedAs string    `json:"interpreted_as"`
	Topic         string    `json:"topic"`
	Filter        string    `json:"filter,omitempty"`
	Messages      []Message `json:"messages"`
	Count         int       `json:"count"`
}

// NaturalLanguageQuery translates a natural language description to a topic + filter + consume call.
func (m *Manager) NaturalLanguageQuery(ctx context.Context, cluster, query string, topics []TopicSummary) (*NaturalLanguageQueryResult, error) {
	q := strings.ToLower(query)

	// Extract limit from query
	limit := 10
	for _, word := range []string{"last ", "recent ", "latest "} {
		if idx := strings.Index(q, word); idx >= 0 {
			rest := q[idx+len(word):]
			parts := strings.Fields(rest)
			if len(parts) > 0 {
				if n, err := strconv.Atoi(parts[0]); err == nil && n > 0 && n <= 1000 {
					limit = n
				}
			}
		}
	}

	// Find the best matching topic
	var bestTopic string
	var bestScore int
	topicNames := make([]string, len(topics))
	for i, t := range topics {
		topicNames[i] = t.Name
	}

	for _, t := range topicNames {
		score := topicMatchScore(q, t)
		if score > bestScore {
			bestScore = score
			bestTopic = t
		}
	}

	if bestTopic == "" && len(topicNames) > 0 {
		// No match found; try partial matching
		for _, t := range topicNames {
			nameParts := strings.Split(strings.ToLower(t), ".")
			for _, part := range nameParts {
				if strings.Contains(q, part) {
					bestTopic = t
					break
				}
			}
			if bestTopic != "" {
				break
			}
		}
	}

	if bestTopic == "" {
		return nil, fmt.Errorf("could not determine topic from query %q; available topics: %v", query, topicNames)
	}

	// Extract filter keywords
	var valueFilter string
	filterKeywords := []string{"failed", "error", "success", "warning", "critical", "timeout", "rejected", "denied"}
	for _, kw := range filterKeywords {
		if strings.Contains(q, kw) {
			valueFilter = kw
			break
		}
	}

	interpretation := fmt.Sprintf("Consume %d messages from topic %q", limit, bestTopic)
	if valueFilter != "" {
		interpretation += fmt.Sprintf(" filtered by %q", valueFilter)
	}

	var msgs []Message
	var err error

	if valueFilter != "" {
		matched, _, searchErr := m.SearchMessages(ctx, cluster, SearchRequest{
			Topic:         bestTopic,
			ValueContains: valueFilter,
			MaxScan:       5000,
			Limit:         limit,
		})
		if searchErr != nil {
			return nil, searchErr
		}
		msgs = matched
	} else {
		msgs, err = m.Consume(ctx, cluster, bestTopic, -1, "latest", limit, 5000, "auto")
		if err != nil {
			return nil, err
		}
	}

	return &NaturalLanguageQueryResult{
		InterpretedAs: interpretation,
		Topic:         bestTopic,
		Filter:        valueFilter,
		Messages:      msgs,
		Count:         len(msgs),
	}, nil
}

func topicMatchScore(query, topicName string) int {
	score := 0
	name := strings.ToLower(topicName)
	nameParts := strings.FieldsFunc(name, func(r rune) bool {
		return r == '.' || r == '-' || r == '_'
	})

	// Exact topic name mention
	if strings.Contains(query, name) {
		score += 100
	}

	// Partial segment matches
	for _, part := range nameParts {
		if len(part) >= 3 && strings.Contains(query, part) {
			score += 10
		}
	}

	// Common keyword-topic associations
	associations := map[string][]string{
		"order":   {"order", "purchase", "checkout", "buy"},
		"payment": {"payment", "pay", "transaction", "billing"},
		"user":    {"user", "customer", "account", "profile"},
		"event":   {"event", "activity", "action", "click"},
		"error":   {"error", "fail", "dlq", "dead"},
		"log":     {"log", "audit", "trace"},
	}
	for segment, keywords := range associations {
		for _, part := range nameParts {
			if strings.Contains(part, segment) {
				for _, kw := range keywords {
					if strings.Contains(query, kw) {
						score += 5
					}
				}
			}
		}
	}

	return score
}
