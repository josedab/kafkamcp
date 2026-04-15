package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// AggregateRequest defines parameters for windowed aggregation.
type AggregateRequest struct {
	Topic      string `json:"topic"`
	Field      string `json:"field"`
	WindowMs   int64  `json:"window_ms"`
	Function   string `json:"function"` // count, sum, avg, min, max
}

// AggregateResult is the result of a windowed aggregation.
type AggregateResult struct {
	Topic         string  `json:"topic"`
	Field         string  `json:"field"`
	Function      string  `json:"function"`
	WindowMs      int64   `json:"window_ms"`
	Value         float64 `json:"value"`
	MessageCount  int     `json:"message_count"`
	WindowStart   string  `json:"window_start"`
	WindowEnd     string  `json:"window_end"`
}

// Aggregate computes count, sum, avg, min, or max over a time window on a numeric field.
func (m *Manager) Aggregate(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
	if req.WindowMs <= 0 {
		req.WindowMs = 300000 // 5 minutes default
	}

	windowStart := time.Now().Add(-time.Duration(req.WindowMs) * time.Millisecond)
	startISO := windowStart.UTC().Format(time.RFC3339)

	msgs, err := m.Consume(ctx, cluster, req.Topic, -1, startISO, 10000, 15000, "auto")
	if err != nil {
		return nil, fmt.Errorf("consuming for aggregation: %w", err)
	}

	var values []float64
	for _, msg := range msgs {
		val, ok := extractNumericField(msg.Value, req.Field)
		if ok {
			values = append(values, val)
		}
	}

	result := &AggregateResult{
		Topic:        req.Topic,
		Field:        req.Field,
		Function:     req.Function,
		WindowMs:     req.WindowMs,
		MessageCount: len(msgs),
		WindowStart:  startISO,
		WindowEnd:    time.Now().UTC().Format(time.RFC3339),
	}

	if len(values) == 0 {
		return result, nil
	}

	switch strings.ToLower(req.Function) {
	case "count":
		result.Value = float64(len(values))
	case "sum":
		var sum float64
		for _, v := range values {
			sum += v
		}
		result.Value = sum
	case "avg":
		var sum float64
		for _, v := range values {
			sum += v
		}
		result.Value = sum / float64(len(values))
	case "min":
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		result.Value = min
	case "max":
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		result.Value = max
	default:
		return nil, fmt.Errorf("unsupported function %q; must be: count, sum, avg, min, max", req.Function)
	}

	return result, nil
}

func extractNumericField(value any, field string) (float64, bool) {
	if field == "" {
		// Try to parse the entire value as a number
		switch v := value.(type) {
		case float64:
			return v, true
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f, true
			}
		}
		return 0, false
	}

	// Navigate nested fields using dot notation
	parts := strings.Split(field, ".")
	current := value
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return 0, false
		}
		current, ok = m[part]
		if !ok {
			return 0, false
		}
	}

	switch v := current.(type) {
	case float64:
		return v, true
	case json.Number:
		f, err := v.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	}
	return 0, false
}

// TopicGraph represents the topology of producer->topic->consumer relationships.
type TopicGraph struct {
	Nodes []TopicGraphNode `json:"nodes"`
	Edges []TopicGraphEdge `json:"edges"`
}

// TopicGraphNode is a node in the topic graph.
type TopicGraphNode struct {
	ID   string `json:"id"`
	Type string `json:"type"` // topic, consumer_group, producer
	Name string `json:"name"`
}

// TopicGraphEdge is an edge in the topic graph.
type TopicGraphEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"` // produces, consumes
}

// GetTopicGraph builds a topology of topic->consumer-group relationships from cluster metadata.
func (m *Manager) GetTopicGraph(ctx context.Context, cluster string) (*TopicGraph, error) {
	topics, err := m.ListTopics(ctx, cluster, false)
	if err != nil {
		return nil, err
	}
	groups, err := m.ListConsumerGroups(ctx, cluster)
	if err != nil {
		return nil, err
	}

	admin, err := m.GetAdmin(cluster)
	if err != nil {
		return nil, err
	}

	graph := &TopicGraph{}
	topicSet := make(map[string]bool)

	// Add topic nodes
	for _, t := range topics {
		topicSet[t.Name] = true
		graph.Nodes = append(graph.Nodes, TopicGraphNode{
			ID:   "topic:" + t.Name,
			Type: "topic",
			Name: t.Name,
		})
	}

	// Add consumer group nodes and their edges
	groupIDs := make([]string, len(groups))
	for i, g := range groups {
		groupIDs[i] = g.GroupID
	}

	if len(groupIDs) > 0 {
		described, err := admin.DescribeGroups(ctx, groupIDs...)
		if err == nil {
			for _, dg := range described.Sorted() {
				graph.Nodes = append(graph.Nodes, TopicGraphNode{
					ID:   "group:" + dg.Group,
					Type: "consumer_group",
					Name: dg.Group,
				})
				// Find which topics this group consumes
				subscribedTopics := make(map[string]bool)
				for _, member := range dg.Members {
					if assign, ok := member.Assigned.AsConsumer(); ok {
						for _, ta := range assign.Topics {
							subscribedTopics[ta.Topic] = true
						}
					}
				}
				for topic := range subscribedTopics {
					if topicSet[topic] {
						graph.Edges = append(graph.Edges, TopicGraphEdge{
							Source: "topic:" + topic,
							Target: "group:" + dg.Group,
							Type:   "consumes",
						})
					}
				}
			}
		}
	}

	return graph, nil
}

// SchemaEvolutionSuggestion is a suggested schema evolution strategy.
type SchemaEvolutionSuggestion struct {
	Strategy    string   `json:"strategy"`
	Description string   `json:"description"`
	Compatible  bool     `json:"compatible"`
	Steps       []string `json:"steps"`
}

// SuggestSchemaEvolution suggests compatible evolution strategies for a schema change.
func SuggestSchemaEvolution(currentSchema map[string]any, changeDescription string) []SchemaEvolutionSuggestion {
	var suggestions []SchemaEvolutionSuggestion

	changeDesc := strings.ToLower(changeDescription)

	if strings.Contains(changeDesc, "add") && strings.Contains(changeDesc, "field") {
		suggestions = append(suggestions, SchemaEvolutionSuggestion{
			Strategy:    "add_optional_field",
			Description: "Add a new optional field with a default value. This is backward and forward compatible.",
			Compatible:  true,
			Steps: []string{
				"Add the new field to the schema with a default value",
				"Register the new schema version",
				"Update producers to populate the new field",
				"Update consumers to handle the new field (they can ignore it initially)",
			},
		})
		suggestions = append(suggestions, SchemaEvolutionSuggestion{
			Strategy:    "add_nullable_field",
			Description: "Add a new field as a union of null and the desired type, with null as default.",
			Compatible:  true,
			Steps: []string{
				"Define the field type as [\"null\", \"desired_type\"] with default null",
				"Register the new schema version",
				"Producers can start sending the field when ready",
				"Consumers treat missing values as null",
			},
		})
	}

	if strings.Contains(changeDesc, "remove") || strings.Contains(changeDesc, "delete") {
		suggestions = append(suggestions, SchemaEvolutionSuggestion{
			Strategy:    "deprecate_then_remove",
			Description: "Mark the field as deprecated, stop writing it, then remove after all consumers have updated.",
			Compatible:  true,
			Steps: []string{
				"Add 'deprecated' documentation to the field",
				"Ensure the field has a default value (for backward compatibility)",
				"Update producers to stop setting the field",
				"After all consumers have updated, remove the field in a new schema version",
			},
		})
	}

	if strings.Contains(changeDesc, "rename") {
		suggestions = append(suggestions, SchemaEvolutionSuggestion{
			Strategy:    "add_alias",
			Description: "Use Avro aliases to rename a field while maintaining compatibility.",
			Compatible:  true,
			Steps: []string{
				"Add the new field name with the old name as an alias",
				"Register the new schema version",
				"Update producers to use the new field name",
				"Consumers will map the old name to the new name via the alias",
			},
		})
		suggestions = append(suggestions, SchemaEvolutionSuggestion{
			Strategy:    "dual_write",
			Description: "Add new field, write to both old and new, then deprecate old.",
			Compatible:  true,
			Steps: []string{
				"Add the new field with a default value",
				"Update producers to write to both old and new fields",
				"Update consumers to read from the new field",
				"Deprecate and eventually remove the old field",
			},
		})
	}

	if strings.Contains(changeDesc, "type") || strings.Contains(changeDesc, "change") {
		suggestions = append(suggestions, SchemaEvolutionSuggestion{
			Strategy:    "widen_type",
			Description: "Widen the field type (e.g., int→long, float→double). This is compatible for promotable types.",
			Compatible:  true,
			Steps: []string{
				"Change the field type to the wider type (int→long, float→double)",
				"Register the new schema version",
				"Producers will automatically use the wider type",
				"Consumers can read the wider type",
			},
		})
		suggestions = append(suggestions, SchemaEvolutionSuggestion{
			Strategy:    "new_field_migration",
			Description: "Add a new field with the desired type, migrate data, then deprecate the old field.",
			Compatible:  true,
			Steps: []string{
				"Add a new field with the desired type and a default value",
				"Update producers to write to both old and new fields",
				"Update consumers to prefer the new field",
				"After migration, deprecate the old field",
			},
		})
	}

	// Generic suggestions if nothing specific matched
	if len(suggestions) == 0 {
		suggestions = append(suggestions, SchemaEvolutionSuggestion{
			Strategy:    "compatibility_check_first",
			Description: "Use kafka_check_schema_compatibility to test your proposed change before applying.",
			Compatible:  true,
			Steps: []string{
				"Write the proposed schema JSON",
				"Call kafka_check_schema_compatibility with the proposed schema",
				"If compatible, register the new version",
				"If incompatible, consider adding the field as optional with a default",
			},
		})
	}

	return suggestions
}

// JoinRequest defines parameters for a cross-topic join.
type JoinRequest struct {
	LeftTopic  string `json:"left_topic"`
	RightTopic string `json:"right_topic"`
	KeyField   string `json:"key_field"`
	WindowMs   int64  `json:"window_ms"`
	Limit      int    `json:"limit"`
}

// JoinedMessage represents a joined message from two topics.
type JoinedMessage struct {
	Key       any    `json:"key"`
	Left      any    `json:"left_value"`
	Right     any    `json:"right_value"`
	LeftTopic string `json:"left_topic"`
	RightTopic string `json:"right_topic"`
	Timestamp string `json:"timestamp"`
}

// JoinResult is the result of a cross-topic join.
type JoinResult struct {
	Joined     []JoinedMessage `json:"joined"`
	LeftCount  int             `json:"left_count"`
	RightCount int             `json:"right_count"`
	MatchCount int             `json:"match_count"`
	WindowMs   int64           `json:"window_ms"`
}

// JoinTopics performs a time-windowed join across two topics on a key field.
func (m *Manager) JoinTopics(ctx context.Context, cluster string, req JoinRequest) (*JoinResult, error) {
	if req.WindowMs <= 0 {
		req.WindowMs = 60000 // 1 minute default
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}

	windowStart := time.Now().Add(-time.Duration(req.WindowMs) * time.Millisecond)
	startISO := windowStart.UTC().Format(time.RFC3339)

	leftMsgs, err := m.Consume(ctx, cluster, req.LeftTopic, -1, startISO, 5000, 10000, "auto")
	if err != nil {
		return nil, fmt.Errorf("consuming left topic: %w", err)
	}

	rightMsgs, err := m.Consume(ctx, cluster, req.RightTopic, -1, startISO, 5000, 10000, "auto")
	if err != nil {
		return nil, fmt.Errorf("consuming right topic: %w", err)
	}

	// Build index on right topic by key
	rightByKey := make(map[string][]Message)
	for _, msg := range rightMsgs {
		key := extractJoinKey(msg, req.KeyField)
		if key != "" {
			rightByKey[key] = append(rightByKey[key], msg)
		}
	}

	// Join left with right
	var joined []JoinedMessage
	for _, leftMsg := range leftMsgs {
		if len(joined) >= req.Limit {
			break
		}
		key := extractJoinKey(leftMsg, req.KeyField)
		if key == "" {
			continue
		}
		if rightMatches, ok := rightByKey[key]; ok {
			for _, rightMsg := range rightMatches {
				if len(joined) >= req.Limit {
					break
				}
				joined = append(joined, JoinedMessage{
					Key:        key,
					Left:       leftMsg.Value,
					Right:      rightMsg.Value,
					LeftTopic:  req.LeftTopic,
					RightTopic: req.RightTopic,
					Timestamp:  leftMsg.Timestamp,
				})
			}
		}
	}

	return &JoinResult{
		Joined:     joined,
		LeftCount:  len(leftMsgs),
		RightCount: len(rightMsgs),
		MatchCount: len(joined),
		WindowMs:   req.WindowMs,
	}, nil
}

func extractJoinKey(msg Message, keyField string) string {
	// If keyField is empty, use the message key
	if keyField == "" {
		return fmt.Sprintf("%v", msg.Key)
	}
	// Extract from value using dot notation
	parts := strings.Split(keyField, ".")
	current := msg.Value
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current, ok = m[part]
		if !ok {
			return ""
		}
	}
	return fmt.Sprintf("%v", current)
}

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
