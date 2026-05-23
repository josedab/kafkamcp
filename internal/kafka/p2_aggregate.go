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
	Topic    string `json:"topic"`
	Field    string `json:"field"`
	WindowMs int64  `json:"window_ms"`
	Function string `json:"function"` // count, sum, avg, min, max
}

// AggregateResult is the result of a windowed aggregation.
type AggregateResult struct {
	Topic        string  `json:"topic"`
	Field        string  `json:"field"`
	Function     string  `json:"function"`
	WindowMs     int64   `json:"window_ms"`
	Value        float64 `json:"value"`
	MessageCount int     `json:"message_count"`
	WindowStart  string  `json:"window_start"`
	WindowEnd    string  `json:"window_end"`
}

// Aggregate computes count, sum, avg, min, or max over a time window on a numeric field.
func (m *Manager) Aggregate(ctx context.Context, cluster string, req AggregateRequest) (*AggregateResult, error) {
	// Validate function before consuming messages to avoid wasted resources
	fn := strings.ToLower(req.Function)
	validFunctions := map[string]bool{"count": true, "sum": true, "avg": true, "min": true, "max": true}
	if !validFunctions[fn] {
		return nil, fmt.Errorf("unsupported function %q; must be: count, sum, avg, min, max", req.Function)
	}

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

	switch fn {
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
