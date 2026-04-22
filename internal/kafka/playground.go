package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strings"
	"time"
)

const playgroundPrefix = "_playground_"

const maxAgentIDLength = 64

var agentIDSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_-]`)

// sanitizeAgentID restricts agent IDs to alphanumeric, hyphen, and underscore
// characters and enforces a maximum length.
func sanitizeAgentID(agentID string) string {
	s := agentIDSanitizer.ReplaceAllString(agentID, "")
	if len(s) > maxAgentIDLength {
		s = s[:maxAgentIDLength]
	}
	if s == "" {
		s = "unknown"
	}
	return s
}

// PlaygroundTopicResult is the result of creating a playground topic.
type PlaygroundTopicResult struct {
	TopicName  string `json:"topic_name"`
	Partitions int32  `json:"partitions"`
	Status     string `json:"status"`
	RetentionMs int64 `json:"retention_ms"`
}

// PlaygroundCreate creates a temporary playground topic.
func (m *Manager) PlaygroundCreate(ctx context.Context, cluster, agentID, name string, partitions int32) (*PlaygroundTopicResult, error) {
	topicName := fmt.Sprintf("%s%s_%s", playgroundPrefix, sanitizeAgentID(agentID), name)
	if partitions <= 0 {
		partitions = 3
	}

	retentionMs := "3600000" // 1 hour
	err := m.CreateTopic(ctx, cluster, CreateTopicRequest{
		Name:              topicName,
		Partitions:        partitions,
		ReplicationFactor: 1,
		Configs: map[string]string{
			"retention.ms": retentionMs,
			"cleanup.policy": "delete",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating playground topic: %w", err)
	}

	return &PlaygroundTopicResult{
		TopicName:   topicName,
		Partitions:  partitions,
		Status:      "created",
		RetentionMs: 3600000,
	}, nil
}

// PlaygroundProduceSample produces sample data to a playground topic.
func (m *Manager) PlaygroundProduceSample(ctx context.Context, cluster, agentID, name, pattern string, count int) (map[string]any, error) {
	topicName := fmt.Sprintf("%s%s_%s", playgroundPrefix, sanitizeAgentID(agentID), name)

	messages := generateSampleData(pattern, count)

	results, err := m.Produce(ctx, cluster, topicName, messages, ProduceOptions{
		Acks: "1", Serializer: "json",
	})
	if err != nil {
		return nil, fmt.Errorf("producing sample data: %w", err)
	}

	return map[string]any{
		"topic":      topicName,
		"pattern":    pattern,
		"produced":   len(results),
		"deliveries": results,
	}, nil
}

// PlaygroundList lists all playground topics for an agent.
func (m *Manager) PlaygroundList(ctx context.Context, cluster, agentID string) ([]TopicSummary, error) {
	topics, err := m.ListTopics(ctx, cluster, false)
	if err != nil {
		return nil, err
	}

	prefix := playgroundPrefix + sanitizeAgentID(agentID)
	var result []TopicSummary
	for _, t := range topics {
		if strings.HasPrefix(t.Name, prefix) {
			result = append(result, t)
		}
	}
	return result, nil
}

// PlaygroundCleanup deletes all playground topics for an agent.
func (m *Manager) PlaygroundCleanup(ctx context.Context, cluster, agentID string) (int, error) {
	topics, err := m.PlaygroundList(ctx, cluster, agentID)
	if err != nil {
		return 0, err
	}

	deleted := 0
	for _, t := range topics {
		if err := m.DeleteTopic(ctx, cluster, t.Name); err != nil {
			m.logger.Warn("failed to delete playground topic", "topic", t.Name, "error", err)
			continue
		}
		deleted++
	}
	return deleted, nil
}

func generateSampleData(pattern string, count int) []ProduceMessage {
	messages := make([]ProduceMessage, 0, count)

	for i := 0; i < count; i++ {
		var msg ProduceMessage
		switch pattern {
		case "iot":
			msg = generateIOTMessage(i)
		case "user_activity":
			msg = generateUserActivityMessage(i)
		case "logs":
			msg = generateLogMessage(i)
		default: // ecommerce
			msg = generateEcommerceMessage(i)
		}
		messages = append(messages, msg)
	}
	return messages
}

func generateEcommerceMessage(idx int) ProduceMessage {
	products := []string{"laptop", "phone", "headphones", "keyboard", "monitor", "tablet", "camera"}
	statuses := []string{"created", "paid", "shipped", "delivered", "cancelled"}
	product := products[rand.IntN(len(products))]
	status := statuses[rand.IntN(len(statuses))]
	orderID := fmt.Sprintf("ORD-%06d", idx+1)

	value := map[string]any{
		"order_id":   orderID,
		"product":    product,
		"quantity":   rand.IntN(5) + 1,
		"price":      float64(rand.IntN(100000)) / 100.0,
		"status":     status,
		"customer_id": fmt.Sprintf("CUST-%04d", rand.IntN(1000)),
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	}

	valueBytes, _ := json.Marshal(value) //nolint:errcheck // marshaling simple map literals cannot fail
	return ProduceMessage{Key: orderID, Value: json.RawMessage(valueBytes)}
}

func generateIOTMessage(idx int) ProduceMessage {
	deviceID := fmt.Sprintf("device-%03d", rand.IntN(50))
	value := map[string]any{
		"device_id":   deviceID,
		"temperature": 15.0 + rand.Float64()*25.0,
		"humidity":    30.0 + rand.Float64()*60.0,
		"pressure":    980.0 + rand.Float64()*40.0,
		"battery":     rand.IntN(100),
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"location": map[string]any{
			"lat": 37.0 + rand.Float64()*10.0,
			"lon": -122.0 + rand.Float64()*5.0,
		},
	}
	valueBytes, _ := json.Marshal(value) //nolint:errcheck // marshaling simple map literals cannot fail
	return ProduceMessage{Key: deviceID, Value: json.RawMessage(valueBytes)}
}

func generateUserActivityMessage(idx int) ProduceMessage {
	actions := []string{"page_view", "click", "scroll", "form_submit", "search", "purchase", "logout"}
	pages := []string{"/home", "/products", "/cart", "/checkout", "/profile", "/search", "/about"}
	userID := fmt.Sprintf("user-%04d", rand.IntN(200))
	action := actions[rand.IntN(len(actions))]

	value := map[string]any{
		"user_id":    userID,
		"action":     action,
		"page":       pages[rand.IntN(len(pages))],
		"session_id": fmt.Sprintf("sess-%08d", rand.IntN(100000)),
		"duration_ms": rand.IntN(30000),
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
		"user_agent": "Mozilla/5.0 (Sample Data)",
	}
	valueBytes, _ := json.Marshal(value) //nolint:errcheck // marshaling simple map literals cannot fail
	return ProduceMessage{Key: userID, Value: json.RawMessage(valueBytes)}
}

func generateLogMessage(idx int) ProduceMessage {
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	services := []string{"api-gateway", "user-service", "payment-service", "order-service", "notification-service"}
	messages := []string{
		"Request processed successfully",
		"Connection timeout to downstream service",
		"Rate limit exceeded for client",
		"Cache miss for key",
		"Database query took longer than threshold",
		"Retrying failed operation",
		"Health check passed",
	}

	level := levels[rand.IntN(len(levels))]
	service := services[rand.IntN(len(services))]

	value := map[string]any{
		"level":     level,
		"service":   service,
		"message":   messages[rand.IntN(len(messages))],
		"trace_id":  fmt.Sprintf("trace-%012d", rand.Int64N(1000000000000)),
		"span_id":   fmt.Sprintf("span-%08d", rand.IntN(100000000)),
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"host":      fmt.Sprintf("%s-%d", service, rand.IntN(5)),
	}
	valueBytes, _ := json.Marshal(value) //nolint:errcheck // marshaling simple map literals cannot fail
	return ProduceMessage{Key: service, Value: json.RawMessage(valueBytes)}
}
