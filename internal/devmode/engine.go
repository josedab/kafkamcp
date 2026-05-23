// Package devmode provides a lightweight local development mode that helps
// developers explore KafkaMCP capabilities with sample data and guided tutorials.
package devmode

import (
	"fmt"
	"log/slog"
	"time"
)

// SampleTopic defines a pre-built topic for local development.
type SampleTopic struct {
	Name              string            `json:"name"`
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replication_factor"`
	Configs           map[string]string `json:"configs"`
	Generator         string            `json:"generator"`
	Description       string            `json:"description"`
}

// SampleMessage is a pre-built message for sample data generation.
type SampleMessage struct {
	Topic string         `json:"topic"`
	Key   string         `json:"key"`
	Value map[string]any `json:"value"`
}

// TutorialStep is one step in a guided walkthrough.
type TutorialStep struct {
	ID          string         `json:"id"`
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Tool        string         `json:"tool"`
	Args        map[string]any `json:"args,omitempty"`
	Expected    string         `json:"expected"`
	Tips        []string       `json:"tips,omitempty"`
}

// Tutorial is a guided interactive walkthrough.
type Tutorial struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Steps       []TutorialStep `json:"steps"`
	Difficulty  string         `json:"difficulty"`
	Duration    string         `json:"estimated_duration"`
}

// DevEnvironment describes the local dev environment state.
type DevEnvironment struct {
	Status        string        `json:"status"`
	KafkaMode     string        `json:"kafka_mode"`
	SampleTopics  []SampleTopic `json:"sample_topics"`
	TutorialCount int           `json:"tutorial_count"`
	SetupCommand  string        `json:"setup_command"`
	QuickStart    []string      `json:"quick_start"`
}

// Engine manages the offline-first local development mode.
type Engine struct {
	topics    []SampleTopic
	tutorials []Tutorial
	logger    *slog.Logger
}

// NewEngine creates a new dev mode engine.
func NewEngine(logger *slog.Logger) *Engine {
	return &Engine{
		topics:    defaultSampleTopics(),
		tutorials: defaultTutorials(),
		logger:    logger,
	}
}

// GetDevEnvironment returns the current dev environment configuration.
func (e *Engine) GetDevEnvironment() *DevEnvironment {
	return &DevEnvironment{
		Status:        "ready",
		KafkaMode:     "kraft-single-node",
		SampleTopics:  e.topics,
		TutorialCount: len(e.tutorials),
		SetupCommand:  "kafkamcp dev",
		QuickStart: []string{
			"1. Run 'kafkamcp dev' to start a local Kafka broker with sample data",
			"2. Use kafka_list_topics to see pre-populated topics",
			"3. Use kafka_consume to read sample messages",
			"4. Follow a tutorial with kafka_dev_tutorial action=start",
		},
	}
}

// GenerateSampleData produces sample messages for a given topic generator.
func (e *Engine) GenerateSampleData(generator string, count int) []SampleMessage {
	if count <= 0 {
		count = 10
	}
	if count > 100 {
		count = 100
	}

	switch generator {
	case "ecommerce-orders":
		return generateOrderMessages(count)
	case "user-events":
		return generateUserEventMessages(count)
	case "iot-sensors":
		return generateIoTMessages(count)
	case "click-stream":
		return generateClickstreamMessages(count)
	case "payment-events":
		return generatePaymentMessages(count)
	default:
		return generateOrderMessages(count)
	}
}

// GetTutorial retrieves a tutorial by ID.
func (e *Engine) GetTutorial(id string) (*Tutorial, error) {
	for i := range e.tutorials {
		if e.tutorials[i].ID == id {
			return &e.tutorials[i], nil
		}
	}
	return nil, fmt.Errorf("tutorial %q not found", id)
}

// ListTutorials returns available tutorials.
func (e *Engine) ListTutorials(difficulty string) []Tutorial {
	if difficulty == "" {
		return e.tutorials
	}
	var result []Tutorial
	for _, t := range e.tutorials {
		if t.Difficulty == difficulty {
			result = append(result, t)
		}
	}
	return result
}

// GetSampleTopics returns the sample topic definitions.
func (e *Engine) GetSampleTopics() []SampleTopic {
	return e.topics
}

func defaultSampleTopics() []SampleTopic {
	return []SampleTopic{
		{
			Name: "orders", Partitions: 6, ReplicationFactor: 1,
			Configs:     map[string]string{"retention.ms": "86400000", "cleanup.policy": "delete"},
			Generator:   "ecommerce-orders",
			Description: "E-commerce order events with order_id, customer, items, total",
		},
		{
			Name: "user-events", Partitions: 3, ReplicationFactor: 1,
			Configs:     map[string]string{"retention.ms": "604800000"},
			Generator:   "user-events",
			Description: "User activity events: login, signup, profile_update, logout",
		},
		{
			Name: "iot-sensors", Partitions: 12, ReplicationFactor: 1,
			Configs:     map[string]string{"retention.ms": "3600000", "cleanup.policy": "compact"},
			Generator:   "iot-sensors",
			Description: "IoT sensor readings: temperature, humidity, pressure",
		},
		{
			Name: "click-stream", Partitions: 6, ReplicationFactor: 1,
			Configs:     map[string]string{"retention.ms": "86400000"},
			Generator:   "click-stream",
			Description: "Web clickstream events: page views, clicks, scrolls",
		},
		{
			Name: "payments", Partitions: 3, ReplicationFactor: 1,
			Configs:     map[string]string{"retention.ms": "2592000000"},
			Generator:   "payment-events",
			Description: "Payment processing events: initiated, authorized, captured, refunded",
		},
	}
}

func defaultTutorials() []Tutorial {
	return []Tutorial{
		{
			ID:          "getting-started",
			Name:        "Getting Started with KafkaMCP",
			Description: "Learn the basics: list topics, consume messages, and explore your cluster.",
			Difficulty:  "beginner",
			Duration:    "5 minutes",
			Steps: []TutorialStep{
				{ID: "s1", Title: "List Topics", Description: "Discover all topics in your cluster", Tool: "kafka_list_topics", Expected: "You should see 5 sample topics"},
				{ID: "s2", Title: "Describe a Topic", Description: "Get details about the orders topic", Tool: "kafka_topic_describe", Args: map[string]any{"topic": "orders"}, Expected: "6 partitions, RF=1"},
				{ID: "s3", Title: "Consume Messages", Description: "Read the latest messages from orders", Tool: "kafka_consume", Args: map[string]any{"topic": "orders", "limit": 5}, Expected: "5 order events with customer info"},
				{ID: "s4", Title: "Check Consumer Groups", Description: "List all consumer groups", Tool: "kafka_consumer_groups", Expected: "Active consumer groups"},
			},
		},
		{
			ID:          "anomaly-detection",
			Name:        "Detecting Anomalies",
			Description: "Learn to detect and investigate anomalies in Kafka metrics.",
			Difficulty:  "intermediate",
			Duration:    "10 minutes",
			Steps: []TutorialStep{
				{ID: "s1", Title: "Check Cluster Health", Description: "Get an overview of cluster health", Tool: "kafka_cluster_info"},
				{ID: "s2", Title: "Run Anomaly Detection", Description: "Detect anomalies across all metrics", Tool: "kafka_anomaly_detect"},
				{ID: "s3", Title: "Diagnose Lag", Description: "If lag anomalies found, diagnose root cause", Tool: "kafka_lag_diagnosis"},
				{ID: "s4", Title: "Review Timeline", Description: "Query time-series for context", Tool: "kafka_timeseries_query", Args: map[string]any{"metric": "consumer_lag", "source": "orders", "window": "1h"}},
			},
		},
		{
			ID:          "schema-management",
			Name:        "Schema Evolution",
			Description: "Draft, test, and evolve schemas safely using the playground.",
			Difficulty:  "intermediate",
			Duration:    "15 minutes",
			Steps: []TutorialStep{
				{ID: "s1", Title: "Create a Draft Schema", Description: "Start a new Avro schema in the playground", Tool: "kafka_schema_playground", Args: map[string]any{"action": "create", "subject": "orders-value", "schema_type": "AVRO"}},
				{ID: "s2", Title: "Test with Messages", Description: "Test sample messages against your schema", Tool: "kafka_schema_playground", Args: map[string]any{"action": "test"}},
				{ID: "s3", Title: "Simulate Evolution", Description: "See what happens when you evolve the schema", Tool: "kafka_schema_playground", Args: map[string]any{"action": "evolve"}},
				{ID: "s4", Title: "Validate Compatibility", Description: "Check backward compatibility", Tool: "kafka_schema_playground", Args: map[string]any{"action": "validate"}},
			},
		},
		{
			ID:          "smart-investigation",
			Name:        "Smart Investigation Workflow",
			Description: "Use smart routing to automatically investigate issues.",
			Difficulty:  "advanced",
			Duration:    "10 minutes",
			Steps: []TutorialStep{
				{ID: "s1", Title: "Start Investigation", Description: "Use smart routing to plan investigation", Tool: "kafka_smart_route", Args: map[string]any{"request": "investigate why orders are delayed"}},
				{ID: "s2", Title: "Execute Steps", Description: "Follow the generated execution plan", Tool: "kafka_smart_route_status", Args: map[string]any{"action": "next_steps"}},
				{ID: "s3", Title: "Record Results", Description: "Complete steps as you investigate", Tool: "kafka_smart_route_status", Args: map[string]any{"action": "complete_step"}},
			},
		},
		{
			ID:          "digital-twin",
			Name:        "Cluster Digital Twin",
			Description: "Test operations safely using a cluster digital twin.",
			Difficulty:  "advanced",
			Duration:    "15 minutes",
			Steps: []TutorialStep{
				{ID: "s1", Title: "Capture Snapshot", Description: "Take a snapshot of the current cluster", Tool: "kafka_twin_snapshot"},
				{ID: "s2", Title: "Create Twin", Description: "Create a mutable copy for simulation", Tool: "kafka_twin_simulate", Args: map[string]any{"action": "create_twin"}},
				{ID: "s3", Title: "Simulate Changes", Description: "Try creating topics and changing configs", Tool: "kafka_twin_simulate", Args: map[string]any{"action": "simulate"}},
				{ID: "s4", Title: "Review Diff", Description: "See what changed vs production", Tool: "kafka_twin_simulate", Args: map[string]any{"action": "diff"}},
				{ID: "s5", Title: "Generate Plan", Description: "Create a migration plan", Tool: "kafka_twin_simulate", Args: map[string]any{"action": "migrate_plan"}},
			},
		},
	}
}

func generateOrderMessages(count int) []SampleMessage {
	statuses := []string{"created", "processing", "shipped", "delivered"}
	msgs := make([]SampleMessage, count)
	for i := 0; i < count; i++ {
		msgs[i] = SampleMessage{
			Topic: "orders",
			Key:   fmt.Sprintf("order-%d", 1000+i),
			Value: map[string]any{
				"order_id":   fmt.Sprintf("ORD-%d", 1000+i),
				"customer":   fmt.Sprintf("customer-%d", (i%50)+1),
				"items":      (i % 5) + 1,
				"total":      float64((i%100)+1) * 9.99,
				"status":     statuses[i%len(statuses)],
				"created_at": time.Now().Add(time.Duration(-i) * time.Minute).Format(time.RFC3339),
			},
		}
	}
	return msgs
}

func generateUserEventMessages(count int) []SampleMessage {
	events := []string{"login", "signup", "profile_update", "logout", "password_reset"}
	msgs := make([]SampleMessage, count)
	for i := 0; i < count; i++ {
		msgs[i] = SampleMessage{
			Topic: "user-events",
			Key:   fmt.Sprintf("user-%d", (i%100)+1),
			Value: map[string]any{
				"user_id":    fmt.Sprintf("USR-%d", (i%100)+1),
				"event_type": events[i%len(events)],
				"ip_address": fmt.Sprintf("192.168.%d.%d", i%255, (i*7)%255),
				"timestamp":  time.Now().Add(time.Duration(-i) * time.Second).Format(time.RFC3339),
			},
		}
	}
	return msgs
}

func generateIoTMessages(count int) []SampleMessage {
	msgs := make([]SampleMessage, count)
	for i := 0; i < count; i++ {
		msgs[i] = SampleMessage{
			Topic: "iot-sensors",
			Key:   fmt.Sprintf("sensor-%d", (i%20)+1),
			Value: map[string]any{
				"sensor_id":   fmt.Sprintf("SENSOR-%d", (i%20)+1),
				"temperature": 20.0 + float64(i%15),
				"humidity":    40.0 + float64(i%30),
				"pressure":    1013.0 + float64(i%10),
				"location":    fmt.Sprintf("zone-%d", (i%5)+1),
				"timestamp":   time.Now().Add(time.Duration(-i) * time.Second).Format(time.RFC3339),
			},
		}
	}
	return msgs
}

func generateClickstreamMessages(count int) []SampleMessage {
	pages := []string{"/", "/products", "/cart", "/checkout", "/account", "/search"}
	actions := []string{"page_view", "click", "scroll", "hover"}
	msgs := make([]SampleMessage, count)
	for i := 0; i < count; i++ {
		msgs[i] = SampleMessage{
			Topic: "click-stream",
			Key:   fmt.Sprintf("session-%d", (i%50)+1),
			Value: map[string]any{
				"session_id":  fmt.Sprintf("SESS-%d", (i%50)+1),
				"user_id":     fmt.Sprintf("USR-%d", (i%100)+1),
				"page":        pages[i%len(pages)],
				"action":      actions[i%len(actions)],
				"duration_ms": (i % 3000) + 100,
				"timestamp":   time.Now().Add(time.Duration(-i) * time.Millisecond * 500).Format(time.RFC3339),
			},
		}
	}
	return msgs
}

func generatePaymentMessages(count int) []SampleMessage {
	statuses := []string{"initiated", "authorized", "captured", "refunded", "failed"}
	msgs := make([]SampleMessage, count)
	for i := 0; i < count; i++ {
		msgs[i] = SampleMessage{
			Topic: "payments",
			Key:   fmt.Sprintf("pay-%d", 5000+i),
			Value: map[string]any{
				"payment_id": fmt.Sprintf("PAY-%d", 5000+i),
				"order_id":   fmt.Sprintf("ORD-%d", 1000+(i%100)),
				"amount":     float64((i%500)+1) * 1.99,
				"currency":   "USD",
				"status":     statuses[i%len(statuses)],
				"method":     []string{"card", "paypal", "bank"}[i%3],
				"timestamp":  time.Now().Add(time.Duration(-i) * time.Minute).Format(time.RFC3339),
			},
		}
	}
	return msgs
}
