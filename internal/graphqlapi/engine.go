package graphqlapi

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/randid"
)

// FieldType represents a GraphQL field type.
type FieldType string

// TypeString and related values define constants for this package.
const (
	TypeString  FieldType = "String"
	TypeInt     FieldType = "Int"
	TypeFloat   FieldType = "Float"
	TypeBoolean FieldType = "Boolean"
	TypeID      FieldType = "ID"
	TypeObject  FieldType = "Object"
	TypeList    FieldType = "List"
)

// SchemaField describes a field in the GraphQL schema.
type SchemaField struct {
	Name        string        `json:"name"`
	Type        FieldType     `json:"type"`
	Required    bool          `json:"required"`
	Description string        `json:"description,omitempty"`
	SubFields   []SchemaField `json:"sub_fields,omitempty"`
}

// SchemaType describes a GraphQL type.
type SchemaType struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Fields      []SchemaField `json:"fields"`
	Kind        string        `json:"kind"` // type, input, enum
}

// QueryDef describes a GraphQL query or mutation.
type QueryDef struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Args        []SchemaField `json:"args,omitempty"`
	ReturnType  string        `json:"return_type"`
	Category    string        `json:"category"`
}

// SubscriptionDef describes a GraphQL subscription.
type SubscriptionDef struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Args        []SchemaField `json:"args,omitempty"`
	ReturnType  string        `json:"return_type"`
	Topic       string        `json:"topic,omitempty"`
}

// Schema represents the complete GraphQL schema.
type Schema struct {
	Types         []SchemaType      `json:"types"`
	Queries       []QueryDef        `json:"queries"`
	Mutations     []QueryDef        `json:"mutations"`
	Subscriptions []SubscriptionDef `json:"subscriptions"`
	GeneratedAt   string            `json:"generated_at"`
}

// Subscriber represents an active subscription connection.
type Subscriber struct {
	ID               string `json:"id"`
	SubscriptionName string `json:"subscription_name"`
	Topic            string `json:"topic,omitempty"`
	CreatedAt        string `json:"created_at"`
	MessageCount     int64  `json:"message_count"`
	Active           bool   `json:"active"`
}

// QueryResult represents the result of a GraphQL query execution.
type QueryResult struct {
	Data   any        `json:"data,omitempty"`
	Errors []GQLError `json:"errors,omitempty"`
}

// GQLError represents a GraphQL error.
type GQLError struct {
	Message   string     `json:"message"`
	Path      []string   `json:"path,omitempty"`
	Locations []Location `json:"locations,omitempty"`
}

// Location is a GraphQL error location.
type Location struct {
	Line   int `json:"line"`
	Column int `json:"column"`
}

// Engine manages the GraphQL schema, resolvers, and subscriptions.
type Engine struct {
	mu           sync.RWMutex
	schema       *Schema
	subscribers  map[string]*Subscriber
	toolResolver func(toolName string, args map[string]any) (any, error)
	logger       *slog.Logger
}

// NewEngine creates a new GraphQL API engine.
func NewEngine(logger *slog.Logger) *Engine {
	e := &Engine{
		subscribers: make(map[string]*Subscriber),
		logger:      logger,
	}
	e.schema = e.generateSchema()
	return e
}

// SetToolResolver sets the function used to resolve tool calls from GraphQL queries.
func (e *Engine) SetToolResolver(resolver func(toolName string, args map[string]any) (any, error)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.toolResolver = resolver
}

// GetSchema returns the current GraphQL schema.
func (e *Engine) GetSchema() *Schema {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.schema
}

// GetSchemaSDL returns the schema in GraphQL SDL format.
func (e *Engine) GetSchemaSDL() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.renderSDL()
}

// ExecuteQuery executes a GraphQL query against the schema.
func (e *Engine) ExecuteQuery(query string, variables map[string]any) *QueryResult {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if query == "" {
		return &QueryResult{Errors: []GQLError{{Message: "query is required"}}}
	}

	trimmed := strings.TrimSpace(query)

	// Parse introspection queries
	if strings.Contains(trimmed, "__schema") || strings.Contains(trimmed, "__type") {
		return e.handleIntrospection()
	}

	// Basic query validation
	if !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "query") &&
		!strings.HasPrefix(trimmed, "mutation") && !strings.HasPrefix(trimmed, "subscription") {
		return &QueryResult{Errors: []GQLError{{Message: "invalid query: must start with '{', 'query', 'mutation', or 'subscription'"}}}
	}

	// Check for unbalanced braces
	if strings.Count(trimmed, "{") != strings.Count(trimmed, "}") {
		return &QueryResult{Errors: []GQLError{{Message: "invalid query: unbalanced braces"}}}
	}

	// Resolve against tool resolver if available
	if e.toolResolver != nil {
		result, err := e.toolResolver(query, variables)
		if err != nil {
			return &QueryResult{Errors: []GQLError{{Message: err.Error()}}}
		}
		return &QueryResult{Data: result}
	}

	return &QueryResult{Data: map[string]any{
		"message": "Query parsed successfully. Set a tool resolver to execute against Kafka tools.",
		"query":   query,
	}}
}

func (e *Engine) handleIntrospection() *QueryResult {
	return &QueryResult{Data: map[string]any{
		"__schema": map[string]any{
			"types":         len(e.schema.Types),
			"queries":       len(e.schema.Queries),
			"mutations":     len(e.schema.Mutations),
			"subscriptions": len(e.schema.Subscriptions),
		},
	}}
}

// Subscribe creates a new subscription.
func (e *Engine) Subscribe(subscriptionName, topic string) (*Subscriber, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Validate subscription exists
	found := false
	for _, sub := range e.schema.Subscriptions {
		if sub.Name == subscriptionName {
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("subscription %q not found in schema", subscriptionName)
	}

	subscriber := &Subscriber{
		ID:               randid.New("gql-sub"),
		SubscriptionName: subscriptionName,
		Topic:            topic,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		Active:           true,
	}

	e.subscribers[subscriber.ID] = subscriber
	return subscriber, nil
}

// Unsubscribe removes a subscription.
func (e *Engine) Unsubscribe(subscriberID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	sub, ok := e.subscribers[subscriberID]
	if !ok {
		return fmt.Errorf("subscriber %q not found", subscriberID)
	}
	sub.Active = false
	delete(e.subscribers, subscriberID)
	return nil
}

// ListSubscribers returns all active subscribers.
func (e *Engine) ListSubscribers() []Subscriber {
	e.mu.RLock()
	defer e.mu.RUnlock()

	result := make([]Subscriber, 0, len(e.subscribers))
	for _, s := range e.subscribers {
		result = append(result, *s)
	}
	return result
}

func (e *Engine) generateSchema() *Schema {
	schema := &Schema{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Core types
	schema.Types = []SchemaType{
		{
			Name: "Topic", Description: "A Kafka topic",
			Kind: "type",
			Fields: []SchemaField{
				{Name: "name", Type: TypeString, Required: true},
				{Name: "partitions", Type: TypeInt, Required: true},
				{Name: "replicationFactor", Type: TypeInt, Required: true},
				{Name: "retentionMs", Type: TypeInt},
				{Name: "messageCount", Type: TypeInt},
				{Name: "isInternal", Type: TypeBoolean},
			},
		},
		{
			Name: "ConsumerGroup", Description: "A Kafka consumer group",
			Kind: "type",
			Fields: []SchemaField{
				{Name: "groupId", Type: TypeID, Required: true},
				{Name: "state", Type: TypeString},
				{Name: "members", Type: TypeInt},
				{Name: "totalLag", Type: TypeInt},
			},
		},
		{
			Name: "Message", Description: "A Kafka message",
			Kind: "type",
			Fields: []SchemaField{
				{Name: "topic", Type: TypeString, Required: true},
				{Name: "partition", Type: TypeInt},
				{Name: "offset", Type: TypeInt},
				{Name: "key", Type: TypeString},
				{Name: "value", Type: TypeString},
				{Name: "timestamp", Type: TypeString},
			},
		},
		{
			Name: "ClusterHealth", Description: "Cluster health status",
			Kind: "type",
			Fields: []SchemaField{
				{Name: "cluster", Type: TypeString, Required: true},
				{Name: "brokerCount", Type: TypeInt},
				{Name: "topicCount", Type: TypeInt},
				{Name: "healthScore", Type: TypeFloat},
				{Name: "status", Type: TypeString},
			},
		},
		{
			Name: "ComplianceResult", Description: "Compliance scan result",
			Kind: "type",
			Fields: []SchemaField{
				{Name: "regulation", Type: TypeString},
				{Name: "score", Type: TypeInt},
				{Name: "violationCount", Type: TypeInt},
				{Name: "status", Type: TypeString},
			},
		},
	}

	// Queries
	schema.Queries = []QueryDef{
		{Name: "topics", Description: "List all topics", ReturnType: "[Topic]", Category: "cluster"},
		{Name: "topic", Description: "Get topic details", ReturnType: "Topic", Category: "cluster",
			Args: []SchemaField{{Name: "name", Type: TypeString, Required: true}}},
		{Name: "consumerGroups", Description: "List consumer groups", ReturnType: "[ConsumerGroup]", Category: "cluster"},
		{Name: "messages", Description: "Consume messages from a topic", ReturnType: "[Message]", Category: "data",
			Args: []SchemaField{
				{Name: "topic", Type: TypeString, Required: true},
				{Name: "limit", Type: TypeInt},
				{Name: "offset", Type: TypeString},
			}},
		{Name: "clusterHealth", Description: "Get cluster health status", ReturnType: "ClusterHealth", Category: "monitoring"},
		{Name: "complianceScan", Description: "Run compliance scan", ReturnType: "ComplianceResult", Category: "governance",
			Args: []SchemaField{{Name: "regulation", Type: TypeString, Required: true}}},
	}

	// Mutations
	schema.Mutations = []QueryDef{
		{Name: "createTopic", Description: "Create a new topic", ReturnType: "Topic", Category: "cluster",
			Args: []SchemaField{
				{Name: "name", Type: TypeString, Required: true},
				{Name: "partitions", Type: TypeInt},
				{Name: "replicationFactor", Type: TypeInt},
			}},
		{Name: "publishMessage", Description: "Publish a message to a topic", ReturnType: "Message", Category: "data",
			Args: []SchemaField{
				{Name: "topic", Type: TypeString, Required: true},
				{Name: "key", Type: TypeString},
				{Name: "value", Type: TypeString, Required: true},
			}},
	}

	// Subscriptions
	schema.Subscriptions = []SubscriptionDef{
		{Name: "onMessage", Description: "Subscribe to new messages on a topic", ReturnType: "Message",
			Args: []SchemaField{{Name: "topic", Type: TypeString, Required: true}}},
		{Name: "onConsumerLag", Description: "Subscribe to consumer lag updates", ReturnType: "ConsumerGroup",
			Args: []SchemaField{{Name: "groupId", Type: TypeString}}},
		{Name: "onClusterHealth", Description: "Subscribe to cluster health changes", ReturnType: "ClusterHealth"},
		{Name: "onComplianceAlert", Description: "Subscribe to compliance violation alerts", ReturnType: "ComplianceResult"},
	}

	return schema
}

func (e *Engine) renderSDL() string {
	var b []byte
	b = append(b, "# KafkaMCP GraphQL Schema\n# Auto-generated\n\n"...)

	for _, t := range e.schema.Types {
		b = append(b, fmt.Sprintf("\"\"\" %s \"\"\"\n", t.Description)...)
		b = append(b, fmt.Sprintf("type %s {\n", t.Name)...)
		for _, f := range t.Fields {
			typeName := string(f.Type)
			if f.Required {
				typeName += "!"
			}
			b = append(b, fmt.Sprintf("  %s: %s\n", f.Name, typeName)...)
		}
		b = append(b, "}\n\n"...)
	}

	b = append(b, "type Query {\n"...)
	for _, q := range e.schema.Queries {
		args := renderArgs(q.Args)
		b = append(b, fmt.Sprintf("  \"\"\"%s\"\"\"\n", q.Description)...)
		b = append(b, fmt.Sprintf("  %s%s: %s\n", q.Name, args, q.ReturnType)...)
	}
	b = append(b, "}\n\n"...)

	b = append(b, "type Mutation {\n"...)
	for _, m := range e.schema.Mutations {
		args := renderArgs(m.Args)
		b = append(b, fmt.Sprintf("  %s%s: %s\n", m.Name, args, m.ReturnType)...)
	}
	b = append(b, "}\n\n"...)

	b = append(b, "type Subscription {\n"...)
	for _, s := range e.schema.Subscriptions {
		args := renderArgs(s.Args)
		b = append(b, fmt.Sprintf("  %s%s: %s\n", s.Name, args, s.ReturnType)...)
	}
	b = append(b, "}\n"...)

	return string(b)
}

func renderArgs(args []SchemaField) string {
	if len(args) == 0 {
		return ""
	}
	parts := make([]string, 0, len(args))
	for _, a := range args {
		typeName := string(a.Type)
		if a.Required {
			typeName += "!"
		}
		parts = append(parts, fmt.Sprintf("%s: %s", a.Name, typeName))
	}
	return "(" + strings.Join(parts, ", ") + ")"
}
