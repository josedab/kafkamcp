package graphqlapi

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

func TestGetSchema(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	schema := e.GetSchema()
	if len(schema.Types) == 0 {
		t.Error("expected types in schema")
	}
	if len(schema.Queries) == 0 {
		t.Error("expected queries")
	}
	if len(schema.Mutations) == 0 {
		t.Error("expected mutations")
	}
	if len(schema.Subscriptions) == 0 {
		t.Error("expected subscriptions")
	}
	if schema.GeneratedAt == "" {
		t.Error("expected generated_at timestamp")
	}
}

func TestGetSchemaSDL(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	sdl := e.GetSchemaSDL()
	if sdl == "" {
		t.Error("expected non-empty SDL")
	}
	if !strings.Contains(sdl, "type Topic") {
		t.Error("expected Topic type in SDL")
	}
	if !strings.Contains(sdl, "type Query") {
		t.Error("expected Query type in SDL")
	}
	if !strings.Contains(sdl, "type Mutation") {
		t.Error("expected Mutation type in SDL")
	}
	if !strings.Contains(sdl, "type Subscription") {
		t.Error("expected Subscription type in SDL")
	}
	// Check SDL has proper field syntax
	if !strings.Contains(sdl, "name: String!") {
		t.Error("expected required field syntax in SDL")
	}
}

func TestExecuteQuery(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	// Valid query
	result := e.ExecuteQuery("{ topics { name } }", nil)
	if result.Data == nil {
		t.Error("expected data for valid query")
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors, got %v", result.Errors)
	}

	// Empty query
	result = e.ExecuteQuery("", nil)
	if len(result.Errors) == 0 {
		t.Error("expected error for empty query")
	}

	// Introspection
	result = e.ExecuteQuery("{ __schema { types } }", nil)
	if result.Data == nil {
		t.Error("expected introspection data")
	}

	// __type introspection
	result = e.ExecuteQuery("{ __type(name: \"Topic\") { name } }", nil)
	if result.Data == nil {
		t.Error("expected __type introspection data")
	}
}

func TestExecuteQueryValidation(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	// Invalid: doesn't start with valid prefix
	result := e.ExecuteQuery("SELECT * FROM topics", nil)
	if len(result.Errors) == 0 {
		t.Error("expected error for SQL-like query")
	}

	// Invalid: unbalanced braces
	result = e.ExecuteQuery("{ topics { name }", nil)
	if len(result.Errors) == 0 {
		t.Error("expected error for unbalanced braces")
	}

	// Valid: query keyword prefix
	result = e.ExecuteQuery("query { topics { name } }", nil)
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors for query prefix, got %v", result.Errors)
	}

	// Valid: mutation prefix
	result = e.ExecuteQuery("mutation { createTopic(name: \"test\") { name } }", nil)
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors for mutation prefix, got %v", result.Errors)
	}
}

func TestExecuteQueryWithResolver(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	e.SetToolResolver(func(toolName string, args map[string]any) (any, error) {
		return map[string]any{"resolved": true, "tool": toolName}, nil
	})

	result := e.ExecuteQuery("{ topics { name } }", nil)
	if result.Data == nil {
		t.Error("expected resolved data")
	}
}

func TestExecuteQueryResolverError(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	e.SetToolResolver(func(toolName string, args map[string]any) (any, error) {
		return nil, fmt.Errorf("resolver error")
	})

	result := e.ExecuteQuery("{ topics { name } }", nil)
	if len(result.Errors) == 0 {
		t.Error("expected error from failing resolver")
	}
}

func TestSubscriptions(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	sub, err := e.Subscribe("onMessage", "orders")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !sub.Active {
		t.Error("expected active subscriber")
	}
	if sub.SubscriptionName != "onMessage" {
		t.Errorf("expected onMessage, got %s", sub.SubscriptionName)
	}

	subs := e.ListSubscribers()
	if len(subs) != 1 {
		t.Errorf("expected 1 subscriber, got %d", len(subs))
	}

	if err := e.Unsubscribe(sub.ID); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	subs = e.ListSubscribers()
	if len(subs) != 0 {
		t.Errorf("expected 0 subscribers after unsubscribe, got %d", len(subs))
	}
}

func TestSubscribeInvalid(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	if _, err := e.Subscribe("nonexistent", ""); err == nil {
		t.Error("expected error for invalid subscription")
	}
}

func TestUnsubscribeInvalid(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	if err := e.Unsubscribe("nonexistent"); err == nil {
		t.Error("expected error for unsubscribing nonexistent subscriber")
	}
}

func TestMultipleSubscriptions(t *testing.T) {
	t.Parallel()
	e := NewEngine(slog.Default())

	e.Subscribe("onMessage", "orders")
	e.Subscribe("onMessage", "events")
	e.Subscribe("onClusterHealth", "")

	subs := e.ListSubscribers()
	if len(subs) != 3 {
		t.Errorf("expected 3 subscribers, got %d", len(subs))
	}
}
