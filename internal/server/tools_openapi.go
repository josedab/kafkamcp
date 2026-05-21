package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerOpenAPITools() {
	s.addTool(mcp.NewTool("kafka_openapi_spec",
		mcp.WithDescription("Generate an OpenAPI 3.0 specification from all registered MCP tools. Returns JSON-formatted OpenAPI spec for SDK code generation."),
		mcp.WithString("format", mcp.Description("Output format: json, yaml. Default: json")),
	), s.handleOpenAPISpec)
}

func (s *Server) handleOpenAPISpec(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_openapi_spec", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		spec := map[string]any{
			"openapi": "3.0.3",
			"info": map[string]any{
				"title":       "KafkaMCP API",
				"description": "Auto-generated OpenAPI specification from KafkaMCP MCP tools",
				"version":     "1.0.0",
				"license":     map[string]string{"name": "Apache-2.0"},
			},
			"servers": []map[string]string{
				{"url": "http://localhost:9092", "description": "Local KafkaMCP server"},
			},
			"paths": s.generateOpenAPIPaths(),
			"components": map[string]any{
				"securitySchemes": map[string]any{
					"agentId": map[string]any{
						"type": "apiKey",
						"in":   "header",
						"name": "X-Agent-ID",
					},
				},
			},
		}

		s.logAudit(ctx, agentID, "kafka_openapi_spec", args, "", "success", 1, nil, start)
		return marshalResult(spec)
	})(ctx, request)
}

func (s *Server) generateOpenAPIPaths() map[string]any {
	paths := make(map[string]any)

	toolDefs := []struct {
		path        string
		summary     string
		description string
	}{
		{"/tools/kafka_list_topics", "List Topics", "List all Kafka topics with optional filter"},
		{"/tools/kafka_describe_topic", "Describe Topic", "Get detailed information about a topic"},
		{"/tools/kafka_consume", "Consume Messages", "Read messages from a topic"},
		{"/tools/kafka_produce", "Produce Messages", "Send messages to a topic"},
		{"/tools/kafka_search", "Search Messages", "Search messages by content"},
		{"/tools/kafka_list_consumer_groups", "List Consumer Groups", "List all consumer groups"},
		{"/tools/kafka_describe_consumer_group", "Describe Consumer Group", "Get consumer group details"},
		{"/tools/kafka_list_schemas", "List Schemas", "List schema registry subjects"},
		{"/tools/kafka_describe_schema", "Describe Schema", "Get schema details"},
		{"/tools/kafka_create_topic", "Create Topic", "Create a new topic"},
		{"/tools/kafka_delete_topic", "Delete Topic", "Delete a topic"},
		{"/tools/kafka_reset_offsets", "Reset Offsets", "Reset consumer group offsets"},
		{"/tools/kafka_nl_query", "Natural Language Query", "Query Kafka with natural language"},
		{"/tools/kafka_anomalies", "Get Anomalies", "Detect anomalies in Kafka metrics"},
		{"/tools/kafka_advisor_scan", "Advisor Scan", "Scan topics for best practices"},
		{"/tools/kafka_advisor_report", "Advisor Report", "Generate advisory report"},
		{"/tools/kafka_flow_debug", "Flow Debug", "Live message flow debugging"},
		{"/tools/kafka_flow_bottlenecks", "Flow Bottlenecks", "Detect flow bottlenecks"},
		{"/tools/kafka_chaos_inject", "Chaos Inject", "Inject chaos experiments"},
		{"/tools/kafka_chaos_status", "Chaos Status", "Get chaos experiment status"},
		{"/tools/kafka_chaos_stop", "Chaos Stop", "Stop chaos experiment"},
		{"/tools/kafka_migration_plan", "Migration Plan", "Plan cluster migration"},
		{"/tools/kafka_migrate_execute", "Migration Execute", "Execute migration plan"},
		{"/tools/kafka_migrate_status", "Migration Status", "Get migration status"},
		{"/tools/kafka_migrate_rollback", "Migration Rollback", "Rollback migration"},
	}

	for _, td := range toolDefs {
		paths[td.path] = map[string]any{
			"post": map[string]any{
				"summary":     td.summary,
				"description": td.description,
				"operationId": td.path[7:], // strip /tools/
				"requestBody": map[string]any{
					"content": map[string]any{
						"application/json": map[string]any{
							"schema": map[string]string{"type": "object"},
						},
					},
				},
				"responses": map[string]any{
					"200": map[string]any{
						"description": "Successful operation",
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]string{"type": "object"},
							},
						},
					},
				},
				"security": []map[string]any{{"agentId": []string{}}},
			},
		}
	}

	return paths
}
