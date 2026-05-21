package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/gateway"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerGatewayTools() {
	if s.gatewayManager == nil {
		return
	}

	s.addTool(mcp.NewTool("gateway_list_backends",
		withCategory("streaming"),
		mcp.WithDescription("List all registered multi-protocol streaming backends (Pulsar, RabbitMQ, NATS) and their health status."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "gateway_list_backends",
		resourceType: "cluster",
		action:       "read",
	}, s.handleGatewayListBackends))

	s.addTool(mcp.NewTool("gateway_produce",
		withCategory("streaming"),
		mcp.WithDescription("Produce a message through a named streaming backend (Pulsar, RabbitMQ, NATS)."),
		mcp.WithString("backend", mcp.Required(), mcp.Description("Backend name")),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic/queue/subject name")),
		mcp.WithString("key", mcp.Description("Message key")),
		mcp.WithString("value", mcp.Required(), mcp.Description("Message value")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "gateway_produce",
		resourceType: "topic",
		action:       "write",
		resourceName: func(r mcp.CallToolRequest) string {
			if t, ok := r.GetArguments()["topic"].(string); ok {
				return t
			}
			return "*"
		},
	}, s.handleGatewayProduce))

	s.addTool(mcp.NewTool("gateway_consume",
		withCategory("streaming"),
		mcp.WithDescription("Consume messages from a named streaming backend (Pulsar, RabbitMQ, NATS)."),
		mcp.WithString("backend", mcp.Required(), mcp.Description("Backend name")),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic/queue/subject name")),
		mcp.WithString("group", mcp.Description("Consumer group/subscription name")),
		mcp.WithNumber("limit", mcp.Description("Max messages to consume. Default: 10")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "gateway_consume",
		resourceType: "topic",
		action:       "read",
		resourceName: func(r mcp.CallToolRequest) string {
			if t, ok := r.GetArguments()["topic"].(string); ok {
				return t
			}
			return "*"
		},
	}, s.handleGatewayConsume))

	s.addTool(mcp.NewTool("gateway_list_topics",
		withCategory("streaming"),
		mcp.WithDescription("List topics/queues/subjects on a named streaming backend."),
		mcp.WithString("backend", mcp.Required(), mcp.Description("Backend name")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "gateway_list_topics",
		resourceType: "topic",
		action:       "read",
	}, s.handleGatewayListTopics))

	s.addTool(mcp.NewTool("gateway_create_topic",
		withCategory("streaming"),
		mcp.WithDescription("Create a topic/queue/subject on a named streaming backend."),
		mcp.WithString("backend", mcp.Required(), mcp.Description("Backend name")),
		mcp.WithString("topic", mcp.Required(), mcp.Description("Topic/queue/subject name")),
		mcp.WithNumber("partitions", mcp.Description("Number of partitions. Default: 1")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "gateway_create_topic",
		resourceType: "topic",
		action:       "create",
	}, s.handleGatewayCreateTopic))
}

func (s *Server) handleGatewayListBackends(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	backends := s.gatewayManager.ListBackends()
	s.logAudit(agentID, "gateway_list_backends", args, "", "success", len(backends), nil, start)
	return marshalResult(map[string]any{
		"backends": backends,
		"total":    len(backends),
	})
}

func (s *Server) handleGatewayProduce(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	backendName, _ := args["backend"].(string)
	topic, _ := args["topic"].(string)
	key, _ := args["key"].(string)
	value, _ := args["value"].(string)

	if backendName == "" || topic == "" || value == "" {
		return mcp.NewToolResultError("backend, topic, and value are required"), nil
	}

	result, err := s.gatewayManager.Produce(ctx, backendName, gateway.ProduceRequest{
		Topic: topic,
		Key:   key,
		Value: []byte(value),
	})
	if err != nil {
		s.logAudit(agentID, "gateway_produce", args, "", "error", 0, err, start)
		return s.toolError("failed to produce via gateway", err), nil
	}

	s.logAudit(agentID, "gateway_produce", args, "", "success", 1, nil, start)
	return marshalResult(result)
}

func (s *Server) handleGatewayConsume(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	backendName, _ := args["backend"].(string)
	topic, _ := args["topic"].(string)
	group, _ := args["group"].(string)

	if backendName == "" || topic == "" {
		return mcp.NewToolResultError("backend and topic are required"), nil
	}

	limit := 10
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
		if limit > 1000 {
			limit = 1000
		}
	}

	messages, err := s.gatewayManager.Consume(ctx, backendName, gateway.ConsumeOptions{
		Topic: topic,
		Group: group,
		Limit: limit,
	})
	if err != nil {
		s.logAudit(agentID, "gateway_consume", args, "", "error", 0, err, start)
		return s.toolError("failed to consume via gateway", err), nil
	}

	s.logAudit(agentID, "gateway_consume", args, "", "success", len(messages), nil, start)
	return marshalResult(map[string]any{
		"messages": messages,
		"count":    len(messages),
	})
}

func (s *Server) handleGatewayListTopics(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	backendName, _ := args["backend"].(string)
	if backendName == "" {
		return mcp.NewToolResultError("backend is required"), nil
	}

	topics, err := s.gatewayManager.ListTopics(ctx, backendName)
	if err != nil {
		s.logAudit(agentID, "gateway_list_topics", args, "", "error", 0, err, start)
		return s.toolError("failed to list gateway topics", err), nil
	}

	s.logAudit(agentID, "gateway_list_topics", args, "", "success", len(topics), nil, start)
	return marshalResult(map[string]any{
		"topics": topics,
		"total":  len(topics),
	})
}

func (s *Server) handleGatewayCreateTopic(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	backendName, _ := args["backend"].(string)
	topic, _ := args["topic"].(string)
	if backendName == "" || topic == "" {
		return mcp.NewToolResultError("backend and topic are required"), nil
	}

	partitions := 1
	if p, ok := args["partitions"].(float64); ok && p > 0 {
		partitions = int(p)
	}
	if partitions > MaxPartitions {
		return mcp.NewToolResultError("partitions must be <= 10000"), nil
	}

	err := s.gatewayManager.CreateTopic(ctx, backendName, topic, partitions, nil)
	if err != nil {
		s.logAudit(agentID, "gateway_create_topic", args, "", "error", 0, err, start)
		return s.toolError("failed to create gateway topic", err), nil
	}

	s.logAudit(agentID, "gateway_create_topic", args, "", "success", 1, nil, start)
	return marshalResult(map[string]any{
		"status":  "created",
		"topic":   topic,
		"backend": backendName,
	})
}
