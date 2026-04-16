package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/kafkamcp/kafkamcp/internal/metrics"
	"github.com/kafkamcp/kafkamcp/internal/schema"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== P0 TOOL HANDLERS =====================

func (s *Server) handleListTopics(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_list_topics", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		filter := request.GetString("filter", "")
		includeInternal := request.GetBool("include_internal", false)
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("filter", filter); deny != nil {
			return deny, nil
		}

		topics, err := s.kafkaManager.ListTopics(ctx, cluster, includeInternal)
		if err != nil {
			s.logAudit(agentID, "kafka_list_topics", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to list topics: %v", err)), nil
		}

		if filter != "" {
			var filtered []kafka.TopicSummary
			for _, t := range topics {
				if matchTopicFilter(filter, t.Name) {
					filtered = append(filtered, t)
				}
			}
			topics = filtered
		}

		// Filter by auth: only return topics the agent can read
		if s.config.Policies.DefaultDeny {
			var allowed []kafka.TopicSummary
			for _, t := range topics {
				if s.policyEngine.Authorize(agentID, auth.ResourceTopic, t.Name, auth.ActionRead) {
					allowed = append(allowed, t)
				}
			}
			topics = allowed
		}

		result := map[string]any{"topics": topics, "total_count": len(topics), "cluster": s.resolveClusterName(cluster)}
		s.logAudit(agentID, "kafka_list_topics", args, cluster, "success", len(topics), nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleDescribeTopic(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_describe_topic", resourceType: auth.ResourceTopic, action: auth.ActionRead,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("topic", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")

		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}

		detail, err := s.kafkaManager.DescribeTopic(ctx, cluster, topic)
		if err != nil {
			s.logAudit(agentID, "kafka_describe_topic", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to describe topic: %v", err)), nil
		}

		s.logAudit(agentID, "kafka_describe_topic", args, cluster, "success", 0, nil, start)
		return marshalResult(detail)
	})(ctx, request)
}

func (s *Server) handleConsume(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_consume", resourceType: auth.ResourceTopic, action: auth.ActionRead,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("topic", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")
		offset := request.GetString("offset", "latest")
		limit := request.GetInt("limit", 10)
		timeoutMs := request.GetInt("timeout_ms", 5000)

		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("offset", offset); deny != nil {
			return deny, nil
		}

		partition := int32(-1)
		if p := request.GetFloat("partition", -1); p >= 0 {
			partition = int32(p)
		}
		if limit < 1 {
			limit = 1
		}
		if limit > MaxConsumeLimit {
			limit = MaxConsumeLimit
		}
		if timeoutMs > MaxConsumeTimeout {
			timeoutMs = MaxConsumeTimeout
		}

		deserializer := request.GetString("deserializer", "auto")
		messages, err := s.kafkaManager.Consume(ctx, cluster, topic, partition, offset, limit, timeoutMs, deserializer)
		if err != nil {
			s.logAudit(agentID, "kafka_consume", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to consume: %v", err)), nil
		}

		s.metrics.MessagesConsumed.WithLabelValues(topic, metrics.SanitizeAgentLabel(agentID), s.resolveClusterName(cluster)).Add(float64(len(messages)))
		s.logAudit(agentID, "kafka_consume", args, cluster, "success", len(messages), nil, start)
		return marshalResult(map[string]any{"messages": messages, "count": len(messages), "topic": topic, "deserialization": deserializer})
	})(ctx, request)
}

func (s *Server) handleProduce(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_produce", resourceType: auth.ResourceTopic, action: auth.ActionWrite,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("topic", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		topic := request.GetString("topic", "")
		cluster := request.GetString("cluster", "")

		if topic == "" {
			return mcp.NewToolResultError("topic is required"), nil
		}
		if deny := validateStringLength("topic", topic); deny != nil {
			return deny, nil
		}

		rawArgs := request.GetRawArguments()
		rawMap, ok := rawArgs.(map[string]any)
		if !ok {
			return mcp.NewToolResultError("invalid arguments"), nil
		}
		msgData, ok := rawMap["messages"]
		if !ok {
			return mcp.NewToolResultError("messages is required"), nil
		}

		var messages []kafka.ProduceMessage
		msgBytes, err := json.Marshal(msgData)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("Invalid messages: %v", err)), nil
		}
		if err := json.Unmarshal(msgBytes, &messages); err != nil {
			var single kafka.ProduceMessage
			if err := json.Unmarshal(msgBytes, &single); err != nil {
				return mcp.NewToolResultError(fmt.Sprintf("Invalid messages format: %v", err)), nil
			}
			messages = []kafka.ProduceMessage{single}
		}
		if len(messages) == 0 {
			return mcp.NewToolResultError("at least one message required"), nil
		}
		if len(messages) > MaxProduceMessages {
			return mcp.NewToolResultError("max 100 messages per call"), nil
		}

		// Validate individual message sizes
		maxSize := s.config.Server.MaxMessageSize
		if maxSize > 0 {
			for i, msg := range messages {
				b, err := json.Marshal(msg.Value)
				if err != nil {
					return mcp.NewToolResultError(fmt.Sprintf("message %d: failed to marshal value: %v", i, err)), nil
				}
				if len(b) > maxSize {
					return mcp.NewToolResultError(fmt.Sprintf("message %d exceeds maximum size of %d bytes", i, maxSize)), nil
				}
			}
		}

		serializer := request.GetString("serializer", "auto")
		acks := request.GetString("acks", "all")
		schemaID := request.GetInt("schema_id", -1)

		deliveries, err := s.kafkaManager.Produce(ctx, cluster, topic, messages, kafka.ProduceOptions{
			Acks: acks, Serializer: serializer, SchemaID: schemaID,
		})
		if err != nil {
			s.logAudit(agentID, "kafka_produce", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to produce: %v", err)), nil
		}

		s.metrics.MessagesProduced.WithLabelValues(topic, metrics.SanitizeAgentLabel(agentID), s.resolveClusterName(cluster)).Add(float64(len(deliveries)))
		s.logAudit(agentID, "kafka_produce", args, cluster, "success", len(deliveries), nil, start)
		return marshalResult(map[string]any{"deliveries": deliveries, "count": len(deliveries), "topic": topic, "serialization": serializer, "acks": acks})
	})(ctx, request)
}

func (s *Server) handleListSchemas(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	filter := request.GetString("filter", "")
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_list_schemas"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_list_schemas", auth.ResourceSchema, "", auth.ActionRead); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("filter", filter); deny != nil {
		return deny, nil
	}

	subjects, err := s.getSchemaClient("").ListSubjectsDetailed(ctx)
	if err != nil {
		s.logAudit(agentID, "kafka_list_schemas", args, "", "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to list schemas: %v", err)), nil
	}

	if filter != "" {
		var filtered []schema.Subject
		for _, subj := range subjects {
			if matchTopicFilter(filter, subj.SubjectName) {
				filtered = append(filtered, subj)
			}
		}
		subjects = filtered
	}

	s.logAudit(agentID, "kafka_list_schemas", args, "", "success", len(subjects), nil, start)
	return marshalResult(map[string]any{"subjects": subjects, "total_count": len(subjects)})
}

func (s *Server) handleGetSchema(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	subject := request.GetString("subject", "")
	version := request.GetString("version", "latest")
	args := request.GetArguments()

	if subject == "" {
		return mcp.NewToolResultError("subject is required"), nil
	}
	if deny := validateStringLength("subject", subject); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_get_schema"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_get_schema", auth.ResourceSchema, "", auth.ActionRead); deny != nil {
		return deny, nil
	}

	info, err := s.getSchemaClient("").GetSchema(ctx, subject, version)
	if err != nil {
		s.logAudit(agentID, "kafka_get_schema", args, "", "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to get schema: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_get_schema", args, "", "success", 0, nil, start)
	return marshalResult(info)
}

func (s *Server) handleDescribeConsumerGroup(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_describe_consumer_group", resourceType: auth.ResourceConsumerGroup, action: auth.ActionDescribe,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		groupID := request.GetString("group_id", "")
		cluster := request.GetString("cluster", "")

		if groupID == "" {
			return mcp.NewToolResultError("group_id is required"), nil
		}
		if deny := validateStringLength("group_id", groupID); deny != nil {
			return deny, nil
		}

		detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, groupID)
		if err != nil {
			s.logAudit(agentID, "kafka_describe_consumer_group", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to describe group: %v", err)), nil
		}

		s.logAudit(agentID, "kafka_describe_consumer_group", args, cluster, "success", 0, nil, start)
		return marshalResult(detail)
	})(ctx, request)
}

func (s *Server) handleListConsumerGroups(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	stateFilter := request.GetString("state_filter", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_list_consumer_groups"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_list_consumer_groups", auth.ResourceConsumerGroup, "", auth.ActionDescribe); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("state_filter", stateFilter); deny != nil {
		return deny, nil
	}

	groups, err := s.kafkaManager.ListConsumerGroups(ctx, cluster)
	if err != nil {
		s.logAudit(agentID, "kafka_list_consumer_groups", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to list groups: %v", err)), nil
	}

	if stateFilter != "" {
		var filtered []kafka.ConsumerGroupSummary
		for _, g := range groups {
			if strings.EqualFold(g.State, stateFilter) {
				filtered = append(filtered, g)
			}
		}
		groups = filtered
	}

	s.logAudit(agentID, "kafka_list_consumer_groups", args, cluster, "success", len(groups), nil, start)
	return marshalResult(map[string]any{"groups": groups, "total_count": len(groups), "cluster": s.resolveClusterName(cluster)})
}
