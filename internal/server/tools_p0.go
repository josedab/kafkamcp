package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/josedab/kafkamcp/internal/auth"
	"github.com/josedab/kafkamcp/internal/kafka"
	"github.com/josedab/kafkamcp/internal/metrics"
	"github.com/josedab/kafkamcp/internal/schema"
)

// ===================== P0 TOOL HANDLERS =====================

// handleListTopics returns all Kafka topics, optionally filtered by name pattern and agent authorization.
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
			s.logAudit(ctx, agentID, "kafka_list_topics", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to list topics", err), nil
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
		s.logAudit(ctx, agentID, "kafka_list_topics", args, cluster, "success", len(topics), nil, start)
		return marshalResult(result)
	})(ctx, request)
}

// handleDescribeTopic returns detailed metadata for a single Kafka topic including partitions and configs.
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
			s.logAudit(ctx, agentID, "kafka_describe_topic", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to describe topic", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_describe_topic", args, cluster, "success", 0, nil, start)
		return marshalResult(detail)
	})(ctx, request)
}

// handleConsume reads messages from a Kafka topic with configurable offset, limit, partition, and deserializer.
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
		if deny := validateStringLength("deserializer", deserializer); deny != nil {
			return deny, nil
		}
		if deny := validateEnum("deserializer", deserializer, validDeserializers); deny != nil {
			return deny, nil
		}
		messages, err := s.kafkaManager.Consume(ctx, cluster, topic, partition, offset, limit, timeoutMs, deserializer)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_consume", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to consume", err), nil
		}

		if err := s.maskMessages(agentID, messages); err != nil {
			s.logAudit(ctx, agentID, "kafka_consume", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to apply data masking", err), nil
		}

		s.metrics.MessagesConsumed.WithLabelValues(metrics.SanitizeTopicLabel(topic), metrics.SanitizeAgentLabel(agentID), s.resolveClusterName(cluster)).Add(float64(len(messages)))
		s.logAudit(ctx, agentID, "kafka_consume", args, cluster, "success", len(messages), nil, start)
		return marshalResult(map[string]any{"messages": messages, "count": len(messages), "topic": topic, "deserialization": deserializer})
	})(ctx, request)
}

// handleProduce writes one or more messages to a Kafka topic with optional schema-aware serialization.
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
			return s.toolError("Invalid messages", err), nil
		}
		if err := json.Unmarshal(msgBytes, &messages); err != nil {
			var single kafka.ProduceMessage
			if err := json.Unmarshal(msgBytes, &single); err != nil {
				return s.toolError("Invalid messages format", err), nil
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
					return s.toolError(fmt.Sprintf("message %d: failed to marshal value", i), err), nil
				}
				if len(b) > maxSize {
					return mcp.NewToolResultError(fmt.Sprintf("message %d exceeds maximum size of %d bytes", i, maxSize)), nil
				}
			}
		}

		acks := request.GetString("acks", "all")
		if deny := validateStringLength("acks", acks); deny != nil {
			return deny, nil
		}
		if deny := validateEnum("acks", acks, validAcks); deny != nil {
			return deny, nil
		}
		deliveries, err := s.kafkaManager.Produce(ctx, cluster, topic, messages, kafka.ProduceOptions{
			Acks: acks,
		})
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_produce", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to produce", err), nil
		}

		s.metrics.MessagesProduced.WithLabelValues(metrics.SanitizeTopicLabel(topic), metrics.SanitizeAgentLabel(agentID), s.resolveClusterName(cluster)).Add(float64(len(deliveries)))
		s.logAudit(ctx, agentID, "kafka_produce", args, cluster, "success", len(deliveries), nil, start)
		return marshalResult(map[string]any{"deliveries": deliveries, "count": len(deliveries), "topic": topic, "serialization": "json-or-string", "acks": acks})
	})(ctx, request)
}

// handleListSchemas returns all Schema Registry subjects, optionally filtered by name pattern.
func (s *Server) handleListSchemas(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_list_schemas", resourceType: auth.ResourceSchema, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		filter := request.GetString("filter", "")

		if deny := validateStringLength("filter", filter); deny != nil {
			return deny, nil
		}

		sc := s.getSchemaClient("")
		if sc == nil {
			s.logAudit(ctx, agentID, "kafka_list_schemas", args, "", "error", 0, fmt.Errorf("no schema registry configured"), start)
			return mcp.NewToolResultError("No schema registry configured for this cluster"), nil
		}

		subjects, err := sc.ListSubjectsDetailed(ctx)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_list_schemas", args, "", "error", 0, err, start)
			return s.toolError("Failed to list schemas", err), nil
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

		s.logAudit(ctx, agentID, "kafka_list_schemas", args, "", "success", len(subjects), nil, start)
		return marshalResult(map[string]any{"subjects": subjects, "total_count": len(subjects)})
	})(ctx, request)
}

// handleGetSchema retrieves a schema by subject and version from the Schema Registry.
func (s *Server) handleGetSchema(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_get_schema", resourceType: auth.ResourceSchema, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		subject := request.GetString("subject", "")
		version := request.GetString("version", "latest")

		if subject == "" {
			return mcp.NewToolResultError("subject is required"), nil
		}
		if deny := validateStringLength("subject", subject); deny != nil {
			return deny, nil
		}

		info, err := s.getSchemaClient("").GetSchema(ctx, subject, version)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_get_schema", args, "", "error", 0, err, start)
			return s.toolError("Failed to get schema", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_get_schema", args, "", "success", 0, nil, start)
		return marshalResult(info)
	})(ctx, request)
}

// handleDescribeConsumerGroup returns detailed state and member assignments for a consumer group.
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
		if deny := validateKafkaIdentifier("group_id", groupID); deny != nil {
			return deny, nil
		}

		detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, cluster, groupID)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_describe_consumer_group", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to describe group", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_describe_consumer_group", args, cluster, "success", 0, nil, start)
		return marshalResult(detail)
	})(ctx, request)
}

// handleListConsumerGroups returns all consumer groups, optionally filtered by state.
func (s *Server) handleListConsumerGroups(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_list_consumer_groups", resourceType: auth.ResourceConsumerGroup, action: auth.ActionDescribe,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		stateFilter := request.GetString("state_filter", "")
		cluster := request.GetString("cluster", "")

		if deny := validateStringLength("state_filter", stateFilter); deny != nil {
			return deny, nil
		}

		groups, err := s.kafkaManager.ListConsumerGroups(ctx, cluster)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_list_consumer_groups", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to list groups", err), nil
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

		s.logAudit(ctx, agentID, "kafka_list_consumer_groups", args, cluster, "success", len(groups), nil, start)
		return marshalResult(map[string]any{"groups": groups, "total_count": len(groups), "cluster": s.resolveClusterName(cluster)})
	})(ctx, request)
}
