package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== P1 TOOL HANDLERS =====================

func (s *Server) handleCreateTopic(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	name := request.GetString("name", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if name == "" {
		return mcp.NewToolResultError("name is required"), nil
	}
	if deny := validateStringLength("name", name); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_create_topic"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_create_topic", auth.ResourceTopic, name, auth.ActionCreate); deny != nil {
		return deny, nil
	}

	partitions := int32(request.GetInt("partitions", 1))
	if partitions > MaxPartitions {
		return mcp.NewToolResultError("partitions must be <= 10000"), nil
	}
	replFactor := int16(request.GetInt("replication_factor", 1))
	if replFactor > MaxReplicationFactor {
		return mcp.NewToolResultError("replication_factor must be <= 10"), nil
	}

	var configs map[string]string
	if rawArgs := request.GetRawArguments(); rawArgs != nil {
		if m, ok := rawArgs.(map[string]any); ok {
			if cfgRaw, ok := m["configs"]; ok {
				b, err := json.Marshal(cfgRaw)
				if err == nil {
					if err := json.Unmarshal(b, &configs); err != nil {
						return mcp.NewToolResultError(fmt.Sprintf("Invalid configs format: %v", err)), nil
					}
				}
			}
		}
	}

	err := s.kafkaManager.CreateTopic(ctx, cluster, kafka.CreateTopicRequest{
		Name: name, Partitions: partitions, ReplicationFactor: replFactor, Configs: configs,
	})
	if err != nil {
		s.logAudit(agentID, "kafka_create_topic", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to create topic: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_create_topic", args, cluster, "success", 0, nil, start)
	return marshalResult(map[string]any{"status": "created", "topic": name, "partitions": partitions, "replication_factor": replFactor})
}

func (s *Server) handleAlterTopic(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	name := request.GetString("name", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if name == "" {
		return mcp.NewToolResultError("name is required"), nil
	}
	if deny := validateStringLength("name", name); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_alter_topic"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_alter_topic", auth.ResourceTopic, name, auth.ActionAdmin); deny != nil {
		return deny, nil
	}

	var configs map[string]string
	if rawArgs := request.GetRawArguments(); rawArgs != nil {
		if m, ok := rawArgs.(map[string]any); ok {
			if cfgRaw, ok := m["configs"]; ok {
				b, err := json.Marshal(cfgRaw)
				if err == nil {
					if err := json.Unmarshal(b, &configs); err != nil {
						return mcp.NewToolResultError(fmt.Sprintf("Invalid configs format: %v", err)), nil
					}
				}
			}
		}
	}
	if len(configs) == 0 {
		return mcp.NewToolResultError("configs is required"), nil
	}

	err := s.kafkaManager.AlterTopic(ctx, cluster, kafka.AlterTopicRequest{Name: name, Configs: configs})
	if err != nil {
		s.logAudit(agentID, "kafka_alter_topic", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to alter topic: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_alter_topic", args, cluster, "success", 0, nil, start)
	return marshalResult(map[string]any{"status": "altered", "topic": name, "configs": configs})
}

func (s *Server) handleDeleteTopic(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	name := request.GetString("name", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if name == "" {
		return mcp.NewToolResultError("name is required"), nil
	}
	if deny := validateStringLength("name", name); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_delete_topic"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_delete_topic", auth.ResourceTopic, name, auth.ActionDelete); deny != nil {
		return deny, nil
	}

	err := s.kafkaManager.DeleteTopic(ctx, cluster, name)
	if err != nil {
		s.logAudit(agentID, "kafka_delete_topic", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to delete topic: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_delete_topic", args, cluster, "success", 0, nil, start)
	return marshalResult(map[string]any{"status": "deleted", "topic": name})
}

func (s *Server) handleSearch(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	topic := request.GetString("topic", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}
	if deny := validateStringLength("topic", topic); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_search"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_search", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
		return deny, nil
	}

	if err := s.acquireExpensiveOp(ctx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	defer s.releaseExpensiveOp()

	req := kafka.SearchRequest{
		Topic:         topic,
		KeyFilter:     request.GetString("key_filter", ""),
		HeaderKey:     request.GetString("header_key", ""),
		HeaderValue:   request.GetString("header_value", ""),
		ValueContains: request.GetString("value_contains", ""),
		StartTime:     request.GetString("start_time", ""),
		EndTime:       request.GetString("end_time", ""),
		MaxScan:       request.GetInt("max_scan", DefaultSearchScan),
		Limit:         request.GetInt("limit", 10),
	}

	// Validate secondary string parameters
	for _, p := range []struct{ name, value string }{
		{"key_filter", req.KeyFilter},
		{"header_key", req.HeaderKey},
		{"header_value", req.HeaderValue},
		{"value_contains", req.ValueContains},
	} {
		if deny := validateStringLength(p.name, p.value); deny != nil {
			return deny, nil
		}
	}

	// Cap max_scan to prevent excessive resource consumption
	if req.MaxScan > MaxSearchScan {
		req.MaxScan = MaxSearchScan
	}

	matched, scanned, err := s.kafkaManager.SearchMessages(ctx, cluster, req)
	if err != nil {
		s.logAudit(agentID, "kafka_search", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to search: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_search", args, cluster, "success", len(matched), nil, start)
	return marshalResult(map[string]any{
		"messages":      matched,
		"matched_count": len(matched),
		"scanned_count": scanned,
		"topic":         topic,
	})
}

func (s *Server) handleResetOffsets(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	groupID := request.GetString("group_id", "")
	topic := request.GetString("topic", "")
	strategy := request.GetString("strategy", "")
	value := request.GetString("value", "")
	dryRun := request.GetBool("dry_run", false)
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if groupID == "" || topic == "" || strategy == "" {
		return mcp.NewToolResultError("group_id, topic, and strategy are required"), nil
	}
	if deny := validateStringLength("group_id", groupID); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("topic", topic); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_reset_offsets"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_reset_offsets", auth.ResourceConsumerGroup, "", auth.ActionReset); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("strategy", strategy); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("value", value); deny != nil {
		return deny, nil
	}

	result, err := s.kafkaManager.ResetOffsets(ctx, cluster, kafka.ResetOffsetsRequest{
		GroupID: groupID, Topic: topic, Strategy: strategy, Value: value, DryRun: dryRun,
	})
	if err != nil {
		s.logAudit(agentID, "kafka_reset_offsets", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to reset offsets: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_reset_offsets", args, cluster, "success", len(result.Offsets), nil, start)
	return marshalResult(result)
}

func (s *Server) handleCheckSchemaCompatibility(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	subject := request.GetString("subject", "")
	schemaStr := request.GetString("schema", "")
	schemaType := request.GetString("schema_type", "AVRO")
	version := request.GetString("version", "latest")
	args := request.GetArguments()

	if subject == "" || schemaStr == "" {
		return mcp.NewToolResultError("subject and schema are required"), nil
	}
	if deny := validateStringLength("subject", subject); deny != nil {
		return deny, nil
	}
	// Cap schema body at MaxSchemaSize to prevent memory exhaustion
	if len(schemaStr) > MaxSchemaSize {
		return mcp.NewToolResultError(fmt.Sprintf("schema body exceeds maximum size of %d bytes", MaxSchemaSize)), nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_check_schema_compatibility"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_check_schema_compatibility", auth.ResourceSchema, "", auth.ActionRead); deny != nil {
		return deny, nil
	}

	result, err := s.getSchemaClient("").CheckCompatibility(ctx, subject, version, schemaStr, schemaType)
	if err != nil {
		s.logAudit(agentID, "kafka_check_schema_compatibility", args, "", "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to check compatibility: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_check_schema_compatibility", args, "", "success", 0, nil, start)
	return marshalResult(result)
}

func (s *Server) handleAnalyzeDLQ(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	topic := request.GetString("topic", "")
	limit := request.GetInt("limit", 100)
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}
	if deny := validateStringLength("topic", topic); deny != nil {
		return deny, nil
	}
	if limit > MaxDLQMessages {
		limit = MaxDLQMessages
	}
	if deny := s.checkRateLimit(agentID, "kafka_analyze_dlq"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_analyze_dlq", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
		return deny, nil
	}

	result, err := s.kafkaManager.AnalyzeDLQ(ctx, cluster, topic, limit)
	if err != nil {
		s.logAudit(agentID, "kafka_analyze_dlq", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to analyze DLQ: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_analyze_dlq", args, cluster, "success", result.TotalMessages, nil, start)
	return marshalResult(result)
}

func (s *Server) handleSubscribe(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	topic := request.GetString("topic", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}
	if deny := validateStringLength("topic", topic); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_subscribe"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_subscribe", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
		return deny, nil
	}

	groupID := request.GetString("group_id", "")
	if deny := validateStringLength("group_id", groupID); deny != nil {
		return deny, nil
	}

	result, err := s.kafkaManager.Subscribe(ctx, cluster, kafka.SubscribeRequest{
		Topic:        topic,
		GroupID:      groupID,
		AutoCommit:   request.GetBool("auto_commit", true),
		MaxMessages:  request.GetInt("max_messages", 100),
		TimeoutMs:    request.GetInt("timeout_ms", 10000),
		Deserializer: request.GetString("deserializer", "auto"),
	})
	if err != nil {
		s.logAudit(agentID, "kafka_subscribe", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to subscribe: %v", err)), nil
	}

	s.metrics.MessagesConsumed.WithLabelValues(topic, agentID, s.resolveClusterName(cluster)).Add(float64(result.Count))
	s.logAudit(agentID, "kafka_subscribe", args, cluster, "success", result.Count, nil, start)
	return marshalResult(result)
}
