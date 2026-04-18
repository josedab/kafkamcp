package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== MULTI-AGENT COLLABORATION HANDLERS =====================

// handleAgentRegistry manages multi-agent collaboration primitives.
func (s *Server) handleAgentRegistry(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	args := request.GetArguments()
	action := request.GetString("action", "")

	if deny := s.checkRateLimit(agentID, "kafka_agent_registry"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_agent_registry", auth.ResourceCluster, "", auth.ActionRead); deny != nil {
		return deny, nil
	}

	if s.agentRegistry == nil {
		return mcp.NewToolResultError("agent registry not initialized"), nil
	}

	switch action {
	case "register":
		name := request.GetString("agent_name", agentID)
		agent := s.agentRegistry.RegisterAgent(agentID, name)
		s.logAudit(agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"status": "registered", "agent": agent})

	case "heartbeat":
		if err := s.agentRegistry.Heartbeat(agentID); err != nil {
			s.logAudit(agentID, "kafka_agent_registry", args, "", "error", 0, err, start)
			return mcp.NewToolResultError(err.Error()), nil
		}
		s.logAudit(agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"status": "heartbeat_updated"})

	case "claim":
		topic := request.GetString("topic", "")
		partition := int32(request.GetFloat("partition", -1))
		if topic == "" || partition < 0 {
			return mcp.NewToolResultError("topic and partition are required for claim"), nil
		}
		if err := s.agentRegistry.ClaimPartition(agentID, topic, partition); err != nil {
			s.logAudit(agentID, "kafka_agent_registry", args, "", "error", 0, err, start)
			return mcp.NewToolResultError(err.Error()), nil
		}
		s.logAudit(agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"status": "claimed", "topic": topic, "partition": partition})

	case "release":
		topic := request.GetString("topic", "")
		partition := int32(request.GetFloat("partition", -1))
		if topic == "" || partition < 0 {
			return mcp.NewToolResultError("topic and partition are required for release"), nil
		}
		if err := s.agentRegistry.ReleasePartition(agentID, topic, partition); err != nil {
			s.logAudit(agentID, "kafka_agent_registry", args, "", "error", 0, err, start)
			return mcp.NewToolResultError(err.Error()), nil
		}
		s.logAudit(agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"status": "released", "topic": topic, "partition": partition})

	case "share_cursor":
		topic := request.GetString("topic", "")
		partition := int32(request.GetFloat("partition", -1))
		offset := int64(request.GetFloat("offset", -1))
		if topic == "" || partition < 0 || offset < 0 {
			return mcp.NewToolResultError("topic, partition, and offset are required for share_cursor"), nil
		}
		cursor := s.agentRegistry.ShareCursor(agentID, topic, partition, offset)
		s.logAudit(agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"status": "cursor_shared", "cursor": cursor})

	case "handoff":
		targetAgent := request.GetString("target_agent", "")
		msgData := request.GetString("message_data", "")
		if targetAgent == "" {
			return mcp.NewToolResultError("target_agent is required for handoff"), nil
		}
		msg, err := s.agentRegistry.QueueHandoff(agentID, targetAgent, msgData)
		if err != nil {
			s.logAudit(agentID, "kafka_agent_registry", args, "", "error", 0, err, start)
			return mcp.NewToolResultError(err.Error()), nil
		}
		s.logAudit(agentID, "kafka_agent_registry", args, "", "success", 0, nil, start)
		return marshalResult(map[string]any{"status": "handoff_queued", "handoff": msg})

	case "list_agents":
		agents := s.agentRegistry.ListAgents()
		cursors := s.agentRegistry.GetCursors()
		pending := s.agentRegistry.PendingHandoffs(agentID)
		s.logAudit(agentID, "kafka_agent_registry", args, "", "success", len(agents), nil, start)
		return marshalResult(map[string]any{
			"agents":           agents,
			"shared_cursors":   cursors,
			"pending_handoffs": pending,
			"total_agents":     len(agents),
		})

	default:
		return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: register, heartbeat, claim, release, share_cursor, handoff, list_agents", action)), nil
	}
}

// ===================== KAFKA CONNECT MANAGEMENT HANDLERS =====================

// handleCreateConnector creates a new Kafka Connect connector.
func (s *Server) handleCreateConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	if deny := s.checkRateLimit(agentID, "kafka_create_connector"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_create_connector", auth.ResourceCluster, "", auth.ActionAdmin); deny != nil {
		return deny, nil
	}

	clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Cluster error: %v", err)), nil
	}

	client, err := s.getConnectClient(clCfg)
	if err != nil || client == nil {
		errMsg := "Kafka Connect URL not configured"
		if err != nil {
			errMsg = err.Error()
		}
		return mcp.NewToolResultError(errMsg), nil
	}

	config := extractStringMapArg(request, "config")
	if len(config) == 0 {
		return mcp.NewToolResultError("config is required"), nil
	}

	result, err := client.CreateConnector(ctx, name, config)
	if err != nil {
		s.logAudit(agentID, "kafka_create_connector", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to create connector: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_create_connector", args, cluster, "success", 0, nil, start)
	return marshalResult(result)
}

// handleUpdateConnector updates a connector's configuration.
func (s *Server) handleUpdateConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	if deny := s.checkRateLimit(agentID, "kafka_update_connector"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_update_connector", auth.ResourceCluster, "", auth.ActionAdmin); deny != nil {
		return deny, nil
	}

	clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Cluster error: %v", err)), nil
	}

	client, err := s.getConnectClient(clCfg)
	if err != nil || client == nil {
		errMsg := "Kafka Connect URL not configured"
		if err != nil {
			errMsg = err.Error()
		}
		return mcp.NewToolResultError(errMsg), nil
	}

	config := extractStringMapArg(request, "config")
	if len(config) == 0 {
		return mcp.NewToolResultError("config is required"), nil
	}

	result, err := client.UpdateConnector(ctx, name, config)
	if err != nil {
		s.logAudit(agentID, "kafka_update_connector", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to update connector: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_update_connector", args, cluster, "success", 0, nil, start)
	return marshalResult(result)
}

// handleDeleteConnector deletes a Kafka Connect connector.
func (s *Server) handleDeleteConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	if deny := s.checkRateLimit(agentID, "kafka_delete_connector"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_delete_connector", auth.ResourceCluster, "", auth.ActionAdmin); deny != nil {
		return deny, nil
	}

	clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Cluster error: %v", err)), nil
	}

	client, err := s.getConnectClient(clCfg)
	if err != nil || client == nil {
		errMsg := "Kafka Connect URL not configured"
		if err != nil {
			errMsg = err.Error()
		}
		return mcp.NewToolResultError(errMsg), nil
	}

	if err := client.DeleteConnector(ctx, name); err != nil {
		s.logAudit(agentID, "kafka_delete_connector", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to delete connector: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_delete_connector", args, cluster, "success", 0, nil, start)
	return marshalResult(map[string]any{"status": "deleted", "name": name})
}

// handleRestartConnector restarts a connector or specific tasks.
func (s *Server) handleRestartConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	if deny := s.checkRateLimit(agentID, "kafka_restart_connector"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_restart_connector", auth.ResourceCluster, "", auth.ActionAdmin); deny != nil {
		return deny, nil
	}

	clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Cluster error: %v", err)), nil
	}

	client, err := s.getConnectClient(clCfg)
	if err != nil || client == nil {
		errMsg := "Kafka Connect URL not configured"
		if err != nil {
			errMsg = err.Error()
		}
		return mcp.NewToolResultError(errMsg), nil
	}

	taskID := request.GetInt("task_id", -1)
	failedOnly := request.GetBool("failed_only", false)

	result, err := client.RestartConnector(ctx, name, taskID, failedOnly)
	if err != nil {
		s.logAudit(agentID, "kafka_restart_connector", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to restart: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_restart_connector", args, cluster, "success", 0, nil, start)
	return marshalResult(result)
}

// handleConnectorHealth computes health score for a connector.
func (s *Server) handleConnectorHealth(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	if deny := s.checkRateLimit(agentID, "kafka_connector_health"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_connector_health", auth.ResourceCluster, "", auth.ActionRead); deny != nil {
		return deny, nil
	}

	clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Cluster error: %v", err)), nil
	}

	client, err := s.getConnectClient(clCfg)
	if err != nil || client == nil {
		errMsg := "Kafka Connect URL not configured"
		if err != nil {
			errMsg = err.Error()
		}
		return mcp.NewToolResultError(errMsg), nil
	}

	health, err := client.ConnectorHealth(ctx, name)
	if err != nil {
		s.logAudit(agentID, "kafka_connector_health", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to get health: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_connector_health", args, cluster, "success", 0, nil, start)
	return marshalResult(health)
}

// extractStringMapArg extracts a map[string]string argument from a request.
func extractStringMapArg(request mcp.CallToolRequest, key string) map[string]string {
	rawArgs := request.GetRawArguments()
	m, ok := rawArgs.(map[string]any)
	if !ok {
		return nil
	}
	cfgRaw, ok := m[key]
	if !ok {
		return nil
	}
	result := make(map[string]string)
	switch v := cfgRaw.(type) {
	case map[string]any:
		for k, val := range v {
			result[k] = fmt.Sprintf("%v", val)
		}
	case map[string]string:
		return v
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// ===================== TIME-TRAVEL QUERY ENGINE HANDLERS =====================

// handleTimeTravel handles point-in-time topic queries.
func (s *Server) handleTimeTravel(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	topic := request.GetString("topic", "")
	cluster := request.GetString("cluster", "")
	action := request.GetString("action", "snapshot")
	args := request.GetArguments()

	if topic == "" {
		return mcp.NewToolResultError("topic is required"), nil
	}
	if deny := validateStringLength("topic", topic); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_time_travel"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_time_travel", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
		return deny, nil
	}

	if err := s.acquireExpensiveOp(ctx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	defer s.releaseExpensiveOp()

	maxKeys := request.GetInt("max_keys", 10000)
	if maxKeys > 100000 {
		maxKeys = 100000
	}

	switch action {
	case "snapshot":
		ts := request.GetString("timestamp", "")
		if ts == "" {
			return mcp.NewToolResultError("timestamp is required for snapshot action"), nil
		}
		result, err := s.kafkaManager.TimeTravelSnapshot(ctx, cluster, topic, ts, maxKeys)
		if err != nil {
			s.logAudit(agentID, "kafka_time_travel", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
		}
		s.logAudit(agentID, "kafka_time_travel", args, cluster, "success", result.KeyCount, nil, start)
		return marshalResult(result)

	case "diff":
		tsFrom := request.GetString("timestamp_from", "")
		tsTo := request.GetString("timestamp_to", "")
		if tsFrom == "" || tsTo == "" {
			return mcp.NewToolResultError("timestamp_from and timestamp_to are required for diff action"), nil
		}
		result, err := s.kafkaManager.TimeTravelDiff(ctx, cluster, topic, tsFrom, tsTo, maxKeys)
		if err != nil {
			s.logAudit(agentID, "kafka_time_travel", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
		}
		s.logAudit(agentID, "kafka_time_travel", args, cluster, "success", result.TotalChanges, nil, start)
		return marshalResult(result)

	default:
		return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: snapshot, diff", action)), nil
	}
}

// ===================== INTELLIGENT RECOMMENDATIONS HANDLERS =====================

// handleRecommend provides topic optimization recommendations.
func (s *Server) handleRecommend(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	topic := request.GetString("topic", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_recommend"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_recommend", auth.ResourceCluster, "", auth.ActionRead); deny != nil {
		return deny, nil
	}

	result, err := s.kafkaManager.GetRecommendations(ctx, cluster, topic)
	if err != nil {
		s.logAudit(agentID, "kafka_recommend", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_recommend", args, cluster, "success", len(result.Recommendations), nil, start)
	return marshalResult(result)
}

// ===================== MESSAGE TRACING HANDLERS =====================

// handleTraceMessage traces a message across topics by correlation ID.
func (s *Server) handleTraceMessage(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	correlationID := request.GetString("correlation_id", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if correlationID == "" {
		return mcp.NewToolResultError("correlation_id is required"), nil
	}
	if deny := validateStringLength("correlation_id", correlationID); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_trace_message"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_trace_message", auth.ResourceTopic, "*", auth.ActionRead); deny != nil {
		return deny, nil
	}

	if err := s.acquireExpensiveOp(ctx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	defer s.releaseExpensiveOp()

	headerName := request.GetString("header_name", "")
	maxScan := request.GetInt("max_scan", 10000)
	if maxScan > MaxSearchScan {
		maxScan = MaxSearchScan
	}

	// Get topics to scan
	var topicNames []string
	if rawArgs := request.GetRawArguments(); rawArgs != nil {
		if m, ok := rawArgs.(map[string]any); ok {
			if t, ok := m["topics"].([]any); ok {
				for _, v := range t {
					if s, ok := v.(string); ok {
						topicNames = append(topicNames, s)
					}
				}
			}
		}
	}

	// If no topics specified, get all
	if len(topicNames) == 0 {
		topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
		if err != nil {
			s.logAudit(agentID, "kafka_trace_message", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed to list topics: %v", err)), nil
		}
		for _, t := range topics {
			topicNames = append(topicNames, t.Name)
		}
	}

	result, err := s.kafkaManager.TraceMessage(ctx, cluster, correlationID, headerName, topicNames, maxScan)
	if err != nil {
		s.logAudit(agentID, "kafka_trace_message", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_trace_message", args, cluster, "success", len(result.Hops), nil, start)
	return marshalResult(result)
}

// ===================== ACL & SECURITY HANDLERS =====================

// handleListACLs lists Kafka ACLs with security analysis.
func (s *Server) handleListACLs(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_list_acls"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_list_acls", auth.ResourceCluster, "", auth.ActionAdmin); deny != nil {
		return deny, nil
	}

	principal := request.GetString("principal", "")
	resourceType := request.GetString("resource_type", "")
	includeAnalysis := request.GetBool("include_analysis", true)

	result, err := s.kafkaManager.ListACLs(ctx, cluster, principal, resourceType, includeAnalysis)
	if err != nil {
		s.logAudit(agentID, "kafka_list_acls", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_list_acls", args, cluster, "success", result.TotalACLs, nil, start)
	return marshalResult(result)
}

// ===================== CONFIG DRIFT HANDLERS =====================

// handleConfigDrift detects configuration drift across clusters.
func (s *Server) handleConfigDrift(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	action := request.GetString("action", "compare")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_config_drift"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_config_drift", auth.ResourceCluster, "", auth.ActionRead); deny != nil {
		return deny, nil
	}

	switch action {
	case "compare":
		sourceCluster := request.GetString("source_cluster", "")
		targetCluster := request.GetString("target_cluster", "")
		topic := request.GetString("topic", "")
		if sourceCluster == "" || targetCluster == "" {
			return mcp.NewToolResultError("source_cluster and target_cluster are required for compare"), nil
		}
		result, err := s.kafkaManager.CompareConfigs(ctx, sourceCluster, targetCluster, topic)
		if err != nil {
			s.logAudit(agentID, "kafka_config_drift", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
		}
		s.logAudit(agentID, "kafka_config_drift", args, cluster, "success", len(result.Diffs), nil, start)
		return marshalResult(result)

	case "audit":
		topic := request.GetString("topic", "")
		template := extractStringMapArg(request, "template")
		result, err := s.kafkaManager.AuditConfigs(ctx, cluster, topic, template)
		if err != nil {
			s.logAudit(agentID, "kafka_config_drift", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
		}
		s.logAudit(agentID, "kafka_config_drift", args, cluster, "success", len(result.Deviations), nil, start)
		return marshalResult(result)

	default:
		return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: compare, audit", action)), nil
	}
}

// ===================== PLAYGROUND HANDLERS =====================

// handlePlayground manages sandboxed Kafka playground environments.
func (s *Server) handlePlayground(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	action := request.GetString("action", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_playground"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_playground", auth.ResourceTopic, "_playground_*", auth.ActionCreate); deny != nil {
		return deny, nil
	}

	switch action {
	case "create":
		name := request.GetString("name", "test")
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}
		partitions := int32(request.GetInt("partitions", 3))
		if partitions > 12 {
			partitions = 12
		}
		result, err := s.kafkaManager.PlaygroundCreate(ctx, cluster, agentID, name, partitions)
		if err != nil {
			s.logAudit(agentID, "kafka_playground", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
		}
		s.logAudit(agentID, "kafka_playground", args, cluster, "success", 0, nil, start)
		return marshalResult(result)

	case "produce_sample":
		name := request.GetString("name", "test")
		pattern := request.GetString("data_pattern", "ecommerce")
		count := request.GetInt("count", 10)
		if count > 100 {
			count = 100
		}
		result, err := s.kafkaManager.PlaygroundProduceSample(ctx, cluster, agentID, name, pattern, count)
		if err != nil {
			s.logAudit(agentID, "kafka_playground", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
		}
		s.logAudit(agentID, "kafka_playground", args, cluster, "success", count, nil, start)
		return marshalResult(result)

	case "list":
		topics, err := s.kafkaManager.PlaygroundList(ctx, cluster, agentID)
		if err != nil {
			s.logAudit(agentID, "kafka_playground", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
		}
		s.logAudit(agentID, "kafka_playground", args, cluster, "success", len(topics), nil, start)
		return marshalResult(map[string]any{"playground_topics": topics, "count": len(topics)})

	case "cleanup":
		count, err := s.kafkaManager.PlaygroundCleanup(ctx, cluster, agentID)
		if err != nil {
			s.logAudit(agentID, "kafka_playground", args, cluster, "error", 0, err, start)
			return mcp.NewToolResultError(fmt.Sprintf("Failed: %v", err)), nil
		}
		s.logAudit(agentID, "kafka_playground", args, cluster, "success", count, nil, start)
		return marshalResult(map[string]any{"status": "cleaned_up", "topics_deleted": count})

	default:
		return mcp.NewToolResultError(fmt.Sprintf("unknown action %q; must be: create, produce_sample, list, cleanup", action)), nil
	}
}

// Ensure imports are used.
var _ = kafka.StreamConfig{}
