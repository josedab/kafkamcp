package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/config"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

func connectTLSOpts(cfg *config.ClusterConfig) []kafka.ConnectTLSOption {
	var opts []kafka.ConnectTLSOption
	if cfg.ConnectTLS.CAFile != "" {
		opts = append(opts, kafka.WithConnectCA(cfg.ConnectTLS.CAFile))
	}
	if cfg.ConnectTLS.InsecureSkipVerify {
		opts = append(opts, kafka.WithConnectInsecureSkipVerify(true))
	}
	return opts
}

// ===================== P2 TOOL HANDLERS =====================

// handleAggregate performs a windowed aggregation (count, sum, avg, min, max) over messages in a topic.
func (s *Server) handleAggregate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	if deny := s.checkRateLimit(agentID, "kafka_aggregate"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_aggregate", auth.ResourceTopic, topic, auth.ActionRead); deny != nil {
		return deny, nil
	}

	if err := s.acquireExpensiveOp(ctx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	defer s.releaseExpensiveOp()

	field := request.GetString("field", "")
	function := request.GetString("function", "count")
	if deny := validateStringLength("field", field); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("function", function); deny != nil {
		return deny, nil
	}

	result, err := s.kafkaManager.Aggregate(ctx, cluster, kafka.AggregateRequest{
		Topic:    topic,
		Field:    field,
		Function: function,
		WindowMs: clampInt64(int64(request.GetInt("window_ms", 300000)), 1, 3600000),
	})
	if err != nil {
		s.logAudit(agentID, "kafka_aggregate", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to aggregate: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_aggregate", args, cluster, "success", result.MessageCount, nil, start)
	return marshalResult(result)
}

// handleSuggestSchemaEvolution suggests backward-compatible schema changes for a given subject.
func (s *Server) handleSuggestSchemaEvolution(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	subject := request.GetString("subject", "")
	change := request.GetString("change", "")
	args := request.GetArguments()

	if subject == "" || change == "" {
		return mcp.NewToolResultError("subject and change are required"), nil
	}
	if deny := validateStringLength("subject", subject); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("change", change); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_suggest_schema_evolution"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_suggest_schema_evolution", auth.ResourceSchema, "", auth.ActionRead); deny != nil {
		return deny, nil
	}

	// Fetch current schema for context
	var currentSchema map[string]any
	sc := s.getSchemaClient("")
	if sc != nil {
		info, err := sc.GetSchema(ctx, subject, "latest")
		if err == nil && info != nil {
			if m, ok := info.Schema.(map[string]any); ok {
				currentSchema = m
			}
		}
	}

	suggestions := kafka.SuggestSchemaEvolution(currentSchema, change)
	s.logAudit(agentID, "kafka_suggest_schema_evolution", args, "", "success", len(suggestions), nil, start)
	return marshalResult(map[string]any{
		"subject":     subject,
		"change":      change,
		"suggestions": suggestions,
		"count":       len(suggestions),
	})
}

// handleListConnectors lists all Kafka Connect connectors for a cluster.
func (s *Server) handleListConnectors(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if deny := s.checkRateLimit(agentID, "kafka_list_connectors"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_list_connectors", auth.ResourceCluster, "", auth.ActionRead); deny != nil {
		return deny, nil
	}

	clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Cluster error: %v", err)), nil
	}

	client, err := s.getConnectClient(clCfg)
	if err != nil {
		s.logAudit(agentID, "kafka_list_connectors", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Connect client error: %v", err)), nil
	}
	if client == nil {
		return mcp.NewToolResultError("Kafka Connect URL not configured for this cluster. Set connect_url in cluster config."), nil
	}

	connectors, err := client.ListConnectors(ctx)
	if err != nil {
		s.logAudit(agentID, "kafka_list_connectors", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to list connectors: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_list_connectors", args, cluster, "success", len(connectors), nil, start)
	return marshalResult(map[string]any{"connectors": connectors, "total_count": len(connectors)})
}

// handleDescribeConnector returns the configuration and status of a single Kafka Connect connector.
func (s *Server) handleDescribeConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
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
	if deny := s.checkRateLimit(agentID, "kafka_describe_connector"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_describe_connector", auth.ResourceCluster, "", auth.ActionRead); deny != nil {
		return deny, nil
	}

	clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("Cluster error: %v", err)), nil
	}

	client, err := s.getConnectClient(clCfg)
	if err != nil {
		s.logAudit(agentID, "kafka_describe_connector", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Connect client error: %v", err)), nil
	}
	if client == nil {
		return mcp.NewToolResultError("Kafka Connect URL not configured for this cluster."), nil
	}

	detail, err := client.DescribeConnector(ctx, name)
	if err != nil {
		s.logAudit(agentID, "kafka_describe_connector", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to describe connector: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_describe_connector", args, cluster, "success", 0, nil, start)
	return marshalResult(detail)
}

// handleJoin performs a key-based join between two topics within a time window.
func (s *Server) handleJoin(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	leftTopic := request.GetString("left_topic", "")
	rightTopic := request.GetString("right_topic", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if leftTopic == "" || rightTopic == "" {
		return mcp.NewToolResultError("left_topic and right_topic are required"), nil
	}
	if deny := validateStringLength("left_topic", leftTopic); deny != nil {
		return deny, nil
	}
	if deny := validateStringLength("right_topic", rightTopic); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_join"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_join", auth.ResourceTopic, leftTopic, auth.ActionRead); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_join", auth.ResourceTopic, rightTopic, auth.ActionRead); deny != nil {
		return deny, nil
	}

	if err := s.acquireExpensiveOp(ctx); err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	defer s.releaseExpensiveOp()

	keyField := request.GetString("key_field", "")
	if deny := validateStringLength("key_field", keyField); deny != nil {
		return deny, nil
	}

	result, err := s.kafkaManager.JoinTopics(ctx, cluster, kafka.JoinRequest{
		LeftTopic:  leftTopic,
		RightTopic: rightTopic,
		KeyField:   keyField,
		WindowMs:   clampInt64(int64(request.GetInt("window_ms", 60000)), 1, 3600000),
		Limit:      int(clampInt64(int64(request.GetInt("limit", 100)), 1, MaxJoinLimit)),
	})
	if err != nil {
		s.logAudit(agentID, "kafka_join", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to join: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_join", args, cluster, "success", result.MatchCount, nil, start)
	return marshalResult(result)
}

// handleNaturalLanguageQuery translates a natural-language question into Kafka operations and executes them.
func (s *Server) handleNaturalLanguageQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	agentID := extractAgentID(request)
	query := request.GetString("query", "")
	cluster := request.GetString("cluster", "")
	args := request.GetArguments()

	if query == "" {
		return mcp.NewToolResultError("query is required"), nil
	}
	if deny := validateStringLength("query", query); deny != nil {
		return deny, nil
	}
	if deny := s.checkRateLimit(agentID, "kafka_query"); deny != nil {
		return deny, nil
	}
	if deny := s.checkAuth(agentID, "kafka_query", auth.ResourceTopic, "*", auth.ActionRead); deny != nil {
		return deny, nil
	}

	// Get topic list for matching
	topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
	if err != nil {
		s.logAudit(agentID, "kafka_query", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to list topics: %v", err)), nil
	}

	// Filter topics by agent's read permissions
	if s.config.Policies.DefaultDeny {
		var allowed []kafka.TopicSummary
		for _, t := range topics {
			if s.policyEngine.Authorize(agentID, auth.ResourceTopic, t.Name, auth.ActionRead) {
				allowed = append(allowed, t)
			}
		}
		topics = allowed
	}

	result, err := s.kafkaManager.NaturalLanguageQuery(ctx, cluster, query, topics)
	if err != nil {
		s.logAudit(agentID, "kafka_query", args, cluster, "error", 0, err, start)
		return mcp.NewToolResultError(fmt.Sprintf("Failed to execute query: %v", err)), nil
	}

	s.logAudit(agentID, "kafka_query", args, cluster, "success", result.Count, nil, start)
	return marshalResult(result)
}
