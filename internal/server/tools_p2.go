package server

import (
	"context"
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
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_aggregate", resourceType: auth.ResourceTopic, action: auth.ActionRead,
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

		if err := s.acquireExpensiveOp(ctx); err != nil {
			return s.toolError("operation capacity exceeded", err), nil
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
			s.logAudit(ctx, agentID, "kafka_aggregate", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to aggregate", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_aggregate", args, cluster, "success", result.MessageCount, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

// handleSuggestSchemaEvolution suggests backward-compatible schema changes for a given subject.
func (s *Server) handleSuggestSchemaEvolution(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_suggest_schema_evolution", resourceType: auth.ResourceSchema, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		subject := request.GetString("subject", "")
		change := request.GetString("change", "")

		if subject == "" || change == "" {
			return mcp.NewToolResultError("subject and change are required"), nil
		}
		if deny := validateStringLength("subject", subject); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("change", change); deny != nil {
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
		s.logAudit(ctx, agentID, "kafka_suggest_schema_evolution", args, "", "success", len(suggestions), nil, start)
		return marshalResult(map[string]any{
			"subject":     subject,
			"change":      change,
			"suggestions": suggestions,
			"count":       len(suggestions),
		})
	})(ctx, request)
}

// handleListConnectors lists all Kafka Connect connectors for a cluster.
func (s *Server) handleListConnectors(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_list_connectors", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		cluster := request.GetString("cluster", "")

		clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
		if err != nil {
			return s.toolError("Cluster error", err), nil
		}

		client, err := s.getConnectClient(clCfg)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_list_connectors", args, cluster, "error", 0, err, start)
			return s.toolError("Connect client error", err), nil
		}
		if client == nil {
			return mcp.NewToolResultError("Kafka Connect URL not configured for this cluster. Set connect_url in cluster config."), nil
		}

		connectors, err := client.ListConnectors(ctx)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_list_connectors", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to list connectors", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_list_connectors", args, cluster, "success", len(connectors), nil, start)
		return marshalResult(map[string]any{"connectors": connectors, "total_count": len(connectors)})
	})(ctx, request)
}

// handleDescribeConnector returns the configuration and status of a single Kafka Connect connector.
func (s *Server) handleDescribeConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_describe_connector", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		cluster := request.GetString("cluster", "")

		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}

		clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
		if err != nil {
			return s.toolError("Cluster error", err), nil
		}

		client, err := s.getConnectClient(clCfg)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_describe_connector", args, cluster, "error", 0, err, start)
			return s.toolError("Connect client error", err), nil
		}
		if client == nil {
			return mcp.NewToolResultError("Kafka Connect URL not configured for this cluster."), nil
		}

		detail, err := client.DescribeConnector(ctx, name)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_describe_connector", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to describe connector", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_describe_connector", args, cluster, "success", 0, nil, start)
		return marshalResult(detail)
	})(ctx, request)
}

// handleJoin performs a key-based join between two topics within a time window.
func (s *Server) handleJoin(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_join", resourceType: auth.ResourceTopic, action: auth.ActionRead,
		resourceName: func(r mcp.CallToolRequest) string { return r.GetString("left_topic", "") },
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		leftTopic := request.GetString("left_topic", "")
		rightTopic := request.GetString("right_topic", "")
		cluster := request.GetString("cluster", "")

		if leftTopic == "" || rightTopic == "" {
			return mcp.NewToolResultError("left_topic and right_topic are required"), nil
		}
		if deny := validateStringLength("left_topic", leftTopic); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("right_topic", rightTopic); deny != nil {
			return deny, nil
		}
		// Dual-topic authorization: withToolAuth already authorized the left topic,
		// but joins read from two independent topics, so we must separately authorize
		// the right topic to prevent agents from joining topics they lack access to.
		if deny := s.checkAuth(agentID, "kafka_join", auth.ResourceTopic, rightTopic, auth.ActionRead); deny != nil {
			return deny, nil
		}

		if err := s.acquireExpensiveOp(ctx); err != nil {
			return s.toolError("operation capacity exceeded", err), nil
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
			s.logAudit(ctx, agentID, "kafka_join", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to join", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_join", args, cluster, "success", result.MatchCount, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

// handleNaturalLanguageQuery translates a natural-language question into Kafka operations and executes them.
func (s *Server) handleNaturalLanguageQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_query", resourceType: auth.ResourceTopic, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		query := request.GetString("query", "")
		cluster := request.GetString("cluster", "")

		if query == "" {
			return mcp.NewToolResultError("query is required"), nil
		}
		if deny := validateStringLength("query", query); deny != nil {
			return deny, nil
		}

		// Get topic list for matching
		topics, err := s.kafkaManager.ListTopics(ctx, cluster, false)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_query", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to list topics", err), nil
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
			s.logAudit(ctx, agentID, "kafka_query", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to execute query", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_query", args, cluster, "success", result.Count, nil, start)
		return marshalResult(result)
	})(ctx, request)
}
