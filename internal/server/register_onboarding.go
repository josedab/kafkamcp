package server

import (
	"context"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerOnboardingTools() {
	if s.onboardingEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_quickstart",
		mcp.WithDescription("Guided onboarding: auto-discovers Kafka clusters, suggests configuration, and generates a step-by-step getting started guide."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: discover (find clusters), suggest_rbac (recommend RBAC), guide (full quickstart guide)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_quickstart",
		resourceType: "cluster",
		action:       "read",
	}, s.handleQuickstart))

	s.mcpServer.AddTool(mcp.NewTool("kafka_discover_clusters",
		mcp.WithDescription("Auto-discover Kafka clusters by scanning common ports, environment variables, and DNS records."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_discover_clusters",
		resourceType: "cluster",
		action:       "read",
	}, s.handleDiscoverClusters))

	s.mcpServer.AddTool(mcp.NewTool("kafka_suggest_rbac",
		mcp.WithDescription("Generate recommended RBAC configuration based on your Kafka topology. Outputs ready-to-use YAML config."),
		mcp.WithString("cluster", mcp.Description("Cluster to analyze")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_suggest_rbac",
		resourceType: "cluster",
		action:       "read",
	}, s.handleSuggestRBAC))
}

func (s *Server) handleQuickstart(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)

	switch action {
	case "discover":
		return s.handleDiscoverClusters(ctx, request, agentID, args, start)
	case "suggest_rbac":
		return s.handleSuggestRBAC(ctx, request, agentID, args, start)
	case "guide":
		discovery := s.onboardingEngine.DiscoverClusters(ctx)
		clusterFound := len(discovery.Clusters) > 0

		topicCount := 0
		if topics, err := s.kafkaManager.ListTopics(ctx, "", false); err == nil {
			topicCount = len(topics)
		}

		guide := s.onboardingEngine.GenerateQuickstart(clusterFound, topicCount)
		s.logAudit(agentID, "kafka_quickstart", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{
			"guide":     guide,
			"discovery": discovery,
		})
	default:
		return mcp.NewToolResultError("action must be: discover, suggest_rbac, or guide"), nil
	}
}

func (s *Server) handleDiscoverClusters(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	result := s.onboardingEngine.DiscoverClusters(ctx)
	s.logAudit(agentID, "kafka_discover_clusters", args, "", "success", len(result.Clusters), nil, start)
	return marshalResult(result)
}

func (s *Server) handleSuggestRBAC(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	cluster, _ := args["cluster"].(string)
	cluster = s.resolveClusterName(cluster)

	var topicNames []string
	if topics, err := s.kafkaManager.ListTopics(ctx, cluster, false); err == nil {
		for _, t := range topics {
			topicNames = append(topicNames, t.Name)
		}
	}

	suggestion := s.onboardingEngine.SuggestRBAC(topicNames)
	s.logAudit(agentID, "kafka_suggest_rbac", args, cluster, "success", len(suggestion.AgentPolicies), nil, start)
	return marshalResult(suggestion)
}
