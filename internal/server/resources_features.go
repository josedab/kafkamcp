package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== FEATURE RESOURCE HANDLERS =====================

// handleAnomalyResource returns anomaly detection alerts and status.
func (s *Server) handleAnomalyResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://anomalies", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://anomalies"); err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://anomalies", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceCluster, "", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot access anomalies", agentID)
		s.logAudit(ctx, agentID, "resource:kafka://anomalies", nil, "", "error", 0, err, start)
		return nil, err
	}

	if s.anomalyDetector == nil {
		return marshalResourceJSON("kafka://anomalies", map[string]any{
			"alerts": []any{}, "status": map[string]any{"active": false},
		})
	}

	since := time.Now().Add(-24 * time.Hour)
	alerts := s.anomalyDetector.GetAlerts(since, "", "", 100)
	status := s.anomalyDetector.GetStatus()

	s.logAudit(ctx, agentID, "resource:kafka://anomalies", nil, "", "success", len(alerts), nil, start)
	return marshalResourceJSON("kafka://anomalies", map[string]any{
		"alerts":      alerts,
		"alert_count": len(alerts),
		"status":      status,
	})
}

// handleWorkflowsResource returns available workflow templates.
func (s *Server) handleWorkflowsResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://workflows", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://workflows"); err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://workflows", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceCluster, "", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot access workflows", agentID)
		s.logAudit(ctx, agentID, "resource:kafka://workflows", nil, "", "error", 0, err, start)
		return nil, err
	}

	templates := s.workflowEngine.ListTemplates("")

	type templateSummary struct {
		Name        string   `json:"name"`
		Description string   `json:"description"`
		Category    string   `json:"category"`
		Tags        []string `json:"tags"`
		StepCount   int      `json:"step_count"`
	}

	var summaries []templateSummary
	for _, t := range templates {
		summaries = append(summaries, templateSummary{
			Name:        t.Name,
			Description: t.Description,
			Category:    t.Category,
			Tags:        t.Tags,
			StepCount:   len(t.Steps),
		})
	}

	s.logAudit(ctx, agentID, "resource:kafka://workflows", nil, "", "success", len(summaries), nil, start)
	return marshalResourceJSON("kafka://workflows", map[string]any{
		"templates":   summaries,
		"total_count": len(summaries),
		"categories":  []string{"operations", "planning", "development", "monitoring", "discovery", "quality", "debugging"},
	})
}

// handleKnowledgeGraphResource returns the full knowledge graph.
func (s *Server) handleKnowledgeGraphResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://knowledge_graph", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://knowledge_graph"); err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://knowledge_graph", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceCluster, "", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot access knowledge graph", agentID)
		s.logAudit(ctx, agentID, "resource:kafka://knowledge_graph", nil, "", "error", 0, err, start)
		return nil, err
	}

	// Refresh graph if empty
	stats := s.knowledgeGraph.GetStats()
	if stats["total_nodes"].(int) == 0 {
		if err := s.refreshKnowledgeGraph(ctx, ""); err != nil {
			s.logger.Warn("failed to refresh knowledge graph", "error", err)
		}
	}

	result, err := s.knowledgeGraph.Query("all")
	if err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://knowledge_graph", nil, "", "error", 0, err, start)
		return nil, fmt.Errorf("querying knowledge graph: %w", err)
	}
	graphStats := s.knowledgeGraph.GetStats()

	s.logAudit(ctx, agentID, "resource:kafka://knowledge_graph", nil, "", "success", result.TotalNodes, nil, start)
	return marshalResourceJSON("kafka://knowledge_graph", map[string]any{
		"nodes":       result.Nodes,
		"edges":       result.Edges,
		"stats":       graphStats,
		"total_nodes": result.TotalNodes,
		"total_edges": result.TotalEdges,
	})
}

// handleFlowGraphResource returns the real-time message flow graph.
func (s *Server) handleFlowGraphResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://flow_graph", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://flow_graph"); err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://flow_graph", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceCluster, "", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot access flow graph", agentID)
		s.logAudit(ctx, agentID, "resource:kafka://flow_graph", nil, "", "error", 0, err, start)
		return nil, err
	}

	// Build a flow graph from current topic/group state
	topics, err := s.kafkaManager.ListTopics(ctx, "", false)
	if err != nil {
		s.logger.Warn("failed to list topics for flow graph", "error", err)
	}
	groups, err := s.kafkaManager.ListConsumerGroups(ctx, "")
	if err != nil {
		s.logger.Warn("failed to list consumer groups for flow graph", "error", err)
	}

	var nodes []map[string]any
	for _, t := range topics {
		nodes = append(nodes, map[string]any{
			"id":           t.Name,
			"type":         "topic",
			"name":         t.Name,
			"partitions":   t.Partitions,
			"message_count": t.MessageCount,
		})
	}

	var edges []map[string]any
	for _, g := range groups {
		nodes = append(nodes, map[string]any{
			"id":           "group:" + g.GroupID,
			"type":         "consumer_group",
			"name":         g.GroupID,
			"state":        g.State,
			"total_lag":    g.TotalLag,
			"member_count": g.MemberCount,
		})

		detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, "", g.GroupID)
		if err == nil {
			topicSet := make(map[string]bool)
			for _, offset := range detail.Offsets {
				topicSet[offset.Topic] = true
			}
			for topic := range topicSet {
				edges = append(edges, map[string]any{
					"source": topic,
					"target": "group:" + g.GroupID,
					"type":   "consumes",
				})
			}
		}
	}

	s.logAudit(ctx, agentID, "resource:kafka://flow_graph", nil, "", "success", len(nodes), nil, start)
	return marshalResourceJSON("kafka://flow_graph", map[string]any{
		"nodes":       nodes,
		"edges":       edges,
		"total_nodes": len(nodes),
		"total_edges": len(edges),
		"updated_at":  time.Now().UTC().Format(time.RFC3339),
	})
}

// handlePlatformResource returns platform information.
func (s *Server) handlePlatformResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://platforms", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://platforms"); err != nil {
		s.logAudit(ctx, agentID, "resource:kafka://platforms", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceCluster, "", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot access platforms", agentID)
		s.logAudit(ctx, agentID, "resource:kafka://platforms", nil, "", "error", 0, err, start)
		return nil, err
	}

	platforms := s.platformMgr.ListPlatforms(ctx)

	// Add cluster info
	var clusterPlatforms []map[string]any
	for _, name := range s.kafkaManager.ClusterNames() {
		entry := map[string]any{
			"cluster": name,
			"type":    "kafka",
		}
		clusterPlatforms = append(clusterPlatforms, entry)
	}

	s.logAudit(ctx, agentID, "resource:kafka://platforms", nil, "", "success", len(platforms), nil, start)
	return marshalResourceJSON("kafka://platforms", map[string]any{
		"platforms":        platforms,
		"cluster_backends": clusterPlatforms,
		"supported_types":  []string{"kafka", "redpanda", "confluent_cloud", "msk_serverless"},
	})
}
