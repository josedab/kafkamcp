package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/kafka"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== RESOURCE HANDLERS =====================

// handleClusterMetadata returns broker metadata for the default cluster.
func (s *Server) handleClusterMetadata(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://cluster/metadata", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://cluster/metadata"); err != nil {
		s.logAudit(agentID, "resource:kafka://cluster/metadata", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceCluster, "", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot read cluster metadata", agentID)
		s.logAudit(agentID, "resource:kafka://cluster/metadata", nil, "", "error", 0, err, start)
		return nil, err
	}
	meta, err := s.kafkaManager.GetClusterMetadata(ctx, "")
	if err != nil {
		err = fmt.Errorf("getting cluster metadata: %w", err)
		s.logAudit(agentID, "resource:kafka://cluster/metadata", nil, "", "error", 0, err, start)
		return nil, err
	}
	s.logAudit(agentID, "resource:kafka://cluster/metadata", nil, "", "success", 0, nil, start)
	return marshalResourceJSON("kafka://cluster/metadata", meta)
}

// handleClusterList returns a summary of all configured Kafka clusters.
func (s *Server) handleClusterList(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://clusters", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://clusters"); err != nil {
		s.logAudit(agentID, "resource:kafka://clusters", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceCluster, "", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot list clusters", agentID)
		s.logAudit(agentID, "resource:kafka://clusters", nil, "", "error", 0, err, start)
		return nil, err
	}
	type clusterSummary struct {
		Name              string `json:"name"`
		HasSchemaRegistry bool   `json:"has_schema_registry"`
		IsDefault         bool   `json:"is_default"`
	}
	var clusters []clusterSummary
	for _, cl := range s.config.Clusters {
		clusters = append(clusters, clusterSummary{
			Name:              cl.Name,
			HasSchemaRegistry: cl.SchemaRegistry.URL != "",
			IsDefault:         cl.Default,
		})
	}
	s.logAudit(agentID, "resource:kafka://clusters", nil, "", "success", len(clusters), nil, start)
	return marshalResourceJSON("kafka://clusters", map[string]any{"clusters": clusters, "total_count": len(clusters)})
}

// handleTopicCatalog returns a catalog of all topics the agent is authorized to read.
func (s *Server) handleTopicCatalog(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://topics", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://topics"); err != nil {
		s.logAudit(agentID, "resource:kafka://topics", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceTopic, "*", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot list topics", agentID)
		s.logAudit(agentID, "resource:kafka://topics", nil, "", "error", 0, err, start)
		return nil, err
	}
	topics, err := s.kafkaManager.ListTopics(ctx, "", false)
	if err != nil {
		err = fmt.Errorf("listing topics: %w", err)
		s.logAudit(agentID, "resource:kafka://topics", nil, "", "error", 0, err, start)
		return nil, err
	}
	// Filter by policy
	if s.config.Policies.DefaultDeny {
		var allowed []kafka.TopicSummary
		for _, t := range topics {
			if s.policyEngine.Authorize(agentID, auth.ResourceTopic, t.Name, auth.ActionRead) {
				allowed = append(allowed, t)
			}
		}
		topics = allowed
	}
	s.logAudit(agentID, "resource:kafka://topics", nil, "", "success", len(topics), nil, start)
	return marshalResourceJSON("kafka://topics", map[string]any{"topics": topics, "total_count": len(topics)})
}

// handleSchemaCatalog returns all Schema Registry subjects for the default cluster.
func (s *Server) handleSchemaCatalog(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://schemas", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://schemas"); err != nil {
		s.logAudit(agentID, "resource:kafka://schemas", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceSchema, "", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot read schemas", agentID)
		s.logAudit(agentID, "resource:kafka://schemas", nil, "", "error", 0, err, start)
		return nil, err
	}
	sc := s.getSchemaClient("")
	if sc == nil {
		return marshalResourceJSON("kafka://schemas", map[string]any{"error": "Schema Registry not configured"})
	}
	subjects, err := sc.ListSubjectsDetailed(ctx)
	if err != nil {
		err = fmt.Errorf("listing schemas: %w", err)
		s.logAudit(agentID, "resource:kafka://schemas", nil, "", "error", 0, err, start)
		return nil, err
	}
	s.logAudit(agentID, "resource:kafka://schemas", nil, "", "success", len(subjects), nil, start)
	return marshalResourceJSON("kafka://schemas", map[string]any{"subjects": subjects, "total_count": len(subjects)})
}

// handleGroupCatalog returns all consumer groups for the default cluster.
func (s *Server) handleGroupCatalog(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://groups", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://groups"); err != nil {
		s.logAudit(agentID, "resource:kafka://groups", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceConsumerGroup, "", auth.ActionDescribe) {
		err := fmt.Errorf("unauthorized: agent %q cannot list consumer groups", agentID)
		s.logAudit(agentID, "resource:kafka://groups", nil, "", "error", 0, err, start)
		return nil, err
	}
	groups, err := s.kafkaManager.ListConsumerGroups(ctx, "")
	if err != nil {
		err = fmt.Errorf("listing consumer groups: %w", err)
		s.logAudit(agentID, "resource:kafka://groups", nil, "", "error", 0, err, start)
		return nil, err
	}
	s.logAudit(agentID, "resource:kafka://groups", nil, "", "success", len(groups), nil, start)
	return marshalResourceJSON("kafka://groups", map[string]any{"groups": groups, "total_count": len(groups)})
}

// handleAuditLogResource returns recent audit log entries from the in-memory ring buffer.
func (s *Server) handleAuditLogResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://audit/log", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://audit/log"); err != nil {
		s.logAudit(agentID, "resource:kafka://audit/log", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceCluster, "", auth.ActionAdmin) {
		err := fmt.Errorf("unauthorized: agent %q cannot access audit logs", agentID)
		s.logAudit(agentID, "resource:kafka://audit/log", nil, "", "error", 0, err, start)
		return nil, err
	}
	entries := s.auditLogger.RecentEntries(1000)
	return marshalResourceJSON("kafka://audit/log", map[string]any{"entries": entries, "count": len(entries)})
}

// handleTopicGraphResource returns a dependency graph of topics based on connector and consumer group relationships.
func (s *Server) handleTopicGraphResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://topic_graph", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://topic_graph"); err != nil {
		s.logAudit(agentID, "resource:kafka://topic_graph", nil, "", "error", 0, err, start)
		return nil, err
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceCluster, "", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot access topic graph", agentID)
		s.logAudit(agentID, "resource:kafka://topic_graph", nil, "", "error", 0, err, start)
		return nil, err
	}
	graph, err := s.kafkaManager.GetTopicGraph(ctx, "")
	if err != nil {
		err = fmt.Errorf("building topic graph: %w", err)
		s.logAudit(agentID, "resource:kafka://topic_graph", nil, "", "error", 0, err, start)
		return nil, err
	}
	s.logAudit(agentID, "resource:kafka://topic_graph", nil, "", "success", len(graph.Nodes), nil, start)
	return marshalResourceJSON("kafka://topic_graph", graph)
}

// handleTopicResource returns detailed metadata for a single topic identified by its URI.
func (s *Server) handleTopicResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://topics/{name}", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://topics/{name}"); err != nil {
		s.logAudit(agentID, "resource:kafka://topics/{name}", nil, "", "error", 0, err, start)
		return nil, err
	}
	uri := request.Params.URI
	name := strings.TrimPrefix(uri, "kafka://topics/")
	if name == "" || name == uri {
		return nil, fmt.Errorf("invalid topic URI")
	}
	if len(name) > maxInputLength {
		return nil, fmt.Errorf("topic name exceeds maximum length of %d characters", maxInputLength)
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceTopic, name, auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot read topic %q", agentID, name)
		s.logAudit(agentID, "resource:kafka://topics/"+name, nil, "", "error", 0, err, start)
		return nil, err
	}
	detail, err := s.kafkaManager.DescribeTopic(ctx, "", name)
	if err != nil {
		err = fmt.Errorf("describing topic: %w", err)
		s.logAudit(agentID, "resource:kafka://topics/"+name, nil, "", "error", 0, err, start)
		return nil, err
	}
	s.logAudit(agentID, "resource:kafka://topics/"+name, nil, "", "success", 0, nil, start)
	return marshalResourceJSON(uri, detail)
}

// handleSchemaResource returns the latest schema for a subject identified by its URI.
func (s *Server) handleSchemaResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://schemas/{subject}", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://schemas/{subject}"); err != nil {
		s.logAudit(agentID, "resource:kafka://schemas/{subject}", nil, "", "error", 0, err, start)
		return nil, err
	}
	sc := s.getSchemaClient("")
	if sc == nil {
		return nil, fmt.Errorf("Schema Registry not configured")
	}
	uri := request.Params.URI
	subject := strings.TrimPrefix(uri, "kafka://schemas/")
	if subject == "" || subject == uri {
		return nil, fmt.Errorf("invalid schema URI")
	}
	if len(subject) > maxInputLength {
		return nil, fmt.Errorf("schema subject exceeds maximum length of %d characters", maxInputLength)
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceSchema, "", auth.ActionRead) {
		err := fmt.Errorf("unauthorized: agent %q cannot read schemas", agentID)
		s.logAudit(agentID, "resource:kafka://schemas/"+subject, nil, "", "error", 0, err, start)
		return nil, err
	}
	info, err := sc.GetSchema(ctx, subject, "latest")
	if err != nil {
		err = fmt.Errorf("getting schema: %w", err)
		s.logAudit(agentID, "resource:kafka://schemas/"+subject, nil, "", "error", 0, err, start)
		return nil, err
	}
	s.logAudit(agentID, "resource:kafka://schemas/"+subject, nil, "", "success", 0, nil, start)
	return marshalResourceJSON(uri, info)
}

// handleGroupResource returns detailed state for a consumer group identified by its URI.
func (s *Server) handleGroupResource(ctx context.Context, request mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
	start := time.Now()
	agentID := extractAgentIDFromResource(request)
	if err := s.validateResourceAgentID(agentID); err != nil {
		s.logAudit(agentID, "resource:kafka://groups/{id}", nil, "", "error", 0, err, start)
		return nil, err
	}
	if err := s.checkResourceRateLimit(agentID, "resource:kafka://groups/{id}"); err != nil {
		s.logAudit(agentID, "resource:kafka://groups/{id}", nil, "", "error", 0, err, start)
		return nil, err
	}
	uri := request.Params.URI
	groupID := strings.TrimPrefix(uri, "kafka://groups/")
	if groupID == "" || groupID == uri {
		return nil, fmt.Errorf("invalid group URI")
	}
	if len(groupID) > maxInputLength {
		return nil, fmt.Errorf("group ID exceeds maximum length of %d characters", maxInputLength)
	}
	if s.config.Policies.DefaultDeny && !s.policyEngine.Authorize(agentID, auth.ResourceConsumerGroup, "", auth.ActionDescribe) {
		err := fmt.Errorf("unauthorized: agent %q cannot describe consumer groups", agentID)
		s.logAudit(agentID, "resource:kafka://groups/"+groupID, nil, "", "error", 0, err, start)
		return nil, err
	}
	detail, err := s.kafkaManager.DescribeConsumerGroup(ctx, "", groupID)
	if err != nil {
		err = fmt.Errorf("describing group: %w", err)
		s.logAudit(agentID, "resource:kafka://groups/"+groupID, nil, "", "error", 0, err, start)
		return nil, err
	}
	s.logAudit(agentID, "resource:kafka://groups/"+groupID, nil, "", "success", 0, nil, start)
	return marshalResourceJSON(uri, detail)
}
