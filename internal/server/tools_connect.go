package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

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
		return s.toolError("Cluster error", err), nil
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
		return s.toolError("Failed to create connector", err), nil
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
		return s.toolError("Cluster error", err), nil
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
		return s.toolError("Failed to update connector", err), nil
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
		return s.toolError("Cluster error", err), nil
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
		return s.toolError("Failed to delete connector", err), nil
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
		return s.toolError("Cluster error", err), nil
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
		return s.toolError("Failed to restart", err), nil
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
		return s.toolError("Cluster error", err), nil
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
		return s.toolError("Failed to get health", err), nil
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
