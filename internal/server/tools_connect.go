package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/mark3labs/mcp-go/mcp"
)

// ===================== KAFKA CONNECT MANAGEMENT HANDLERS =====================

// handleCreateConnector creates a new Kafka Connect connector.
func (s *Server) handleCreateConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_create_connector", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		cluster := request.GetString("cluster", "")

		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
		if err != nil {
			return s.toolError("Cluster error", err), nil
		}

		client, err := s.getConnectClient(clCfg)
		if err != nil || client == nil {
			if err != nil {
				s.logger.Warn("Kafka Connect client error", "error", err)
			}
			return mcp.NewToolResultError("Kafka Connect is not available"), nil
		}

		config := extractStringMapArg(request, "config")
		if len(config) == 0 {
			return mcp.NewToolResultError("config is required"), nil
		}
		if deny, _ := validateConnectorConfig(config); deny != nil {
			return deny, nil
		}

		result, err := client.CreateConnector(ctx, name, config)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_create_connector", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to create connector", err), nil
		}

		_, redactedConfig := validateConnectorConfig(config)
		s.logger.Info("connector created", "name", name, "config", redactedConfig)
		s.logAudit(ctx, agentID, "kafka_create_connector", args, cluster, "success", 0, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

// handleUpdateConnector updates a connector's configuration.
func (s *Server) handleUpdateConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_update_connector", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		cluster := request.GetString("cluster", "")

		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
		if err != nil {
			return s.toolError("Cluster error", err), nil
		}

		client, err := s.getConnectClient(clCfg)
		if err != nil || client == nil {
			if err != nil {
				s.logger.Warn("Kafka Connect client error", "error", err)
			}
			return mcp.NewToolResultError("Kafka Connect is not available"), nil
		}

		config := extractStringMapArg(request, "config")
		if len(config) == 0 {
			return mcp.NewToolResultError("config is required"), nil
		}
		if deny, _ := validateConnectorConfig(config); deny != nil {
			return deny, nil
		}

		result, err := client.UpdateConnector(ctx, name, config)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_update_connector", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to update connector", err), nil
		}

		_, redactedConfig := validateConnectorConfig(config)
		s.logger.Info("connector updated", "name", name, "config", redactedConfig)
		s.logAudit(ctx, agentID, "kafka_update_connector", args, cluster, "success", 0, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

// handleDeleteConnector deletes a Kafka Connect connector.
func (s *Server) handleDeleteConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_delete_connector", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		cluster := request.GetString("cluster", "")

		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
		if err != nil {
			return s.toolError("Cluster error", err), nil
		}

		client, err := s.getConnectClient(clCfg)
		if err != nil || client == nil {
			if err != nil {
				s.logger.Warn("Kafka Connect client error", "error", err)
			}
			return mcp.NewToolResultError("Kafka Connect is not available"), nil
		}

		if err := client.DeleteConnector(ctx, name); err != nil {
			s.logAudit(ctx, agentID, "kafka_delete_connector", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to delete connector", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_delete_connector", args, cluster, "success", 0, nil, start)
		return marshalResult(map[string]any{"status": "deleted", "name": name})
	})(ctx, request)
}

// handleRestartConnector restarts a connector or specific tasks.
func (s *Server) handleRestartConnector(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_restart_connector", resourceType: auth.ResourceCluster, action: auth.ActionAdmin,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		cluster := request.GetString("cluster", "")

		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
		if err != nil {
			return s.toolError("Cluster error", err), nil
		}

		client, err := s.getConnectClient(clCfg)
		if err != nil || client == nil {
			if err != nil {
				s.logger.Warn("Kafka Connect client error", "error", err)
			}
			return mcp.NewToolResultError("Kafka Connect is not available"), nil
		}

		taskID := request.GetInt("task_id", -1)
		failedOnly := request.GetBool("failed_only", false)

		result, err := client.RestartConnector(ctx, name, taskID, failedOnly)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_restart_connector", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to restart", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_restart_connector", args, cluster, "success", 0, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

// handleConnectorHealth computes health score for a connector.
func (s *Server) handleConnectorHealth(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool: "kafka_connector_health", resourceType: auth.ResourceCluster, action: auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		name := request.GetString("name", "")
		cluster := request.GetString("cluster", "")

		if name == "" {
			return mcp.NewToolResultError("name is required"), nil
		}
		if deny := validateStringLength("name", name); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster", cluster); deny != nil {
			return deny, nil
		}

		clCfg, err := s.kafkaManager.GetClusterConfig(cluster)
		if err != nil {
			return s.toolError("Cluster error", err), nil
		}

		client, err := s.getConnectClient(clCfg)
		if err != nil || client == nil {
			if err != nil {
				s.logger.Warn("Kafka Connect client error", "error", err)
			}
			return mcp.NewToolResultError("Kafka Connect is not available"), nil
		}

		health, err := client.ConnectorHealth(ctx, name)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_connector_health", args, cluster, "error", 0, err, start)
			return s.toolError("Failed to get health", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_connector_health", args, cluster, "success", 0, nil, start)
		return marshalResult(health)
	})(ctx, request)
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

// sensitiveConnectorConfigKeys lists connector config keys whose values should be redacted in logs.
var sensitiveConnectorConfigKeys = []string{
	"password", "secret", "token", "credential", "api.key", "aws.secret",
	"ssl.keystore.password", "ssl.truststore.password", "ssl.key.password",
	"sasl.jaas.config", "consumer.override.sasl.jaas.config",
	"producer.override.sasl.jaas.config",
}

// validateConnectorConfig validates connector config keys and values for length,
// and returns a redacted copy suitable for audit logging.
func validateConnectorConfig(config map[string]string) (*mcp.CallToolResult, map[string]string) {
	redacted := make(map[string]string, len(config))
	for k, v := range config {
		if deny := validateStringLength("config key", k); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("config value", v); deny != nil {
			return deny, nil
		}
		redacted[k] = v
		lk := strings.ToLower(k)
		for _, sensitive := range sensitiveConnectorConfigKeys {
			if strings.Contains(lk, sensitive) {
				redacted[k] = "***REDACTED***"
				break
			}
		}
	}
	return nil, redacted
}
