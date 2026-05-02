package server

import (
	"context"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/clouddeploy"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleCloudDeployGenerate(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cloud_deploy_generate",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		provider := request.GetString("provider", "")
		if provider == "" {
			return mcp.NewToolResultError("provider is required"), nil
		}
		if deny := validateStringLength("provider", provider); deny != nil {
			return deny, nil
		}

		cfg := clouddeploy.DeploymentConfig{
			Provider:         clouddeploy.CloudProvider(provider),
			Service:          clouddeploy.KafkaService(request.GetString("service", "")),
			BootstrapServers: request.GetString("bootstrap_servers", "localhost:9092"),
			Region:           request.GetString("region", ""),
			ClusterName:      request.GetString("cluster_name", "kafkamcp"),
		}

		if deny := validateStringLength("region", cfg.Region); deny != nil {
			return deny, nil
		}
		if deny := validateStringLength("cluster_name", cfg.ClusterName); deny != nil {
			return deny, nil
		}

		// Auto-detect service from provider
		if cfg.Service == "" {
			switch cfg.Provider {
			case clouddeploy.ProviderAWS:
				cfg.Service = clouddeploy.ServiceMSK
			case clouddeploy.ProviderGCP:
				cfg.Service = clouddeploy.ServiceGCPKafka
			case clouddeploy.ProviderAzure:
				cfg.Service = clouddeploy.ServiceEventHubs
			case clouddeploy.ProviderConfluent:
				cfg.Service = clouddeploy.ServiceConfluentCloud
			}
		}

		templates, err := s.cloudDeployGenerator.Generate(cfg)
		if err != nil {
			s.logAudit(agentID, "kafka_cloud_deploy_generate", args, "", "error", 0, err, start)
			return s.toolError("Failed to generate templates", err), nil
		}

		s.logAudit(agentID, "kafka_cloud_deploy_generate", args, "", "success", len(templates), nil, start)
		return marshalResult(map[string]any{
			"templates": templates,
			"total":     len(templates),
			"message":   fmt.Sprintf("Generated %d deployment templates for %s/%s", len(templates), provider, cfg.Service),
		})
	})(ctx, request)
}

func (s *Server) handleCloudDeployList(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_cloud_deploy_list",
		resourceType: auth.ResourceCluster,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		templates := s.cloudDeployGenerator.ListTemplates()

		s.logAudit(agentID, "kafka_cloud_deploy_list", args, "", "success", len(templates), nil, start)
		return marshalResult(map[string]any{"templates": templates, "total": len(templates)})
	})(ctx, request)
}
