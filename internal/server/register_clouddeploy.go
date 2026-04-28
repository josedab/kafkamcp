package server

import "github.com/mark3labs/mcp-go/mcp"

func (s *Server) registerCloudDeployTools() {
	s.mcpServer.AddTool(mcp.NewTool("kafka_cloud_deploy_generate",
		withCategory("ops"),
		mcp.WithDescription("Generate zero-config deployment templates for AWS (MSK), GCP, Azure (Event Hubs), or Confluent Cloud. Produces Docker Compose, Kubernetes manifests, cloud CLI commands, and config files."),
		mcp.WithString("provider", mcp.Required(), mcp.Description("Cloud provider: aws, gcp, azure, confluent")),
		mcp.WithString("service", mcp.Description("Kafka service: aws-msk, confluent-cloud, azure-event-hubs, gcp-managed-kafka")),
		mcp.WithString("bootstrap_servers", mcp.Description("Kafka bootstrap servers")),
		mcp.WithString("region", mcp.Description("Cloud region (defaults to provider's primary region)")),
		mcp.WithString("cluster_name", mcp.Description("Cluster name. Default: kafkamcp")),
	), s.handleCloudDeployGenerate)

	s.mcpServer.AddTool(mcp.NewTool("kafka_cloud_deploy_list",
		withCategory("ops"),
		mcp.WithDescription("List previously generated deployment templates."),
	), s.handleCloudDeployList)
}
