package server

import "github.com/mark3labs/mcp-go/mcp"

var stableResourceURIs = []string{
	"kafka://audit/log",
	"kafka://cluster/metadata",
	"kafka://clusters",
	"kafka://groups",
	"kafka://groups/{id}",
	"kafka://schemas",
	"kafka://schemas/{subject}",
	"kafka://topics",
	"kafka://topics/{name}",
}

func (s *Server) registerResources() {
	s.mcpServer.AddResource(mcp.NewResource("kafka://cluster/metadata", "Cluster Metadata",
		mcp.WithResourceDescription("Kafka cluster information: brokers, controller, topic/partition counts"),
		mcp.WithMIMEType("application/json"),
	), s.handleClusterMetadata)

	s.mcpServer.AddResource(mcp.NewResource("kafka://clusters", "Cluster List",
		mcp.WithResourceDescription("All configured Kafka clusters for multi-cluster discovery"),
		mcp.WithMIMEType("application/json"),
	), s.handleClusterList)

	s.mcpServer.AddResource(mcp.NewResource("kafka://topics", "Topic Catalog",
		mcp.WithResourceDescription("List of all topics with partition counts and retention"),
		mcp.WithMIMEType("application/json"),
	), s.handleTopicCatalog)

	s.mcpServer.AddResource(mcp.NewResource("kafka://schemas", "Schema Catalog",
		mcp.WithResourceDescription("All Schema Registry subjects with latest versions"),
		mcp.WithMIMEType("application/json"),
	), s.handleSchemaCatalog)

	s.mcpServer.AddResource(mcp.NewResource("kafka://groups", "Consumer Group Catalog",
		mcp.WithResourceDescription("All consumer groups with state and total lag"),
		mcp.WithMIMEType("application/json"),
	), s.handleGroupCatalog)

	s.mcpServer.AddResource(mcp.NewResource("kafka://audit/log", "Agent Audit Trail",
		mcp.WithResourceDescription("Recent agent interactions with KafkaMCP (last 1000 entries)"),
		mcp.WithMIMEType("application/json"),
	), s.handleAuditLogResource)

	// Resource templates for individual items
	s.mcpServer.AddResourceTemplate(mcp.NewResourceTemplate("kafka://topics/{name}", "Topic Detail",
		mcp.WithTemplateDescription("Full config, partition map, associated schemas for a topic"),
		mcp.WithTemplateMIMEType("application/json"),
	), s.handleTopicResource)

	s.mcpServer.AddResourceTemplate(mcp.NewResourceTemplate("kafka://schemas/{subject}", "Schema Detail",
		mcp.WithTemplateDescription("Full schema definition with field summary"),
		mcp.WithTemplateMIMEType("application/json"),
	), s.handleSchemaResource)

	s.mcpServer.AddResourceTemplate(mcp.NewResourceTemplate("kafka://groups/{id}", "Consumer Group Detail",
		mcp.WithTemplateDescription("Members, offsets, lag per partition for a consumer group"),
		mcp.WithTemplateMIMEType("application/json"),
	), s.handleGroupResource)

}
