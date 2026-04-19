package server

import "github.com/mark3labs/mcp-go/mcp"

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

s.mcpServer.AddResource(mcp.NewResource("kafka://topic_graph", "Topic Topology Graph",
mcp.WithResourceDescription("Producer -> topic -> consumer group relationship graph"),
mcp.WithMIMEType("application/json"),
), s.handleTopicGraphResource)

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

s.mcpServer.AddResource(mcp.NewResource("kafka://replication/status", "Replication Status",
mcp.WithResourceDescription("Cross-cluster replication status with lag per topic-partition"),
mcp.WithMIMEType("application/json"),
), s.handleReplicationStatus)

s.mcpServer.AddResource(mcp.NewResource("kafka://plugins", "Plugin Registry",
mcp.WithResourceDescription("Registered plugins and extensions"),
mcp.WithMIMEType("application/json"),
), s.handlePluginRegistry)

s.mcpServer.AddResource(mcp.NewResource("kafka://watch/subscriptions", "Watch Subscriptions",
mcp.WithResourceDescription("Active watch subscriptions for event-driven workflows"),
mcp.WithMIMEType("application/json"),
), s.handleWatchSubscriptionsResource)

// New Feature Resources
s.mcpServer.AddResource(mcp.NewResource("kafka://anomalies", "Anomaly Alerts",
mcp.WithResourceDescription("AI-detected anomalies in consumer lag, throughput, and error rates (last 24h)"),
mcp.WithMIMEType("application/json"),
), s.handleAnomalyResource)

s.mcpServer.AddResource(mcp.NewResource("kafka://workflows", "Workflow Templates",
mcp.WithResourceDescription("Pre-built workflow templates for common Kafka operations"),
mcp.WithMIMEType("application/json"),
), s.handleWorkflowsResource)

s.mcpServer.AddResource(mcp.NewResource("kafka://knowledge_graph", "Knowledge Graph",
mcp.WithResourceDescription("Topic relationships, schema dependencies, and consumer group topology graph"),
mcp.WithMIMEType("application/json"),
), s.handleKnowledgeGraphResource)

s.mcpServer.AddResource(mcp.NewResource("kafka://flow_graph", "Message Flow Graph",
mcp.WithResourceDescription("Real-time message flow visualization with throughput and latency data"),
mcp.WithMIMEType("application/json"),
), s.handleFlowGraphResource)

s.mcpServer.AddResource(mcp.NewResource("kafka://platforms", "Platform Info",
mcp.WithResourceDescription("Configured Kafka-compatible platforms and their capabilities"),
mcp.WithMIMEType("application/json"),
), s.handlePlatformResource)
}
