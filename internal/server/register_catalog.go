package server

import (
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) registerCatalogTools() {
	if s.catalogEngine == nil {
		return
	}

	s.mcpServer.AddTool(mcp.NewTool("kafka_catalog_search",
		withCategory("governance"),
		mcp.WithDescription("Search the data catalog for topics, schemas, connectors by name, description, tag, owner, freshness, or quality score."),
		mcp.WithString("query", mcp.Description("Full-text search query")),
		mcp.WithString("type", mcp.Description("Filter by type: topic, schema, connector, consumer_group")),
		mcp.WithObject("tags", mcp.Description("Filter by tags")),
		mcp.WithString("owner", mcp.Description("Filter by owner")),
		mcp.WithNumber("min_quality", mcp.Description("Minimum quality score (0-100)")),
		mcp.WithString("freshness", mcp.Description("Filter by freshness: live, stale, inactive")),
		mcp.WithNumber("limit", mcp.Description("Max results (default: 20)")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_catalog_search",
		resourceType: "topic",
		action:       "read",
	}, s.handleCatalogSearch))

	s.mcpServer.AddTool(mcp.NewTool("kafka_catalog_tag",
		withCategory("governance"),
		mcp.WithDescription("Add tags to a catalog entry, index a new entry, or set lineage information."),
		mcp.WithString("action", mcp.Required(), mcp.Description("Action: tag, index, lineage, delete")),
		mcp.WithString("entry_id", mcp.Description("Catalog entry ID (for tag/lineage/delete)")),
		mcp.WithObject("tags", mcp.Description("Tags to add")),
		mcp.WithString("name", mcp.Description("Resource name (for index)")),
		mcp.WithString("type", mcp.Description("Resource type: topic, schema, connector (for index)")),
		mcp.WithString("description", mcp.Description("Description (for index)")),
		mcp.WithString("owner", mcp.Description("Owner (for index)")),
		mcp.WithObject("lineage", mcp.Description("Lineage info: {upstream, downstream, producers, consumers}")),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_catalog_tag",
		resourceType: "topic",
		action:       "write",
	}, s.handleCatalogTag))

	s.mcpServer.AddTool(mcp.NewTool("kafka_catalog_stats",
		withCategory("governance"),
		mcp.WithDescription("Get catalog statistics: entry counts by type, freshness distribution, quality scores, tag usage."),
	), s.withToolAuth(toolAuthConfig{
		tool:         "kafka_catalog_stats",
		resourceType: "topic",
		action:       "read",
	}, s.handleCatalogStats))
}
