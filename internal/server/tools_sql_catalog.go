package server

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/catalog"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleSQLExecute(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	sql, _ := args["sql"].(string)
	if sql == "" {
		return mcp.NewToolResultError("sql query is required"), nil
	}

	if deny := validateStringLength("sql", sql); deny != nil {
		return deny, nil
	}

	// Parse the SQL query
	query, err := s.streamSQLEngine.Parse(sql, agentID)
	if err != nil {
		return s.toolError("SQL parse error", err), nil
	}

	// Consume messages from the source topic
	cluster, _ := args["cluster"].(string)
	cluster = s.resolveClusterName(cluster)

	limit := 100
	if v, ok := args["limit"].(float64); ok && v > 0 {
		limit = int(v)
		if limit > 10000 {
			limit = 10000
		}
	}

	messages, err := s.kafkaManager.Consume(ctx, cluster, query.SourceTopic, -1, "latest", limit, 10000, "")
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_sql_execute", args, cluster, "error", 0, err, start)
		return s.toolError("failed to consume from source topic", err), nil
	}

	// Convert to maps for SQL processing
	var msgMaps []map[string]any
	for _, m := range messages {
		record := make(map[string]any)
		record["_key"] = fmt.Sprintf("%v", m.Key)
		record["_partition"] = m.Partition
		record["_offset"] = m.Offset

		// Try to parse value as JSON
		if strVal, ok := m.Value.(string); ok {
			var parsed map[string]any
			if json.Unmarshal([]byte(strVal), &parsed) == nil {
				for k, v := range parsed {
					record[k] = v
				}
			} else {
				record["_value"] = strVal
			}
		} else {
			record["_value"] = m.Value
		}

		for k, v := range m.Headers {
			record["_header_"+k] = v
		}
		msgMaps = append(msgMaps, record)
	}

	// Execute the query
	result, err := s.streamSQLEngine.Execute(query.ID, msgMaps)
	if err != nil {
		s.logAudit(ctx, agentID, "kafka_sql_execute", args, cluster, "error", 0, err, start)
		return s.toolError("SQL execution error", err), nil
	}

	s.logAudit(ctx, agentID, "kafka_sql_execute", args, cluster, "success", result.RowCount, nil, start)
	return marshalResult(result)
}

func (s *Server) handleSQLExplain(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	sql, _ := args["sql"].(string)
	if sql == "" {
		return mcp.NewToolResultError("sql query is required"), nil
	}

	query, err := s.streamSQLEngine.Parse(sql, agentID)
	if err != nil {
		return s.toolError("SQL parse error", err), nil
	}

	plan, err := s.streamSQLEngine.Explain(query.ID)
	if err != nil {
		return s.toolError("SQL explain failed", err), nil
	}

	s.logAudit(ctx, agentID, "kafka_sql_explain", args, "", "success", 1, nil, start)
	return marshalResult(plan)
}

func (s *Server) handleSQLManage(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)
	queryID, _ := args["query_id"].(string)

	switch action {
	case "list":
		queries := s.streamSQLEngine.ListQueries(50)
		s.logAudit(ctx, agentID, "kafka_sql_manage", args, "", "success", len(queries), nil, start)
		return marshalResult(map[string]any{"queries": queries, "total": len(queries)})

	case "get":
		if queryID == "" {
			return mcp.NewToolResultError("query_id is required"), nil
		}
		q, err := s.streamSQLEngine.GetQuery(queryID)
		if err != nil {
			return s.toolError("query not found", err), nil
		}
		s.logAudit(ctx, agentID, "kafka_sql_manage", args, "", "success", 1, nil, start)
		return marshalResult(q)

	case "delete":
		if queryID == "" {
			return mcp.NewToolResultError("query_id is required"), nil
		}
		if err := s.streamSQLEngine.DeleteQuery(queryID); err != nil {
			return s.toolError("failed to delete query", err), nil
		}
		s.logAudit(ctx, agentID, "kafka_sql_manage", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "deleted"})

	default:
		return mcp.NewToolResultError("action must be: list, get, or delete"), nil
	}
}

func (s *Server) handleCatalogSearch(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	opts := catalog.SearchOptions{
		Query:     request.GetString("query", ""),
		Type:      request.GetString("type", ""),
		Owner:     request.GetString("owner", ""),
		Freshness: request.GetString("freshness", ""),
	}

	if v, ok := args["min_quality"].(float64); ok {
		opts.MinQuality = v
	}
	if v, ok := args["limit"].(float64); ok {
		opts.Limit = int(v)
	}

	if raw, ok := args["tags"]; ok {
		data, err := json.Marshal(raw)
		if err != nil {
			return s.toolError("failed to process tags", err), nil
		}
		if err := json.Unmarshal(data, &opts.Tags); err != nil {
			return s.toolError("failed to process tags", err), nil
		}
	}

	results := s.catalogEngine.Search(opts)

	s.logAudit(ctx, agentID, "kafka_catalog_search", args, "", "success", len(results), nil, start)
	return marshalResult(map[string]any{"results": results, "total": len(results)})
}

func (s *Server) handleCatalogTag(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	action, _ := args["action"].(string)

	switch action {
	case "tag":
		entryID, _ := args["entry_id"].(string)
		if entryID == "" {
			return mcp.NewToolResultError("entry_id is required"), nil
		}
		var tags []string
		if raw, ok := args["tags"]; ok {
			data, err := json.Marshal(raw)
			if err != nil {
				return s.toolError("failed to process tags", err), nil
			}
			if err := json.Unmarshal(data, &tags); err != nil {
				return s.toolError("failed to process tags", err), nil
			}
		}
		entry, err := s.catalogEngine.Tag(entryID, tags)
		if err != nil {
			return s.toolError("failed to tag catalog entry", err), nil
		}
		s.logAudit(ctx, agentID, "kafka_catalog_tag", args, "", "success", 1, nil, start)
		return marshalResult(entry)

	case "index":
		name, _ := args["name"].(string)
		entryType, _ := args["type"].(string)
		description, _ := args["description"].(string)
		owner, _ := args["owner"].(string)

		if name == "" || entryType == "" {
			return mcp.NewToolResultError("name and type are required for index"), nil
		}

		var tags []string
		if raw, ok := args["tags"]; ok {
			data, err := json.Marshal(raw)
			if err != nil {
				return s.toolError("failed to process tags", err), nil
			}
			if err := json.Unmarshal(data, &tags); err != nil {
				return s.toolError("failed to process tags", err), nil
			}
		}

		entry, err := s.catalogEngine.Index(name, entryType, description, owner, tags, nil, nil)
		if err != nil {
			return s.toolError("failed to index catalog entry", err), nil
		}
		s.logAudit(ctx, agentID, "kafka_catalog_tag", args, "", "success", 1, nil, start)
		return marshalResult(entry)

	case "lineage":
		entryID, _ := args["entry_id"].(string)
		if entryID == "" {
			return mcp.NewToolResultError("entry_id is required"), nil
		}
		var lineage catalog.LineageInfo
		if raw, ok := args["lineage"]; ok {
			data, err := json.Marshal(raw)
			if err != nil {
				return s.toolError("failed to process lineage data", err), nil
			}
			if err := json.Unmarshal(data, &lineage); err != nil {
				return s.toolError("failed to process lineage data", err), nil
			}
		}
		if err := s.catalogEngine.SetLineage(entryID, &lineage); err != nil {
			return s.toolError("failed to set lineage", err), nil
		}
		s.logAudit(ctx, agentID, "kafka_catalog_tag", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "lineage_set"})

	case "delete":
		entryID, _ := args["entry_id"].(string)
		if entryID == "" {
			return mcp.NewToolResultError("entry_id is required"), nil
		}
		if err := s.catalogEngine.DeleteEntry(entryID); err != nil {
			return s.toolError("failed to delete catalog entry", err), nil
		}
		s.logAudit(ctx, agentID, "kafka_catalog_tag", args, "", "success", 1, nil, start)
		return marshalResult(map[string]string{"status": "deleted"})

	default:
		return mcp.NewToolResultError("action must be: tag, index, lineage, or delete"), nil
	}
}

func (s *Server) handleCatalogStats(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
	stats := s.catalogEngine.Stats()
	s.logAudit(ctx, agentID, "kafka_catalog_stats", args, "", "success", stats.TotalEntries, nil, start)
	return marshalResult(stats)
}
