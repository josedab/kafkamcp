package server

import (
	"context"
	"time"

	"github.com/kafkamcp/kafkamcp/internal/auth"
	"github.com/kafkamcp/kafkamcp/internal/timetravel"
	"github.com/mark3labs/mcp-go/mcp"
)

func (s *Server) handleTimeTravelRecord(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_timetravel_record",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		key := request.GetString("key", "")
		snapType := request.GetString("type", "")
		if key == "" || snapType == "" {
			return mcp.NewToolResultError("key and type are required"), nil
		}
		if deny := validateStringLength("key", key); deny != nil {
			return deny, nil
		}

		state, _ := args["state"].(map[string]any)
		if state == nil {
			return mcp.NewToolResultError("state must be a JSON object"), nil
		}

		s.timeTravelEngine.RecordSnapshot(key, snapType, state)
		s.logAudit(ctx, agentID, "kafka_timetravel_record", args, "", "success", 1, nil, start)
		return marshalResult(map[string]any{"status": "recorded", "key": key, "type": snapType})
	})(ctx, request)
}

func (s *Server) handleTimeTravelQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_timetravel_query",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		key := request.GetString("key", "")
		snapType := request.GetString("type", "")
		tsStr := request.GetString("timestamp", "")
		if key == "" || snapType == "" || tsStr == "" {
			return mcp.NewToolResultError("key, type, and timestamp are required"), nil
		}
		if deny := validateStringLength("key", key); deny != nil {
			return deny, nil
		}

		ts, err := time.Parse(time.RFC3339, tsStr)
		if err != nil {
			return mcp.NewToolResultError("invalid timestamp: " + err.Error()), nil
		}

		result, err := s.timeTravelEngine.QueryAt(timetravel.TimeTravelQuery{
			Key:       key,
			Type:      snapType,
			Timestamp: ts,
		})
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_timetravel_query", args, "", "error", 0, err, start)
			return s.toolError("Failed to query time-travel state", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_timetravel_query", args, "", "success", 1, nil, start)
		return marshalResult(result)
	})(ctx, request)
}

func (s *Server) handleTimeTravelRange(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_timetravel_range",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		key := request.GetString("key", "")
		snapType := request.GetString("type", "")
		fromStr := request.GetString("from", "")
		toStr := request.GetString("to", "")
		if key == "" || snapType == "" || fromStr == "" || toStr == "" {
			return mcp.NewToolResultError("key, type, from, and to are required"), nil
		}
		if deny := validateStringLength("key", key); deny != nil {
			return deny, nil
		}

		from, err := time.Parse(time.RFC3339, fromStr)
		if err != nil {
			return mcp.NewToolResultError("invalid from timestamp: " + err.Error()), nil
		}
		to, err := time.Parse(time.RFC3339, toStr)
		if err != nil {
			return mcp.NewToolResultError("invalid to timestamp: " + err.Error()), nil
		}

		snapshots, err := s.timeTravelEngine.QueryRange(key, snapType, from, to)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_timetravel_range", args, "", "error", 0, err, start)
			return s.toolError("Failed to query time-travel range", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_timetravel_range", args, "", "success", len(snapshots), nil, start)
		return marshalResult(snapshots)
	})(ctx, request)
}

func (s *Server) handleTimeTravelDiff(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_timetravel_diff",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		key := request.GetString("key", "")
		snapType := request.GetString("type", "")
		t1Str := request.GetString("t1", "")
		t2Str := request.GetString("t2", "")
		if key == "" || snapType == "" || t1Str == "" || t2Str == "" {
			return mcp.NewToolResultError("key, type, t1, and t2 are required"), nil
		}
		if deny := validateStringLength("key", key); deny != nil {
			return deny, nil
		}

		t1, err := time.Parse(time.RFC3339, t1Str)
		if err != nil {
			return mcp.NewToolResultError("invalid t1 timestamp: " + err.Error()), nil
		}
		t2, err := time.Parse(time.RFC3339, t2Str)
		if err != nil {
			return mcp.NewToolResultError("invalid t2 timestamp: " + err.Error()), nil
		}

		changes, err := s.timeTravelEngine.DiffAt(key, snapType, t1, t2)
		if err != nil {
			s.logAudit(ctx, agentID, "kafka_timetravel_diff", args, "", "error", 0, err, start)
			return s.toolError("Failed to diff time-travel state", err), nil
		}

		s.logAudit(ctx, agentID, "kafka_timetravel_diff", args, "", "success", len(changes), nil, start)
		return marshalResult(changes)
	})(ctx, request)
}

func (s *Server) handleTimeTravelList(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	return s.withToolAuth(toolAuthConfig{
		tool:         "kafka_timetravel_list",
		resourceType: auth.ResourceTopic,
		action:       auth.ActionRead,
	}, func(ctx context.Context, request mcp.CallToolRequest, agentID string, args map[string]any, start time.Time) (*mcp.CallToolResult, error) {
		snapType := request.GetString("type", "")

		keys := s.timeTravelEngine.ListKeys(snapType)
		s.logAudit(ctx, agentID, "kafka_timetravel_list", args, "", "success", len(keys), nil, start)
		return marshalResult(map[string]any{"keys": keys, "count": len(keys)})
	})(ctx, request)
}
